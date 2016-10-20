package com.conveyal.datatools.manager;

import com.amazonaws.services.s3.AmazonS3Client;
import com.conveyal.datatools.manager.auth.Auth0Connection;

import com.conveyal.datatools.manager.controllers.DumpController;
import com.conveyal.datatools.manager.controllers.api.*;
import com.conveyal.datatools.editor.controllers.api.*;

import com.conveyal.datatools.manager.extensions.ExternalFeedResource;
import com.conveyal.datatools.manager.extensions.mtc.MtcFeedResource;
import com.conveyal.datatools.manager.extensions.transitfeeds.TransitFeedsFeedResource;
import com.conveyal.datatools.manager.extensions.transitland.TransitLandFeedResource;

import com.conveyal.datatools.manager.jobs.FetchProjectFeedsJob;
import com.conveyal.datatools.manager.jobs.LoadGtfsApiFeedJob;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.utils.CorsFilter;
import com.conveyal.gtfs.GTFSCache;
import com.conveyal.gtfs.api.ApiMain;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.gson.Gson;
import org.apache.http.concurrent.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.utils.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.*;

import static spark.Spark.*;

public class DataManager {

    public static final Logger LOG = LoggerFactory.getLogger(DataManager.class);

    public static JsonNode config;
    public static JsonNode serverConfig;

    public static JsonNode gtfsPlusConfig;
    public static JsonNode gtfsConfig;

    public static final Map<String, ExternalFeedResource> feedResources = new HashMap<>();

    public static Map<String, Set<MonitorableJob>> userJobsMap = new HashMap<>();

    public static Map<String, ScheduledFuture> autoFetchMap = new HashMap<>();
    public final static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    public static GTFSCache gtfsCache;

    public static String feedBucket;
    public static String bucketFolder;

//    public final AmazonS3Client s3Client;
    public static boolean useS3;
    public static final String apiPrefix = "/api/manager/";

    private static List<String> apiFeedSources = new ArrayList<>();

    public static void main(String[] args) throws IOException {

        // load config
        loadConfig(args);

        // set port
        if (getConfigProperty("application.port") != null) {
            port(Integer.parseInt(getConfigPropertyAsText("application.port")));
        }
        useS3 = getConfigPropertyAsText("application.data.use_s3_storage").equals("true");

        // initialize map of auto fetched projects
        for (Project p : Project.getAll()) {
            if (p.autoFetchFeeds != null && autoFetchMap.get(p.id) == null){
                if (p.autoFetchFeeds) {
                    ScheduledFuture scheduledFuture = ProjectController.scheduleAutoFeedFetch(p.id, p.autoFetchHour, p.autoFetchMinute, 1, p.defaultTimeZone);
                    autoFetchMap.put(p.id, scheduledFuture);
                }
            }
        }

        feedBucket = getConfigPropertyAsText("application.data.gtfs_s3_bucket");
        bucketFolder = FeedStore.s3Prefix;

        if (useS3) {
            LOG.info("Initializing gtfs-api for bucket {}/{} and cache dir {}", feedBucket, bucketFolder, FeedStore.basePath);
            gtfsCache = new GTFSCache(feedBucket, bucketFolder, FeedStore.basePath);
        }
        else {
            LOG.info("Initializing gtfs cache locally (no s3 bucket) {}", FeedStore.basePath);
            gtfsCache = new GTFSCache(null, FeedStore.basePath);
        }
        CorsFilter.apply();

        // core controllers
        ProjectController.register(apiPrefix);
        FeedSourceController.register(apiPrefix);
        FeedVersionController.register(apiPrefix);
        RegionController.register(apiPrefix);
        NoteController.register(apiPrefix);
        StatusController.register(apiPrefix);

        // Editor routes
        if ("true".equals(getConfigPropertyAsText("modules.editor.enabled"))) {
            gtfsConfig = yamlMapper.readTree(new File("gtfs.yml"));
            AgencyController.register(apiPrefix);
            CalendarController.register(apiPrefix);
            RouteController.register(apiPrefix);
            RouteTypeController.register(apiPrefix);
            ScheduleExceptionController.register(apiPrefix);
            StopController.register(apiPrefix);
            TripController.register(apiPrefix);
            TripPatternController.register(apiPrefix);
            SnapshotController.register(apiPrefix);
            FeedInfoController.register(apiPrefix);
            FareController.register(apiPrefix);
        }

        // module-specific controllers
        if (isModuleEnabled("deployment")) {
            DeploymentController.register(apiPrefix);
        }
        if (isModuleEnabled("gtfsapi")) {
            GtfsApiController.register(apiPrefix);
        }
        if (isModuleEnabled("gtfsplus")) {
            GtfsPlusController.register(apiPrefix);
            gtfsPlusConfig = yamlMapper.readTree(new File("gtfsplus.yml"));
        }
        if (isModuleEnabled("user_admin")) {
            UserController.register(apiPrefix);
        }
        if (isModuleEnabled("dump")) {
            DumpController.register("/");
        }

        before(apiPrefix + "secure/*", (request, response) -> {
            if(request.requestMethod().equals("OPTIONS")) return;
            Auth0Connection.checkUser(request);
        });

        // lazy load by feed source id if new one is requested
//        if ("true".equals(getConfigPropertyAsText("modules.gtfsapi.load_on_fetch"))) {
//            before(apiPrefix + "*", (request, response) -> {
//                String feeds = request.queryParams("feed");
//                if (feeds != null) {
//                    String[] feedIds = feeds.split(",");
//                    for (String feedId : feedIds) {
//                        FeedSource fs = FeedSource.get(feedId);
//                        if (fs == null) {
//                            continue;
//                        }
//                        else if (!GtfsApiController.gtfsApi.registeredFeedSources.contains(fs.id) && !apiFeedSources.contains(fs.id)) {
//                            apiFeedSources.add(fs.id);
//
//                            LoadGtfsApiFeedJob loadJob = new LoadGtfsApiFeedJob(fs);
//                            new Thread(loadJob).start();
//                        halt(202, "Initializing feed load...");
//                        }
//                        else if (apiFeedSources.contains(fs.id) && !GtfsApiController.gtfsApi.registeredFeedSources.contains(fs.id)) {
//                            halt(202, "Loading feed, please try again later");
//                        }
//                    }
//
//                }
//            });
//        }
        // return "application/json" for all API routes
        after(apiPrefix + "*", (request, response) -> {
            response.type("application/json");
            response.header("Content-Encoding", "gzip");
        });

        // return js for any other request
        get("/main.js", (request, response) -> {
            try (InputStream stream = DataManager.class.getResourceAsStream("/public/main.js")) {
                return IOUtils.toString(stream);
            } catch (IOException e) {
                return null;
                // if the resource doesn't exist we just carry on.
            }
        });

        // return 404 for any api response that's not found
        get(apiPrefix + "*", (request, response) -> {
            halt(404);
            return null;
        });
        
        // return assets as byte array
        get("/assets/*", (request, response) -> {
            try (InputStream stream = DataManager.class.getResourceAsStream("/public" + request.pathInfo())) {
                return IOUtils.toByteArray(stream);
            } catch (IOException e) {
                return null;
            }
        });
        // return index.html for any sub-directory
        get("/*", (request, response) -> {
            response.type("text/html");
            try (InputStream stream = DataManager.class.getResourceAsStream("/public/index.html")) {
                return IOUtils.toString(stream);
            } catch (IOException e) {
                return null;
                // if the resource doesn't exist we just carry on.
            }
        });
        registerExternalResources();
    }


    public static JsonNode getConfigProperty(String name) {
        // try the server config first, then the main config
        JsonNode fromServerConfig = getConfigProperty(serverConfig, name);
        if(fromServerConfig != null) return fromServerConfig;

        return getConfigProperty(config, name);
    }

    public static JsonNode getConfigProperty(JsonNode config, String name) {
        String parts[] = name.split("\\.");
        JsonNode node = config;
        for(int i = 0; i < parts.length; i++) {
            if(node == null) return null;
            node = node.get(parts[i]);
        }
        return node;
    }

    public static String getConfigPropertyAsText(String name) {
        JsonNode node = getConfigProperty(name);
        return (node != null) ? node.asText() : null;
    }

    public static boolean isModuleEnabled(String moduleName) {
        return "true".equals(getConfigPropertyAsText("modules." + moduleName + ".enabled"));
    }

    private static void registerExternalResources() {

        if ("true".equals(getConfigPropertyAsText("extensions.mtc.enabled"))) {
            LOG.info("Registering MTC Resource");
            registerExternalResource(new MtcFeedResource());
        }

        if ("true".equals(getConfigPropertyAsText("extensions.transitland.enabled"))) {
            LOG.info("Registering TransitLand Resource");
            registerExternalResource(new TransitLandFeedResource());
        }

        if ("true".equals(getConfigPropertyAsText("extensions.transitfeeds.enabled"))) {
            LOG.info("Registering TransitFeeds Resource");
            registerExternalResource(new TransitFeedsFeedResource());
        }
    }
    private static void loadConfig (String[] args) throws IOException {
        FileInputStream in;

        if (args.length == 0)
            in = new FileInputStream(new File("config.yml"));
        else
            in = new FileInputStream(new File(args[0]));

        config = yamlMapper.readTree(in);
        serverConfig = yamlMapper.readTree(new File("config_server.yml"));
    }
    private static void registerExternalResource(ExternalFeedResource resource) {
        feedResources.put(resource.getResourceType(), resource);
    }
}
