package com.conveyal.datatools.manager;

import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.manager.auth.Auth0Connection;

import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.controllers.DumpController;
import com.conveyal.datatools.manager.controllers.api.*;
import com.conveyal.datatools.editor.controllers.api.*;

import com.conveyal.datatools.manager.extensions.ExternalFeedResource;
import com.conveyal.datatools.manager.extensions.mtc.MtcFeedResource;
import com.conveyal.datatools.manager.extensions.transitfeeds.TransitFeedsFeedResource;
import com.conveyal.datatools.manager.extensions.transitland.TransitLandFeedResource;

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
import com.google.common.io.Resources;
import org.apache.commons.io.Charsets;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.utils.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static spark.Spark.*;

public class DataManager {

    public static final Logger LOG = LoggerFactory.getLogger(DataManager.class);

    public static JsonNode config;
    public static JsonNode serverConfig;

    public static JsonNode gtfsPlusConfig;
    public static JsonNode gtfsConfig;

    // TODO: define type for ExternalFeedResource Strings
    public static final Map<String, ExternalFeedResource> feedResources = new HashMap<>();

    public static ConcurrentHashMap<String, ConcurrentHashSet<MonitorableJob>> userJobsMap = new ConcurrentHashMap<>();

    public static Map<String, ScheduledFuture> autoFetchMap = new HashMap<>();
    public final static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());


    // heavy executor should contain long-lived CPU-intensive tasks (e.g., feed loading/validation)
    public static Executor heavyExecutor = Executors.newFixedThreadPool(4); // Runtime.getRuntime().availableProcessors()
    // light executor is for tasks for things that should finish quickly (e.g., email notifications)
    public static Executor lightExecutor = Executors.newSingleThreadExecutor();

    public static String feedBucket;
    public static String awsRole;
    public static String bucketFolder;

//    public final AmazonS3Client s3Client;
    public static boolean useS3;
    public static final String API_PREFIX = "/api/manager/";
    public static final String EDITOR_API_PREFIX = "/api/editor/";
    public static final String DEFAULT_ENV = "configurations/default/env.yml";
    public static final String DEFAULT_CONFIG = "configurations/default/server.yml";

    public static void main(String[] args) throws IOException {

        // load config
        loadConfig(args);

        // set port
        if (getConfigProperty("application.port") != null) {
            port(Integer.parseInt(getConfigPropertyAsText("application.port")));
        }
        useS3 = "true".equals(getConfigPropertyAsText("application.data.use_s3_storage"));

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
        awsRole = getConfigPropertyAsText("application.data.aws_role");
        bucketFolder = FeedStore.s3Prefix;

        // initialize gtfs-api
        if (useS3) {
            LOG.info("Initializing gtfs-api for bucket {}/{} and cache dir {}", feedBucket, bucketFolder, FeedStore.basePath);
            ApiMain.initialize(feedBucket, bucketFolder, FeedStore.basePath.getAbsolutePath());
        }
        else {
            LOG.info("Initializing gtfs-api cache locally (no s3 bucket) {}", FeedStore.basePath);
            // NOTE: null value must be passed here (even if feedBucket is not null)
            // because otherwise zip files will be DELETED!!! on removal from GTFSCache.
            ApiMain.initialize(null, FeedStore.basePath.getAbsolutePath());
        }

        registerRoutes();

        registerExternalResources();
    }

    private static void registerRoutes() throws IOException {
        CorsFilter.apply();

        // core controllers
        ProjectController.register(API_PREFIX);
        FeedSourceController.register(API_PREFIX);
        FeedVersionController.register(API_PREFIX);
        RegionController.register(API_PREFIX);
        NoteController.register(API_PREFIX);
        StatusController.register(API_PREFIX);
        OrganizationController.register(API_PREFIX);

        // Editor routes
        if (isModuleEnabled("editor")) {
            String gtfs = IOUtils.toString(DataManager.class.getResourceAsStream("/gtfs/gtfs.yml"));
            gtfsConfig = yamlMapper.readTree(gtfs);
            AgencyController.register(EDITOR_API_PREFIX);
            CalendarController.register(EDITOR_API_PREFIX);
            RouteController.register(EDITOR_API_PREFIX);
            RouteTypeController.register(EDITOR_API_PREFIX);
            ScheduleExceptionController.register(EDITOR_API_PREFIX);
            StopController.register(EDITOR_API_PREFIX);
            TripController.register(EDITOR_API_PREFIX);
            TripPatternController.register(EDITOR_API_PREFIX);
            SnapshotController.register(EDITOR_API_PREFIX);
            FeedInfoController.register(EDITOR_API_PREFIX);
            FareController.register(EDITOR_API_PREFIX);
            GisController.register(EDITOR_API_PREFIX);
        }

        // log all exceptions to system.out
        exception(Exception.class, (e, req, res) -> LOG.error("error", e));

        // module-specific controllers
        if (isModuleEnabled("deployment")) {
            DeploymentController.register(API_PREFIX);
        }
        if (isModuleEnabled("gtfsapi")) {
            GtfsApiController.register(API_PREFIX);
        }
        if (isModuleEnabled("gtfsplus")) {
            GtfsPlusController.register(API_PREFIX);
            URL gtfsplus = DataManager.class.getResource("/gtfs/gtfsplus.yml");
            gtfsPlusConfig = yamlMapper.readTree(Resources.toString(gtfsplus, Charsets.UTF_8));
        }
        if (isModuleEnabled("user_admin")) {
            UserController.register(API_PREFIX);
        }
        if (isModuleEnabled("dump")) {
            DumpController.register("/");
        }

        before(EDITOR_API_PREFIX + "secure/*", ((request, response) -> {
            Auth0Connection.checkUser(request);
            Auth0Connection.checkEditPrivileges(request);
        }));

        before(API_PREFIX + "secure/*", (request, response) -> {
            if(request.requestMethod().equals("OPTIONS")) return;
            Auth0Connection.checkUser(request);
        });

        // return "application/json" for all API routes
        after(API_PREFIX + "*", (request, response) -> {
//            LOG.info(request.pathInfo());
            response.type("application/json");
            response.header("Content-Encoding", "gzip");
        });
        // load index.html
        InputStream stream = DataManager.class.getResourceAsStream("/public/index.html");
        String index = IOUtils.toString(stream).replace("${S3BUCKET}", getConfigPropertyAsText("application.assets_bucket"));
        stream.close();

        // return 404 for any api response that's not found
        get(API_PREFIX + "*", (request, response) -> {
            halt(404, SparkUtils.formatJSON("Unknown error occurred.", 404));
            return null;
        });

        // return index.html for any sub-directory
        get("/*", (request, response) -> {
            response.type("text/html");
            return index;
        });
    }

    public static boolean hasConfigProperty(String name) {
        // try the server config first, then the main config
        boolean fromServerConfig = hasConfigProperty(serverConfig, name);
        if(fromServerConfig) return fromServerConfig;

        return hasConfigProperty(config, name);
    }

    public static boolean hasConfigProperty(JsonNode config, String name) {
        String parts[] = name.split("\\.");
        JsonNode node = config;
        for(int i = 0; i < parts.length; i++) {
            if(node == null) return false;
            node = node.get(parts[i]);
        }
        return node != null;
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
            if(node == null) {
                LOG.warn("Config property {} not found", name);
                return null;
            }
            node = node.get(parts[i]);
        }
        return node;
    }

    public static String getConfigPropertyAsText(String name) {
        JsonNode node = getConfigProperty(name);
        if (node != null) {
            return node.asText();
        } else {
            LOG.warn("Config property {} not found", name);
            return null;
        }
    }

    public static boolean isModuleEnabled(String moduleName) {
        return hasConfigProperty("modules." + moduleName) && "true".equals(getConfigPropertyAsText("modules." + moduleName + ".enabled"));
    }

    public static boolean isExtensionEnabled(String extensionName) {
        return hasConfigProperty("extensions." + extensionName) && "true".equals(getConfigPropertyAsText("extensions." + extensionName + ".enabled"));
    }

    private static void registerExternalResources() {

        if (isExtensionEnabled("mtc")) {
            LOG.info("Registering MTC Resource");
            registerExternalResource(new MtcFeedResource());
        }

        if (isExtensionEnabled("transitland")) {
            LOG.info("Registering TransitLand Resource");
            registerExternalResource(new TransitLandFeedResource());
        }

        if (isExtensionEnabled("transitfeeds")) {
            LOG.info("Registering TransitFeeds Resource");
            registerExternalResource(new TransitFeedsFeedResource());
        }
    }
    private static void loadConfig (String[] args) throws IOException {
        FileInputStream configStream;
        FileInputStream serverConfigStream;

        if (args.length == 0) {
            LOG.warn("Using default env.yml: {}", DEFAULT_ENV);
            LOG.warn("Using default server.yml: {}", DEFAULT_CONFIG);
            configStream = new FileInputStream(new File(DEFAULT_ENV));
            serverConfigStream = new FileInputStream(new File(DEFAULT_CONFIG));
        }
        else {
            LOG.info("Loading env.yml: {}", args[0]);
            LOG.info("Loading server.yml: {}", args[1]);
            configStream = new FileInputStream(new File(args[0]));
            serverConfigStream = new FileInputStream(new File(args[1]));
        }

        config = yamlMapper.readTree(configStream);
        serverConfig = yamlMapper.readTree(serverConfigStream);
    }
    private static void registerExternalResource(ExternalFeedResource resource) {
        feedResources.put(resource.getResourceType(), resource);
    }
}
