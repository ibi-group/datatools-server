package com.conveyal.datatools.manager;

import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.manager.auth.Auth0Connection;

import com.conveyal.datatools.manager.controllers.DumpController;
import com.conveyal.datatools.manager.controllers.api.*;
import com.conveyal.datatools.editor.controllers.api.*;

import com.conveyal.datatools.manager.extensions.ExternalFeedResource;
import com.conveyal.datatools.manager.extensions.mtc.MtcFeedResource;
import com.conveyal.datatools.manager.extensions.transitfeeds.TransitFeedsFeedResource;
import com.conveyal.datatools.manager.extensions.transitland.TransitLandFeedResource;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.CorsFilter;
import com.conveyal.gtfs.GTFS;
import com.conveyal.gtfs.api.GraphQLMain;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.Resources;
import org.apache.commons.io.Charsets;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.utils.IOUtils;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
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

    public static Map<String, ConcurrentHashSet<MonitorableJob>> userJobsMap = new ConcurrentHashMap<>();

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
    // TODO: move gtfs-api routes to gtfs path and add auth
    // private static final String GTFS_API_PREFIX = API_PREFIX + "gtfs/";
    private static final String GTFS_API_PREFIX = API_PREFIX;
    public static final String EDITOR_API_PREFIX = "/api/editor/";
    public static final String publicPath = "(" + DataManager.API_PREFIX + "|" + DataManager.EDITOR_API_PREFIX + ")public/.*";
    public static final String DEFAULT_ENV = "configurations/default/env.yml";
    public static final String DEFAULT_CONFIG = "configurations/default/server.yml";
//    public static FeedStore feedStore;
    public static DataSource GTFS_DATA_SOURCE;
//    public static Persistence persistence;

    public static void main(String[] args) throws IOException {

        // load config
        loadConfig(args);

        // FIXME: initialize feedStore here instaed of FeedVersion?
//        feedStore = new FeedStore();
        // FIXME: hack to statically load FeedStore
        LOG.info(FeedStore.class.getSimpleName());

        // set port
        if (getConfigProperty("application.port") != null) {
            port(Integer.parseInt(getConfigPropertyAsText("application.port")));
        }
        useS3 = "true".equals(getConfigPropertyAsText("application.data.use_s3_storage"));

        GTFS_DATA_SOURCE = GTFS.createDataSource(
                getConfigPropertyAsText("GTFS_DATABASE_URL"),
                getConfigPropertyAsText("GTFS_DATABASE_USER"),
                getConfigPropertyAsText("GTFS_DATABASE_PASSWORD")
        );

        feedBucket = getConfigPropertyAsText("application.data.gtfs_s3_bucket");
        awsRole = getConfigPropertyAsText("application.data.aws_role");
        bucketFolder = FeedStore.s3Prefix;

        // initialize GTFS GraphQL API service
        GraphQLMain.initialize(GTFS_DATA_SOURCE, API_PREFIX);
        LOG.info("Initialized gtfs-api at localhost:port{}", API_PREFIX);

        Persistence.initialize();

        // initialize map of auto fetched projects
        for (Project project : Persistence.projects.getAll()) {
            if (project.autoFetchFeeds) {
                ScheduledFuture scheduledFuture = ProjectController.scheduleAutoFeedFetch(project, 1);
                autoFetchMap.put(project.id, scheduledFuture);
            }
        }

        registerRoutes();

        registerExternalResources();
    }

    /**
     * Register API routes with Spark. This register core application routes, any routes associated with optional
     * modules and sets other core routes (e.g., 404 response) and response headers (e.g., API content type is JSON).
     */
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
//            GisController.register(EDITOR_API_PREFIX);
        }

        // log all exceptions to system.out
        exception(Exception.class, (e, req, res) -> LOG.error("error", e));

        // module-specific controllers
        if (isModuleEnabled("deployment")) {
            DeploymentController.register(API_PREFIX);
        }
        if (isModuleEnabled("gtfsapi")) {
            GtfsApiController.register(GTFS_API_PREFIX);
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

        // FIXME: add auth check for gtfs-api. Should access to certain feeds be restricted by feedId or namespace?
//        before(GTFS_API_PREFIX + "*", (request, response) -> {
//            if(request.requestMethod().equals("OPTIONS")) return;
//            Auth0Connection.checkUser(request);
//        });

        // return "application/json" for all API routes
        after(API_PREFIX + "*", (request, response) -> {
//            LOG.info(request.pathInfo());
            response.type("application/json");
            response.header("Content-Encoding", "gzip");
        });
        // load index.html
        InputStream stream = DataManager.class.getResourceAsStream("/public/index.html");
        final String index = IOUtils.toString(stream).replace("${S3BUCKET}", getConfigPropertyAsText("application.assets_bucket"));
        stream.close();

        // return 404 for any api response that's not found
        get(API_PREFIX + "*", (request, response) -> {
            halt(404, SparkUtils.formatJSON("Unknown error occurred.", 404));
            return null;
        });

        InputStream auth0Stream = DataManager.class.getResourceAsStream("/public/auth0-silent-callback.html");
        final String auth0html = IOUtils.toString(auth0Stream);
        auth0Stream.close();

        // auth0 silent callback
        get("/api/auth0-silent-callback", (request, response) -> {
            response.type("text/html");
            return auth0html;
        });

        // return index.html for any sub-directory
        get("/*", (request, response) -> {
            response.type("text/html");
            return index;
        });
    }

    /**
     * Convenience function to check existence of a config property (nested fields defined by dot notation
     * "data.use_s3_storage") in either server.yml or env.yml.
     */
    public static boolean hasConfigProperty(String name) {
        // try the server config first, then the main config
        boolean fromServerConfig = hasConfigProperty(serverConfig, name);
        if(fromServerConfig) return fromServerConfig;

        return hasConfigProperty(config, name);
    }

    private static boolean hasConfigProperty(JsonNode config, String name) {
        String parts[] = name.split("\\.");
        JsonNode node = config;
        for(int i = 0; i < parts.length; i++) {
            if(node == null) return false;
            node = node.get(parts[i]);
        }
        return node != null;
    }

    /**
     * Convenience function to get a config property (nested fields defined by dot notation "data.use_s3_storage") as
     * JsonNode. Checks server.yml, then env.yml, and finally returns null if property is not found.
     */
    public static JsonNode getConfigProperty(String name) {
        // try the server config first, then the main config
        JsonNode fromServerConfig = getConfigProperty(serverConfig, name);
        if(fromServerConfig != null) return fromServerConfig;

        return getConfigProperty(config, name);
    }

    private static JsonNode getConfigProperty(JsonNode config, String name) {
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

    /**
     * Get a config property (nested fields defined by dot notation "data.use_s3_storage") as text.
     */
    public static String getConfigPropertyAsText(String name) {
        JsonNode node = getConfigProperty(name);
        if (node != null) {
            return node.asText();
        } else {
            LOG.warn("Config property {} not found", name);
            return null;
        }
    }

    /**
     * Checks if an application module (e.g., editor, GTFS+) has been enabled. The UI must also have the module
     * enabled in order to use.
     */
    public static boolean isModuleEnabled(String moduleName) {
        return hasConfigProperty("modules." + moduleName) && "true".equals(getConfigPropertyAsText("modules." + moduleName + ".enabled"));
    }

    /**
     * Checks if an extension has been enabled. Extensions primarily define external resources
     * the application can sync with. The UI config must also have the extension enabled in order to use.
     */
    public static boolean isExtensionEnabled(String extensionName) {
        return hasConfigProperty("extensions." + extensionName) && "true".equals(getConfigPropertyAsText("extensions." + extensionName + ".enabled"));
    }

    /**
     * Check if extension is enabled and, if so, register it.
     */
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

    /**
     * Load config files from either program arguments or (if no args specified) from
     * default configuration file locations. Config fields are retrieved with getConfigProperty.
     */
    public static void loadConfig (String[] args) throws IOException {
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

    /**
     * Register external feed resource (e.g., transit.land) with feedResources map.
     * This essentially "enables" the syncing and storing feeds from the external resource.
     */
    private static void registerExternalResource(ExternalFeedResource resource) {
        feedResources.put(resource.getResourceType(), resource);
    }
}
