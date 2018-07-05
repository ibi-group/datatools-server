package com.conveyal.datatools.manager;

import com.conveyal.datatools.editor.controllers.EditorLockController;
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
import com.conveyal.datatools.manager.persistence.TransportNetworkCache;
import com.conveyal.datatools.common.utils.CorsFilter;
import com.conveyal.gtfs.GTFS;
import com.conveyal.gtfs.GraphQLMain;
import com.conveyal.gtfs.loader.Table;
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

import static com.conveyal.datatools.common.utils.SparkUtils.haltWithMessage;
import static com.conveyal.datatools.manager.auth.Auth0Connection.logRequest;
import static spark.Spark.*;

/**
 * This is the singleton where the application is initialized. It currently stores a number of static fields which are
 * referenced throughout the application.
 */
public class DataManager {
    private static final Logger LOG = LoggerFactory.getLogger(DataManager.class);

    // These fields hold YAML files that represent the server configuration.
    private static JsonNode envConfig;
    private static JsonNode serverConfig;

    // These fields hold YAML files that represent the GTFS and GTFS+ specifications.
    public static JsonNode gtfsPlusConfig;
    public static JsonNode gtfsConfig;

    // Contains the config-enabled ExternalFeedResource objects that define connections to third-party feed indexes
    // (e.g., transit.land, TransitFeeds.com)
    // TODO: define type for ExternalFeedResource Strings
    public static final Map<String, ExternalFeedResource> feedResources = new HashMap<>();

    // Stores jobs underway by user ID.
    public static Map<String, ConcurrentHashSet<MonitorableJob>> userJobsMap = new ConcurrentHashMap<>();

    // Caches r5 transport networks for use in generating isochrones
    public static final TransportNetworkCache transportNetworkCache = new TransportNetworkCache();

    // Stores ScheduledFuture objects that kick off runnable tasks (e.g., fetch project feeds at 2:00 AM).
    public static Map<String, ScheduledFuture> autoFetchMap = new HashMap<>();
    // Scheduled executor that handles running scheduled jobs.
    public final static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    // ObjectMapper that loads in YAML config files
    private static final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());


    // Heavy executor should contain long-lived CPU-intensive tasks (e.g., feed loading/validation)
    public static Executor heavyExecutor = Executors.newFixedThreadPool(4);
    // light executor is for tasks for things that should finish quickly (e.g., email notifications)
    public static Executor lightExecutor = Executors.newSingleThreadExecutor();

    public static String feedBucket;
    public static String bucketFolder;

    public static boolean useS3;
    private static final String API_PREFIX = "/api/manager/";
    private static final String GTFS_API_PREFIX = API_PREFIX + "secure/gtfs/";
    private static final String EDITOR_API_PREFIX = "/api/editor/";
    public static final String publicPath = "(" + API_PREFIX + "|" + EDITOR_API_PREFIX + ")public/.*";
    private static final String DEFAULT_ENV = "configurations/default/env.yml";
    private static final String DEFAULT_CONFIG = "configurations/default/server.yml";
    public static DataSource GTFS_DATA_SOURCE;

    public static void main(String[] args) throws IOException {

        initializeApplication(args);

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

    static void initializeApplication(String[] args) throws IOException {
        // Load configuration files (env.yml and server.yml).
        loadConfig(args);

        // FIXME: hack to statically load FeedStore
        LOG.info(FeedStore.class.getSimpleName());

        // Optionally set port for server. Otherwise, Spark defaults to 4567.
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
        bucketFolder = FeedStore.s3Prefix;

        // Initialize MongoDB storage
        Persistence.initialize();
    }

    /**
     * Register API routes with Spark. This register core application routes, any routes associated with optional
     * modules and sets other core routes (e.g., 404 response) and response headers (e.g., API content type is JSON).
     */
    static void registerRoutes() throws IOException {
        CorsFilter.apply();
        // Initialize GTFS GraphQL API service
        // FIXME: Add user permissions check to ensure user has access to feeds.
        GraphQLMain.initialize(GTFS_DATA_SOURCE, GTFS_API_PREFIX);
        // Register core API routes
        ProjectController.register(API_PREFIX);
        FeedSourceController.register(API_PREFIX);
        FeedVersionController.register(API_PREFIX);
        RegionController.register(API_PREFIX);
        NoteController.register(API_PREFIX);
        StatusController.register(API_PREFIX);
        OrganizationController.register(API_PREFIX);

        // Register editor API routes
        if (isModuleEnabled("editor")) {

            SnapshotController.register(EDITOR_API_PREFIX);
            EditorLockController.register(EDITOR_API_PREFIX);

            String gtfs = IOUtils.toString(DataManager.class.getResourceAsStream("/gtfs/gtfs.yml"));
            gtfsConfig = yamlMapper.readTree(gtfs);
            new EditorControllerImpl(EDITOR_API_PREFIX, Table.AGENCY, DataManager.GTFS_DATA_SOURCE);
            new EditorControllerImpl(EDITOR_API_PREFIX, Table.CALENDAR, DataManager.GTFS_DATA_SOURCE);
            new EditorControllerImpl(EDITOR_API_PREFIX, Table.FARE_ATTRIBUTES, DataManager.GTFS_DATA_SOURCE);
            new EditorControllerImpl(EDITOR_API_PREFIX, Table.FEED_INFO, DataManager.GTFS_DATA_SOURCE);
            new EditorControllerImpl(EDITOR_API_PREFIX, Table.ROUTES, DataManager.GTFS_DATA_SOURCE);
            // NOTE: Patterns controller handles updates to nested tables shapes, pattern stops, and frequencies.
            new EditorControllerImpl(EDITOR_API_PREFIX, Table.PATTERNS, DataManager.GTFS_DATA_SOURCE);
            new EditorControllerImpl(EDITOR_API_PREFIX, Table.SCHEDULE_EXCEPTIONS, DataManager.GTFS_DATA_SOURCE);
            new EditorControllerImpl(EDITOR_API_PREFIX, Table.STOPS, DataManager.GTFS_DATA_SOURCE);
            new EditorControllerImpl(EDITOR_API_PREFIX, Table.TRIPS, DataManager.GTFS_DATA_SOURCE);
            // TODO: Add transfers.txt controller?
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

        // FIXME: Add auth check for gtfs-api. This is tricky because it requires extracting the namespace argument from
        // the GraphQL query, which could be embedded in the query itself or in the variables JSON. We would then need
        // to check against both the snapshots and feed versions collections for the feed source ID to use in the
        // permissions check.
//        before(GTFS_API_PREFIX + "*", (request, response) -> {
//            Auth0Connection.checkGTFSPrivileges(request);
//        });

        // return "application/json" for all API routes
        before(API_PREFIX + "*", (request, response) -> {
            logRequest(request, response);
            response.type("application/json");
            response.header("Content-Encoding", "gzip");
        });
        before(EDITOR_API_PREFIX + "*", (request, response) -> {
            logRequest(request, response);
            response.type("application/json");
            response.header("Content-Encoding", "gzip");
        });
        // load index.html
        final String index = resourceToString("/public/index.html")
                .replace("${S3BUCKET}", getConfigPropertyAsText("application.assets_bucket"));
        final String auth0html = resourceToString("/public/auth0-silent-callback.html");

        // auth0 silent callback
        get("/api/auth0-silent-callback", (request, response) -> {
            response.type("text/html");
            return auth0html;
        });

        /////////////////    Final API routes     /////////////////////

        // Return 404 for any API path that is not configured.
        // IMPORTANT: Any API paths must be registered before this halt.
        get("/api/" + "*", (request, response) -> {
            haltWithMessage(404, "No API route configured for this path.");
            return null;
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
        return hasConfigProperty(serverConfig, name) || hasConfigProperty(envConfig, name);
    }

    private static String resourceToString (String resourceName) {
        try (InputStream stream = DataManager.class.getResourceAsStream(resourceName)) {
            return IOUtils.toString(stream);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Could not find resource.");
        }
    }

    private static boolean hasConfigProperty(JsonNode config, String name) {
        String parts[] = name.split("\\.");
        JsonNode node = config;
        for (int i = 0; i < parts.length; i++) {
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

        return getConfigProperty(envConfig, name);
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
    private static void loadConfig(String[] args) throws IOException {
        FileInputStream envConfigStream;
        FileInputStream serverConfigStream;

        if (args.length == 0) {
            LOG.warn("Using default env.yml: {}", DEFAULT_ENV);
            LOG.warn("Using default server.yml: {}", DEFAULT_CONFIG);
            envConfigStream = new FileInputStream(new File(DEFAULT_ENV));
            serverConfigStream = new FileInputStream(new File(DEFAULT_CONFIG));
        }
        else {
            LOG.info("Loading env.yml: {}", args[0]);
            LOG.info("Loading server.yml: {}", args[1]);
            envConfigStream = new FileInputStream(new File(args[0]));
            serverConfigStream = new FileInputStream(new File(args[1]));
        }

        envConfig = yamlMapper.readTree(envConfigStream);
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
