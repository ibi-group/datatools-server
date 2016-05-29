package com.conveyal.datatools.manager;

import com.conveyal.datatools.manager.auth.Auth0Connection;

import com.conveyal.datatools.manager.controllers.DumpController;
import com.conveyal.datatools.manager.controllers.api.*;
import com.conveyal.datatools.editor.controllers.api.*;

import com.conveyal.datatools.manager.extensions.ExternalFeedResource;
import com.conveyal.datatools.manager.extensions.mtc.MtcFeedResource;
import com.conveyal.datatools.manager.extensions.transitfeeds.TransitFeedsFeedResource;
import com.conveyal.datatools.manager.extensions.transitland.TransitLandFeedResource;

import com.conveyal.datatools.manager.jobs.LoadGtfsApiFeedJob;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.utils.CorsFilter;
import com.conveyal.datatools.manager.utils.ResponseError;
import com.conveyal.gtfs.api.ApiMain;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.utils.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static spark.Spark.*;

public class DataManager {

    public static final Logger LOG = LoggerFactory.getLogger(DataManager.class);

    public static JsonNode config;
    public static JsonNode serverConfig;

    public static JsonNode gtfsPlusConfig;
    public static JsonNode gtfsConfig;

    public static final Map<String, ExternalFeedResource> feedResources = new HashMap<>();

    public static void main(String[] args) throws IOException {
        FileInputStream in;

        if (args.length == 0)
            in = new FileInputStream(new File("config.yml"));
        else
            in = new FileInputStream(new File(args[0]));

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        config = mapper.readTree(in);

        ObjectMapper serverMapper = new ObjectMapper(new YAMLFactory());
        serverConfig = serverMapper.readTree(new File("config_server.yml"));

        if(config.get("application").has("port")) {
            port(Integer.parseInt(config.get("application").get("port").asText()));
        }

        CorsFilter.apply();

        String apiPrefix = "/api/manager/";

        // core controllers
        ProjectController.register(apiPrefix);
        FeedSourceController.register(apiPrefix);
        FeedVersionController.register(apiPrefix);
        RegionController.register(apiPrefix);
        NoteController.register(apiPrefix);

        // Editor routes
        if ("true".equals(getConfigPropertyAsText("modules.editor.enabled"))) {
            gtfsConfig = mapper.readTree(new File("gtfs.yml"));
            AgencyController.register(apiPrefix);
            CalendarController.register(apiPrefix);
            RouteController.register(apiPrefix);
            RouteTypeController.register(apiPrefix);
            ScheduleExceptionController.register(apiPrefix);
            StopController.register(apiPrefix);
            TripController.register(apiPrefix);
            TripPatternController.register(apiPrefix);
        }

        // module-specific controllers
        if ("true".equals(getConfigPropertyAsText("modules.deployment.enabled"))) {
            DeploymentController.register(apiPrefix);
        }
        if ("true".equals(getConfigPropertyAsText("modules.gtfsapi.enabled"))) {
            GtfsApiController.register(apiPrefix);
        }
        if ("true".equals(getConfigPropertyAsText("modules.gtfsplus.enabled"))) {
            GtfsPlusController.register(apiPrefix);
            gtfsPlusConfig = mapper.readTree(new File("gtfsplus.yml"));
        }
        if ("true".equals(getConfigPropertyAsText("modules.user_admin.enabled"))) {
            UserController.register(apiPrefix);
        }
        if ("true".equals(getConfigPropertyAsText("modules.dump.enabled"))) {
            DumpController.register("/");
        }

        before(apiPrefix + "secure/*", (request, response) -> {
            if(request.requestMethod().equals("OPTIONS")) return;
            Auth0Connection.checkUser(request);
        });

        // lazy load feeds if new one is requested
        before(apiPrefix + "*", (request, response) -> {
            String feeds = request.queryParams("feed");
            if (feeds != null) {
                String[] feedIds = feeds.split(",");
                for (String feedId : feedIds) {
                    FeedSource fs = FeedSource.get(feedId);
                    if (fs != null && !GtfsApiController.gtfsApi.feedSources.keySet().contains(feedId)) {
                        new LoadGtfsApiFeedJob(fs).run();
//                        halt(503, "Loading feed, please try again later");
                    }
                }

            }
        });

        after(apiPrefix + "*", (request, response) -> {
            response.type("application/json");
            response.header("Content-Encoding", "gzip");
        });

        get("/main.js", (request, response) -> {
            try (InputStream stream = ApiMain.class.getResourceAsStream("/public/main.js")) {
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

        // return index.html for any sub-directory
        get("/*", (request, response) -> {
            response.type("text/html");
            try (InputStream stream = ApiMain.class.getResourceAsStream("/public/index.html")) {
                return IOUtils.toString(stream);
            } catch (IOException e) {
                return null;
                // if the resource doesn't exist we just carry on.
            }
        });
        exception(IllegalArgumentException.class,(e,req,res) -> {
            res.status(400);
            res.body(new Gson().toJson(new ResponseError(e)));
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

    private static void registerExternalResource(ExternalFeedResource resource) {
        feedResources.put(resource.getResourceType(), resource);
    }
}
