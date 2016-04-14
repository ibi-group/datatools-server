package com.conveyal.datatools.manager;

import com.conveyal.datatools.manager.auth.Auth0Connection;

import com.conveyal.datatools.manager.controllers.api.*;

import com.conveyal.datatools.manager.extensions.ExternalFeedResource;
import com.conveyal.datatools.manager.extensions.mtc.MtcFeedResource;
import com.conveyal.datatools.manager.extensions.transitfeeds.TransitFeedsFeedResource;
import com.conveyal.datatools.manager.extensions.transitland.TransitLandFeedResource;

import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.gtfs.api.ApiMain;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Filter;
import spark.Request;
import spark.Response;
import spark.utils.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static spark.Spark.*;

public class DataManager {

    public static final Logger LOG = LoggerFactory.getLogger(DataManager.class);

    public static JsonNode config;
    public static JsonNode serverConfig;

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

//        staticFileLocation("/public");

        enableCORS("*", "*", "Authorization");

        String apiPrefix = "/api/manager/";
        ConfigController.register(apiPrefix);
        ProjectController.register(apiPrefix);
        FeedSourceController.register(apiPrefix);
        FeedVersionController.register(apiPrefix);
        UserController.register(apiPrefix);
        //        ServiceAlertsController.register(apiPrefix);
        GtfsApiController.register(apiPrefix);

        GtfsPlusController.register(apiPrefix);

        before(apiPrefix + "secure/*", (request, response) -> {
            if(request.requestMethod().equals("OPTIONS")) return;
            Auth0Connection.checkUser(request);
        });

        after(apiPrefix + "*", (request, response) -> response.type("application/json"));

        get("/main.js", (request, response) -> {
            try (InputStream stream = ApiMain.class.getResourceAsStream("/public/main.js")) {
                return IOUtils.toString(stream);
            } catch (IOException e) {
                return null;
                // if the resource doesn't exist we just carry on.
            }
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
        registerExternalResources();
    }

    private static void enableCORS(final String origin, final String methods, final String headers) {
        after((request, response) -> {
            response.header("Access-Control-Allow-Origin", origin);
            response.header("Access-Control-Request-Method", methods);
            response.header("Access-Control-Allow-Headers", headers);
        });
    }

    private static void registerExternalResources() {

        String mtcEnabled = config.get("extensions").get("mtc").get("enabled").asText();
        if (mtcEnabled != null && mtcEnabled.equals("true")) {
            LOG.info("Registering MTC Resource");
            registerExternalResource(new MtcFeedResource());
        }

        String transitLandEnabled = config.get("extensions").get("transitland").get("enabled").asText();
        if (transitLandEnabled != null && transitLandEnabled.equals("true")) {
            LOG.info("Registering TransitLand Resource");
            registerExternalResource(new TransitLandFeedResource());
        }

        String transitFeedsEnabled = config.get("extensions").get("transitfeeds").get("enabled").asText();
        if (transitFeedsEnabled != null && transitFeedsEnabled.equals("true")) {
            LOG.info("Registering TransitFeeds Resource");
            registerExternalResource(new TransitFeedsFeedResource());
        }
    }

    private static void registerExternalResource(ExternalFeedResource resource) {
        feedResources.put(resource.getResourceType(), resource);
    }
}
