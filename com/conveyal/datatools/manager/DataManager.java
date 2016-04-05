package com.conveyal.datatools.manager;

import com.conveyal.datatools.manager.auth.Auth0Connection;

import com.conveyal.datatools.manager.controllers.api.ConfigController;
import com.conveyal.datatools.manager.controllers.api.FeedSourceController;
import com.conveyal.datatools.manager.controllers.api.FeedVersionController;
import com.conveyal.datatools.manager.controllers.api.ProjectController;
import com.conveyal.datatools.manager.controllers.api.UserController;
import com.conveyal.datatools.manager.extensions.ExternalFeedResource;
import com.conveyal.datatools.manager.extensions.mtc.MtcFeedResource;
import com.conveyal.datatools.manager.extensions.transitfeeds.TransitFeedsFeedResource;
import com.conveyal.datatools.manager.extensions.transitland.TransitLandFeedResource;

import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.gtfs.api.ApiMain;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

    public static final Properties config = new Properties();

    public static final Map<String, ExternalFeedResource> feedResources = new HashMap<>();

    public static void main(String[] args) throws IOException {
        FileInputStream in;

        if (args.length == 0)
            in = new FileInputStream(new File("application.conf"));
        else
            in = new FileInputStream(new File(args[0]));

        config.load(in);

        if(config.containsKey("application.port")) {
            port(Integer.parseInt(config.getProperty("application.port")));
        }

//        staticFileLocation("/public");

        enableCORS("*", "*", "Authorization");

        String apiPrefix = "/api/manager/";
        ConfigController.register(apiPrefix);
        ProjectController.register(apiPrefix);
        FeedSourceController.register(apiPrefix);
        FeedVersionController.register(apiPrefix);
        UserController.register(apiPrefix);

        before(apiPrefix + "secure/*", (request, response) -> {
            if(request.requestMethod().equals("OPTIONS")) return;
            Auth0Connection.checkUser(request);
        });

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
        if (config.getProperty("application.extensions.mtc.enabled").equals("true")) {
            LOG.info("Registering MTC Resource");
            registerExternalResource(new MtcFeedResource());
        }

        if (config.getProperty("application.extensions.transitland.enabled").equals("true")) {
            LOG.info("Registering TransitLand Resource");
            registerExternalResource(new TransitLandFeedResource());
        }

        if (config.getProperty("application.extensions.transitfeeds.enabled").equals("true")) {
            LOG.info("Registering TransitFeeds Resource");
            registerExternalResource(new TransitFeedsFeedResource());
        }
    }

    private static void registerExternalResource(ExternalFeedResource resource) {
        feedResources.put(resource.getResourceType(), resource);
    }
}
