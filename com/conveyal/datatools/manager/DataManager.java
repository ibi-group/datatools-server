package com.conveyal.datatools.manager;

import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.controllers.api.*;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.fasterxml.jackson.databind.node.ObjectNode;
import spark.Filter;
import spark.Request;
import spark.Response;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static spark.Spark.*;

public class DataManager {

    public static final Properties config = new Properties();

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

        staticFileLocation("/public");

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


    }

    private static void enableCORS(final String origin, final String methods, final String headers) {
        after((request, response) -> {
            response.header("Access-Control-Allow-Origin", origin);
            response.header("Access-Control-Request-Method", methods);
            response.header("Access-Control-Allow-Headers", headers);
        });
    }
}
