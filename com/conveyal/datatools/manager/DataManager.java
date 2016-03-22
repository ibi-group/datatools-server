package com.conveyal.datatools.manager;

import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.controllers.api.ConfigController;
import com.conveyal.datatools.manager.controllers.api.FeedSourceController;
import com.conveyal.datatools.manager.controllers.api.ProjectController;
import com.fasterxml.jackson.databind.node.ObjectNode;

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

        String apiPrefix = "/api/manager/";
        ConfigController.register(apiPrefix);
        ProjectController.register(apiPrefix);
        FeedSourceController.register(apiPrefix);

        before(apiPrefix + "secure/*", (request, response) -> {
            Auth0Connection.checkUser(request);
        });

    }
}
