package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.Serializable;

import static spark.Spark.get;

/**
 * Created by demory on 3/15/16.
 */

public class ConfigController {

    public static final Logger LOG = LoggerFactory.getLogger(ConfigController.class);

    public static Config getConfig(Request req, Response res) throws JsonProcessingException {
        return new Config();
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "config", ConfigController::getConfig, JsonUtil.objectMapper::writeValueAsString);
    }
}

class Config implements Serializable {
    public final String auth0Domain = DataManager.config.getProperty("application.auth0.domain");
    public final String auth0ClientId = DataManager.config.getProperty("application.auth0.client_id");
    public final String editorUrl = DataManager.config.getProperty("application.editor_url");
    public final String userAdminUrl = DataManager.config.getProperty("application.user_admin_url");
    public final String logo = DataManager.config.getProperty("application.logo");
    public final String title = DataManager.config.getProperty("application.title");
}

