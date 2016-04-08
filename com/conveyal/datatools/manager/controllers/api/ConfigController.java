package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.Serializable;
import java.util.Collection;

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
    public final String auth0Domain = DataManager.config.get("auth0").get("domain").asText();
    public final String auth0ClientId = DataManager.config.get("auth0").get("client_id").asText();
    public final String editorUrl = DataManager.config.get("modules").get("editor").get("url").asText();
    public final String alertsUrl = DataManager.config.get("modules").get("alerts").get("url").asText();
    public final String userAdminUrl = DataManager.config.get("modules").get("user_admin").get("url").asText();
    public final String logo = DataManager.config.get("application").get("logo").asText();
    public final String title = DataManager.config.get("application").get("title").asText();
    public final Collection<String> resourceTypes = DataManager.feedResources.keySet();
}

