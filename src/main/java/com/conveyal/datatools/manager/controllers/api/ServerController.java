package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.utils.json.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.util.HashMap;
import java.util.Map;

import static com.conveyal.datatools.manager.DataManager.commit;
import static com.conveyal.datatools.manager.DataManager.repoUrl;
import static spark.Spark.get;

public class ServerController {
    public static final Logger LOG = LoggerFactory.getLogger(ServerController.class);

    public static Map<String, String> getInfo(Request req, Response res) {
        HashMap<String, String> json = new HashMap<>();
        json.put("repoUrl", repoUrl);
        json.put("commit", commit);
        return json;
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "public/serverinfo", ServerController::getInfo, JsonUtil.objectMapper::writeValueAsString);
    }
}
