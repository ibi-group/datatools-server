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

public class AppInfoController {
    public static final Logger LOG = LoggerFactory.getLogger(AppInfoController.class);

    public static Map<String, String> getInfo(Request req, Response res) {
        // TODO: convert into a POJO if more stuff is needed here
        Map<String, String> json = new HashMap<>();
        json.put("repoUrl", repoUrl);
        json.put("commit", commit);
        return json;
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "public/appinfo", AppInfoController::getInfo, JsonUtil.objectMapper::writeValueAsString);
    }
}
