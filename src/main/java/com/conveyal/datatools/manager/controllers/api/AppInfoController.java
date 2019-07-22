package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import static com.conveyal.datatools.manager.DataManager.commit;
import static com.conveyal.datatools.manager.DataManager.repoUrl;
import static spark.Spark.get;

/**
 * Controller that provides information about the application being run on the server.
 */
public class AppInfoController {
    public static final Logger LOG = LoggerFactory.getLogger(AppInfoController.class);

    /**
     * HTTP endpoint that provides commit hash for the version of the server being run as well as
     * shared configuration variables (e.g., the modules or extensions that are enabled).
     */
    private static ObjectNode getInfo(Request req, Response res) {
        // TODO: convert into a POJO if more stuff is needed here. Although there would need to be an accommodation for
        //  the config field, which has a structure that is dependent on whatever is included in server.yml.
        return JsonUtil.objectMapper.createObjectNode()
            .put("repoUrl", repoUrl)
            .put("commit", commit)
            // Add config variables. NOTE: this uses server.yml, which is not intended to have any secure information.
            .putPOJO("config", DataManager.getPublicConfig());
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "public/appinfo", AppInfoController::getInfo, JsonUtil.objectMapper::writeValueAsString);
    }
}
