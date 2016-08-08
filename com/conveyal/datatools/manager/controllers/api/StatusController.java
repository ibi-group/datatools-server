package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.util.HashSet;
import java.util.Set;

import static spark.Spark.*;
import static spark.Spark.delete;
import static spark.Spark.get;

/**
 * Created by landon on 6/13/16.
 */
public class StatusController {
    public static final Logger LOG = LoggerFactory.getLogger(ProjectController.class);

    public static JsonManager<MonitorableJob.Status> json =
            new JsonManager<>(MonitorableJob.Status.class, JsonViews.UserInterface.class);

    /*public static Object getStatus(Request req, Response res) {
//        Auth0UserProfile userProfile = req.attribute("user");
        String userId = req.params("id");
        System.out.println("getting status for: " + userId);
        return DataManager.userJobsMap.get(userId);
    }*/

    public static Object getUserJobs(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        String userId = userProfile.getUser_id();
        return DataManager.userJobsMap.containsKey(userId)
            ? DataManager.userJobsMap.get(userId).toArray()
            : new HashSet<>();
    }

    public static void register (String apiPrefix) {
        options(apiPrefix + "public/status", (q, s) -> "");
        get(apiPrefix + "secure/status/jobs", StatusController::getUserJobs, json::write);
    }
}
