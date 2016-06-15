package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.MonitorableJob;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

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

    public static Object getStatus(Request req, Response res) {
//        Auth0UserProfile userProfile = req.attribute("user");
        String userId = req.params("id");
        System.out.println("getting status for: " + userId);
        return DataManager.userJobsMap.get(userId);
    }

    public static Object getAllStatuses(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        if (!userProfile.canAdministerApplication()) {
            halt(403, "Not authorized to view all statuses");
            return null;
        }
        return DataManager.userJobsMap;
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "public/status/:id", StatusController::getStatus, json::write);
        options(apiPrefix + "public/status", (q, s) -> "");
        get(apiPrefix + "secure/status", StatusController::getAllStatuses, json::write);
//        post(apiPrefix + "secure/status", StatusController::createRegion, json::write);
//        put(apiPrefix + "secure/status/:id", StatusController::updateRegion, json::write);
//        delete(apiPrefix + "secure/status/:id", StatusController::deleteRegion, json::write);
//
//        // Public routes
//        get(apiPrefix + "public/status/:id", StatusController::getRegion, json::write);
//        get(apiPrefix + "public/status", StatusController::getAllRegions, json::write);

//        get(apiPrefix + "public/seedstatuss", StatusController::seedRegions, json::write);
    }
}
