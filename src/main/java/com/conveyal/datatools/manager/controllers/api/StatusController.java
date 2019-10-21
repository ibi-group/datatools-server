package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.common.utils.RequestSummary;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;
import static spark.Spark.delete;
import static spark.Spark.get;

/**
 * Created by landon on 6/13/16.
 */
public class StatusController {
    private static final Logger LOG = LoggerFactory.getLogger(ProjectController.class);

    private static JsonManager<MonitorableJob.Status> json =
        new JsonManager<>(MonitorableJob.Status.class, JsonViews.UserInterface.class);

    /**
     * Admin API route to return active jobs for all application users.
     */
    private static Set<MonitorableJob> getAllJobsRoute(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        if (!userProfile.canAdministerApplication()) {
            logMessageAndHalt(req, 401, "User not authorized to view all jobs");
        }
        return getAllJobs();
    }

    /**
     * Admin API route to return latest requests made to applications.
     */
    private static List<RequestSummary> getAllRequestsRoute(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        if (!userProfile.canAdministerApplication()) {
            logMessageAndHalt(req, 401, "User not authorized to view all requests");
        }
        return DataManager.lastRequestForUser.values().stream()
            .sorted(Comparator.comparingLong(RequestSummary::getTime).reversed())
            .collect(Collectors.toList());
    }

    public static Set<MonitorableJob> getAllJobs() {
        return DataManager.userJobsMap.values().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    /**
     * API route that returns single job by ID from among the jobs for the currently authenticated user.
     */
    private static MonitorableJob getOneJobRoute(Request req, Response res) {
        String jobId = req.params("jobId");
        Auth0UserProfile userProfile = req.attribute("user");
        // FIXME: refactor underscore in user_id methods
        String userId = userProfile.getUser_id();
        return getJobById(userId, jobId, true);
    }

    /**
     * API route that cancels a single job by ID.
     */
    // TODO Add ability to cancel job. This requires some changes to how these jobs are executed. It appears that
    //  only scheduled jobs can be canceled.
//    private static MonitorableJob cancelJob(Request req, Response res) {
//        String jobId = req.params("jobId");
//        Auth0UserProfile userProfile = req.attribute("user");
//        // FIXME: refactor underscore in user_id methods
//        String userId = userProfile.getUser_id();
//        MonitorableJob job = getJobById(userId, jobId, true);
//        return job;
//    }

    /** Shorthand method for getting a single job by job ID. */
    public static MonitorableJob getJobByJobId(String jobId) {
        for (MonitorableJob job : getAllJobs()) if (job.jobId.equals(jobId)) return job;
        return null;
    }

    /**
     * Gets a job by user ID and job ID.
     * @param clearCompleted if true, remove requested job if it has completed or errored
     */
    public static MonitorableJob getJobById(String userId, String jobId, boolean clearCompleted) {
        // Get jobs set directly from userJobsMap because we may remove an element from it below.
        Set<MonitorableJob> userJobs = DataManager.userJobsMap.get(userId);
        if (userJobs == null) {
            return null;
        }
        for (MonitorableJob job : userJobs) {
            if (job.jobId.equals(jobId)) {
                if (clearCompleted && (job.status.completed || job.status.error)) {
                    // remove job if completed or errored
                    userJobs.remove(job);
                }
                return job;
            }
        }
        // if job is not found (because it doesn't exist or was completed).
        return null;
    }

    /**
     * API route that returns a set of active jobs for the currently authenticated user.
     */
    public static Set<MonitorableJob> getUserJobsRoute(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        // FIXME: refactor underscore in user_id methods
        String userId = userProfile.getUser_id();
        // Get a copy of all existing jobs before we purge the completed ones.
        return getJobsByUserId(userId, true);
    }

    /**
     * Convenience wrapper method to retrieve all jobs for {@link Auth0UserProfile}.
     */
    public static Set<MonitorableJob> getJobsForUser(Auth0UserProfile user) {
        if (user == null) {
            LOG.warn("Null user passed to getJobsForUser!");
            return Sets.newConcurrentHashSet();
        }
        return getJobsByUserId(user.getUser_id(), false);
    }

    /**
     * Get set of active jobs by user ID. If there are no active jobs, return a new set.
     *
     * NOTE: this should be a concurrent hash set so that it is threadsafe.
     *
     * @param clearCompleted if true, remove all completed and errored jobs for this user.
     */
    private static Set<MonitorableJob> getJobsByUserId(String userId, boolean clearCompleted) {
        Set<MonitorableJob> allJobsForUser = DataManager.userJobsMap.get(userId);
        if (allJobsForUser == null) {
            return Sets.newConcurrentHashSet();
        }
        if (clearCompleted) {
            // Any active jobs will still have their status updated, so they need to be retrieved again with any status
            // updates. All completed or errored jobs are in their final state and will not be updated any longer, so we
            // remove them once the client has seen them.
            Set<MonitorableJob> jobsStillActive = filterActiveJobs(allJobsForUser);

            DataManager.userJobsMap.put(userId, jobsStillActive);
        }
        return allJobsForUser;
    }

    public static Set<MonitorableJob> filterActiveJobs(Set<MonitorableJob> jobs) {
        // Note: this must be a thread-safe set in case it is placed into the DataManager#userJobsMap.
        Set<MonitorableJob> jobsStillActive = Sets.newConcurrentHashSet();
        jobs.stream()
                .filter(job -> !job.status.completed && !job.status.error)
                .forEach(jobsStillActive::add);
        return jobsStillActive;
    }

    public static void register (String apiPrefix) {

        get(apiPrefix + "secure/status/requests", StatusController::getAllRequestsRoute, json::write);
        // These endpoints return all jobs for the current user, all application jobs, or a specific job
        get(apiPrefix + "secure/status/jobs", StatusController::getUserJobsRoute, json::write);
        // FIXME Change endpoint for all jobs (to avoid overlap with jobId param)?
        get(apiPrefix + "secure/status/jobs/all", StatusController::getAllJobsRoute, json::write);
        get(apiPrefix + "secure/status/jobs/:jobId", StatusController::getOneJobRoute, json::write);
        // TODO Add ability to cancel job
//        delete(apiPrefix + "secure/status/jobs/:jobId", StatusController::cancelJob, json::write);
    }
}
