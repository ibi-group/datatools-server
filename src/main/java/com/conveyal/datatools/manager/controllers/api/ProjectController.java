package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.FetchProjectFeedsJob;
import com.conveyal.datatools.manager.jobs.MakePublicJob;
import com.conveyal.datatools.manager.jobs.MergeProjectFeedsJob;
import com.conveyal.datatools.manager.models.FeedDownloadToken;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.conveyal.datatools.common.utils.S3Utils.downloadFromS3;
import static com.conveyal.datatools.common.utils.SparkUtils.downloadFile;
import static com.conveyal.datatools.common.utils.SparkUtils.formatJobMessage;
import static com.conveyal.datatools.common.utils.SparkUtils.haltWithMessage;
import static com.conveyal.datatools.manager.DataManager.publicPath;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;
import static spark.Spark.put;

/**
 * Handlers for HTTP API requests that affect Projects.
 * These methods are mapped to API endpoints by Spark.
 * TODO we could probably have one generic controller for all data types, and use path elements from the URL to route to different typed persistence instances.
 */
@SuppressWarnings({"unused", "ThrowableNotThrown"})
public class ProjectController {

    // TODO We can probably replace this with something from Mongo so we use one JSON serializer / deserializer throughout
    public static JsonManager<Project> json =
            new JsonManager<>(Project.class, JsonViews.UserInterface.class);

    public static final Logger LOG = LoggerFactory.getLogger(ProjectController.class);

    /**
     * @return a list of all projects that are public or visible given the current user and organization.
     */
    private static Collection<Project> getAllProjects(Request req, Response res) throws JsonProcessingException {
        Auth0UserProfile userProfile = req.attribute("user");
        // TODO: move this filtering into database query to reduce traffic / memory
        return Persistence.projects.getAll().stream()
                .filter(p -> req.pathInfo().matches(publicPath) || userProfile.hasProject(p.id, p.organizationId))
                .map(p -> checkProjectPermissions(req, p, "view"))
                .collect(Collectors.toList());
    }

    /**
     * @return a Project object for the UUID included in the request.
     */
    private static Project getProject(Request req, Response res) {
        return requestProjectById(req, "view");
    }

    /**
     * Create a new Project and store it, setting fields according to the JSON in the request body.
     * @return the newly created Project with all the supplied fields, as it appears in the database.
     */
    private static Project createProject(Request req, Response res) {
        // TODO error handling when request is bogus
        // TODO factor out user profile fetching, permissions checks etc.
        Auth0UserProfile userProfile = req.attribute("user");
        Document newProjectFields = Document.parse(req.body());
        String organizationId = (String) newProjectFields.get("organizationId");
        boolean allowedToCreate = userProfile.canAdministerApplication() || userProfile.canAdministerOrganization(organizationId);
        // Data manager can operate without organizations for now, so we (hackishly/insecurely) deactivate permissions here
        if (organizationId == null) allowedToCreate = true;
        if (allowedToCreate) {
            Project newlyStoredProject = Persistence.projects.create(req.body());
            return newlyStoredProject;
        } else {
            haltWithMessage(req, 403, "Not authorized to create a project on organization " + organizationId);
            return null;
        }
    }

    /**
     * Update fields in the Project with the given UUID. The fields to be updated are supplied as JSON in the request
     * body.
     * @return the Project as it appears in the database after the update.
     */
    private static Project updateProject(Request req, Response res) throws IOException {
        // Fetch the project once to check permissions
        requestProjectById(req, "manage");
        try {
            String id = req.params("id");
            Document updateDocument = Document.parse(req.body());
            Project updatedProject = Persistence.projects.update(id, req.body());
            // Catch updates to auto-fetch params, and update the autofetch schedule accordingly.
            // TODO factor out into generic update hooks, or at least separate method
            if (updateDocument.containsKey("autoFetchHour")
                    || updateDocument.containsKey("autoFetchMinute")
                    || updateDocument.containsKey("autoFetchFeeds")
                    || updateDocument.containsKey("defaultTimeZone")) {
                // If auto fetch flag is turned on
                if (updatedProject.autoFetchFeeds) {
                    ScheduledFuture fetchAction = scheduleAutoFeedFetch(updatedProject, 1);
                    DataManager.autoFetchMap.put(updatedProject.id, fetchAction);
                } else {
                    // otherwise, cancel any existing task for this id
                    cancelAutoFetch(updatedProject.id);
                }
            }
            return updatedProject;
        } catch (Exception e) {
            e.printStackTrace();
            haltWithMessage(req, 400, "Error updating project");
            return null;
        }
    }

    /**
     * Delete the project for the UUID given in the request.
     */
    private static Project deleteProject(Request req, Response res) {
        // Fetch project first to check permissions, and so we can return the deleted project after deletion.
        Project project = requestProjectById(req, "manage");
        boolean successfullyDeleted = project.delete();
        if (!successfullyDeleted) {
            haltWithMessage(req, 400, "Did not delete project.");
        }
        return project;
    }

    /**
     * Manually fetch a feed all feeds in the project as a one-off operation, when the user clicks a button to request it.
     */
    public static Boolean fetch(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        Project p = requestProjectById(req, "manage");
        FetchProjectFeedsJob fetchProjectFeedsJob = new FetchProjectFeedsJob(p, userProfile.getUser_id());
        // This job is runnable because sometimes we schedule the task for a later time, but here we call it immediately
        // because it is short lived and just cues up more work.
        fetchProjectFeedsJob.run();
        return true;
    }

    /**
     * Public helper function that returns the requested object if the user has permissions for the specified action.
     * FIXME why can't we do this checking by passing in the project ID rather than the whole request?
     * FIXME: eliminate all stringly typed variables (action)
     * @param req spark Request object from API request
     * @param action action type (either "view" or "manage")
     * @return requested project
     */
    private static Project requestProjectById (Request req, String action) {
        String id = req.params("id");
        if (id == null) {
            haltWithMessage(req, 400, "Please specify id param");
        }
        return checkProjectPermissions(req, Persistence.projects.getById(id), action);
    }

    /**
     * Given a project object, this checks the user's permissions to take some specific action on it.
     * If the user does not have permission the Spark request is halted with an error.
     * TODO: remove all Spark halt calls from data manipulation functions, API implementation is leaking into data model
     * If the user does have permission we return the same project object that was input, but with the feedSources nulled out.
     * In the special case that the user is not logged in and is therefore only looking at public objects, the feed
     * sources list is replaced with one that only contains publicly visible feed sources.
     * This is because the UI only uses Project objects with embedded feedSources in the landing page, nowhere else.
     * That fetch with embedded feedSources should be done with something like GraphQL generating multiple backend
     * fetches, not with a field that's only populated and returned in special cases.
     * FIXME: this is a method with side effects and no clear single purpose, in terms of transformation of input to output.
     */
    private static Project checkProjectPermissions(Request req, Project project, String action) {

        Auth0UserProfile userProfile = req.attribute("user");
        // Check if request was made by a user that is not logged in
        boolean publicFilter = req.pathInfo().matches(publicPath);

        // check for null project
        if (project == null) {
            haltWithMessage(req, 400, "Project ID does not exist");
            return null;
        }

        boolean authorized;
        switch (action) {
            // TODO: limit create action to app/org admins? see code currently in createProject.
//            case "create":
//                authorized = userProfile.canAdministerOrganization(p.organizationId);
//                break;
            case "manage":
                authorized = userProfile.canAdministerProject(project.id, project.organizationId);
                break;
            case "view":
                // request only authorized if not via public path and user can view
                authorized = !publicFilter && userProfile.hasProject(project.id, project.organizationId);
                break;
            default:
                authorized = false;
                break;
        }

        // If the user is not logged in, include only public feed sources
        if (publicFilter){
            project.feedSources = project.retrieveProjectFeedSources().stream()
                    .filter(fs -> fs.isPublic)
                    .collect(Collectors.toList());
        } else {
            project.feedSources = null;
            if (!authorized) {
                haltWithMessage(req, 403, "User not authorized to perform action on project");
                return null;
            }
        }
        // if we make it here, user has permission and this is a valid project.
        return project;
    }

    /**
     * HTTP endpoint to initialize a merge project feeds operation. Client should check the job status endpoint for the
     * completion of merge project feeds job. On successful completion of the job, the client should make a GET request
     * to getFeedDownloadCredentials with the project ID to obtain either temporary S3 credentials or a download token
     * (depending on application configuration "application.data.use_s3_storage") to download the zip file.
     */
    private static String downloadMergedFeed(Request req, Response res) {
        Project project = requestProjectById(req, "view");
        Auth0UserProfile userProfile = req.attribute("user");
        // TODO: make this an authenticated call?
        MergeProjectFeedsJob mergeProjectFeedsJob = new MergeProjectFeedsJob(project, userProfile.getUser_id());
        DataManager.heavyExecutor.execute(mergeProjectFeedsJob);
        // Return job ID to requester for monitoring job status.
        return formatJobMessage(mergeProjectFeedsJob.jobId, "Merge operation is processing.");
    }

    /**
     * Returns credentials that a client may use to then download a feed version. Functionality
     * changes depending on whether application.data.use_s3_storage config property is true.
     */
    public static Object getFeedDownloadCredentials(Request req, Response res) {
        Project project = requestProjectById(req, "view");

        // if storing feeds on s3, return temporary s3 credentials for that zip file
        if (DataManager.useS3) {
            // Return presigned download link if using S3.
            String key = String.format("project/%s.zip", project.id);
            return downloadFromS3(FeedStore.s3Client, DataManager.feedBucket, key, false, res);
        } else {
            // when feeds are stored locally, single-use download token will still be used
            FeedDownloadToken token = new FeedDownloadToken(project);
            Persistence.tokens.create(token);
            return token;
        }
    }

    /**
     * Copy all the latest feed versions for all public feed sources in this project to a bucket on S3.
     * Updates the index.html document that serves as a listing of those objects on S3.
     * This is often referred to as "deploying" the project.
     */
    private static boolean publishPublicFeeds(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        String id = req.params("id");
        if (id == null) {
            haltWithMessage(req, 400, "must provide project id!");
        }
        Project p = Persistence.projects.getById(id);
        if (p == null) {
            haltWithMessage(req, 400, "no such project!");
        }
        // Run this as a synchronous job; if it proves to be too slow we will change to asynchronous.
        new MakePublicJob(p, userProfile.getUser_id()).run();
        return true;
    }

    /**
     * Spark endpoint to synchronize this project's feed sources with another website or service that maintains an
     * index of GTFS data. This action is triggered manually by a UI button and for now never happens automatically.
     * An ExternalFeedResource of the specified type must be present in DataManager.feedResources
     */
    private static Project thirdPartySync(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        String id = req.params("id");
        Project proj = Persistence.projects.getById(id);

        String syncType = req.params("type");

        if (!userProfile.canAdministerProject(proj.id, proj.organizationId)) {
            haltWithMessage(req, 403, "Third-party sync not permitted for user.");
        }

        LOG.info("syncing with third party " + syncType);
        if(DataManager.feedResources.containsKey(syncType)) {
            DataManager.feedResources.get(syncType).importFeedsForProject(proj, req.headers("Authorization"));
            return proj;
        }

        haltWithMessage(req, 404, syncType + " sync type not enabled for application.");
        return null;
    }

    /**
     * Schedule an action that fetches all the feeds in the given project according to the autoFetch fields of that project.
     * Currently feeds are not auto-fetched independently, they must be all fetched together as part of a project.
     * This method is called when a Project's auto-fetch settings are updated, and when the system starts up to populate
     * the auto-fetch scheduler.
     */
    public static ScheduledFuture scheduleAutoFeedFetch (Project project, int intervalInDays) {
        TimeUnit minutes = TimeUnit.MINUTES;
        try {
            // First cancel any already scheduled auto fetch task for this project id.
            cancelAutoFetch(project.id);

            ZoneId timezone;
            try {
                timezone = ZoneId.of(project.defaultTimeZone);
            }catch(Exception e){
                timezone = ZoneId.of("America/New_York");
            }
            LOG.info("Scheduling auto-fetch for projectID: {}", project.id);

            // NOW in default timezone
            ZonedDateTime now = ZonedDateTime.ofInstant(Instant.now(), timezone);

            // Scheduled start time
            ZonedDateTime startTime = LocalDateTime.of(LocalDate.now(),
                    LocalTime.of(project.autoFetchHour, project.autoFetchMinute)).atZone(timezone);
            LOG.info("Now: {}", now.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
            LOG.info("Scheduled start time: {}", startTime.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));

            // Get diff between start time and current time
            long diffInMinutes = (startTime.toEpochSecond() - now.toEpochSecond()) / 60;
            long delayInMinutes;
            if ( diffInMinutes >= 0 ){
                delayInMinutes = diffInMinutes; // delay in minutes
            }
            else{
                delayInMinutes = 24 * 60 + diffInMinutes; // wait for one day plus difference (which is negative)
            }

            LOG.info("Auto fetch begins in {} hours and runs every {} hours", String.valueOf(delayInMinutes / 60.0), TimeUnit.DAYS.toHours(intervalInDays));

            // system is defined as owner because owner field must not be null
            FetchProjectFeedsJob fetchProjectFeedsJob = new FetchProjectFeedsJob(project, "system");
            return DataManager.scheduler.scheduleAtFixedRate(fetchProjectFeedsJob,
                    delayInMinutes, TimeUnit.DAYS.toMinutes(intervalInDays), minutes);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Cancel an existing auto-fetch job that is scheduled for the given project ID.
     * There is only one auto-fetch job per project, not one for each feedSource within the project.
     */
    private static void cancelAutoFetch(String projectId){
        Project p = Persistence.projects.getById(projectId);
        if ( p != null && DataManager.autoFetchMap.get(p.id) != null) {
            LOG.info("Cancelling auto-fetch for projectID: {}", p.id);
            DataManager.autoFetchMap.get(p.id).cancel(true);
        }
    }

    /**
     * This connects all the above HTTP API handlers to URL paths (registers them with the Spark framework).
     * A bit too static/global for an OO language, but that's how Spark works.
     */
    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/project/:id", ProjectController::getProject, json::write);
        get(apiPrefix + "secure/project", ProjectController::getAllProjects, json::write);
        post(apiPrefix + "secure/project", ProjectController::createProject, json::write);
        put(apiPrefix + "secure/project/:id", ProjectController::updateProject, json::write);
        delete(apiPrefix + "secure/project/:id", ProjectController::deleteProject, json::write);
        get(apiPrefix + "secure/project/:id/thirdPartySync/:type", ProjectController::thirdPartySync, json::write);
        post(apiPrefix + "secure/project/:id/fetch", ProjectController::fetch, json::write);
        post(apiPrefix + "secure/project/:id/deployPublic", ProjectController::publishPublicFeeds, json::write);

        get(apiPrefix + "secure/project/:id/download", ProjectController::downloadMergedFeed);
        get(apiPrefix + "secure/project/:id/downloadtoken", ProjectController::getFeedDownloadCredentials, json::write);

        get(apiPrefix + "public/project/:id", ProjectController::getProject, json::write);
        get(apiPrefix + "public/project", ProjectController::getAllProjects, json::write);
        get(apiPrefix + "downloadprojectfeed/:token", ProjectController::downloadMergedFeedWithToken);
    }

    /**
     * HTTP endpoint that allows the requester to download a merged project feeds file stored locally (it should only
     * be invoked if the application is not using S3 storage) given that the requester supplies a valid token.
     */
    private static Object downloadMergedFeedWithToken(Request req, Response res) {
        FeedDownloadToken token = Persistence.tokens.getById(req.params("token"));

        if(token == null || !token.isValid()) {
            haltWithMessage(req, 400, "Feed download token not valid");
        }

        Project project = token.retrieveProject();

        Persistence.tokens.removeById(token.id);
        String fileName = project.id + ".zip";
        return downloadFile(FeedVersion.feedStore.getFeed(fileName), fileName, req, res);
    }

}
