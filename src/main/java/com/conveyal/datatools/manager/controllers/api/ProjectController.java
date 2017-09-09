package com.conveyal.datatools.manager.controllers.api;

import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.FetchProjectFeedsJob;
import com.conveyal.datatools.manager.jobs.MakePublicJob;
import com.conveyal.datatools.manager.jobs.MergeProjectFeedsJob;
import com.conveyal.datatools.manager.models.FeedDownloadToken;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.OtpBuildConfig;
import com.conveyal.datatools.manager.models.OtpRouterConfig;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import spark.Request;
import spark.Response;

import static com.conveyal.datatools.common.utils.S3Utils.getS3Credentials;
import static com.conveyal.datatools.common.utils.SparkUtils.downloadFile;
import static com.conveyal.datatools.manager.DataManager.publicPath;
import static spark.Spark.*;

/**
 * Created by demory on 3/14/16.
 */

@SuppressWarnings({"unused", "ThrowableNotThrown"})
public class ProjectController {

    public static JsonManager<Project> json =
            new JsonManager<>(Project.class, JsonViews.UserInterface.class);

    public static final Logger LOG = LoggerFactory.getLogger(ProjectController.class);
    private static ObjectMapper mapper = new ObjectMapper();

    private static Collection<Project> getAllProjects(Request req, Response res) throws JsonProcessingException {
        Auth0UserProfile userProfile = req.attribute("user");
        // FIXME: move this filtering into database query
        return Persistence.getProjects().stream()
                .filter(p -> req.pathInfo().matches(publicPath) || userProfile.hasProject(p.id, p.organizationId))
                .map(p -> requestProject(req, p, "view"))
                .collect(Collectors.toList());
    }

    private static Project getProject(Request req, Response res) {
        return requestProjectById(req, "view");
    }

    private static Project createProject(Request req, Response res) {
        // TODO: use Persistence createProject
        Project p = Persistence.createProject(Document.parse(req.body()));
        return p;
    }

    private static Project updateProject(Request req, Response res) throws IOException {
        Project p = requestProjectById(req, "manage");
        try {
            applyJsonToProject(p, req.body());
            p.save();
        } catch (Exception e) {
            e.printStackTrace();
            halt(400, SparkUtils.formatJSON("Error saving retrieveProject"));
        }
        return p;
    }

    private static Project deleteProject(Request req, Response res) throws IOException {
        Project p = requestProjectById(req, "manage");
        p.delete();
        return p;
    }

    public static Boolean fetch(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        Project p = requestProjectById(req, "manage");

        FetchProjectFeedsJob fetchProjectFeedsJob = new FetchProjectFeedsJob(p, userProfile.getUser_id());

        // this is runnable because sometimes we schedule the task for a later time, but here we call it immediately
        // because it is short lived and just cues more work
        fetchProjectFeedsJob.run();
        return true;
    }

    private static void applyJsonToProject(Project p, String json) throws IOException {
        JsonNode node = mapper.readTree(json);
        boolean updateFetchSchedule = false;
        Iterator<Map.Entry<String, JsonNode>> fieldsIter = node.fields();
        while (fieldsIter.hasNext()) {
            Map.Entry<String, JsonNode> entry = fieldsIter.next();
            if(entry.getKey().equals("name")) {
                p.name = entry.getValue().asText();
            }
            else if(entry.getKey().equals("defaultLocationLat")) {
                p.defaultLocationLat = entry.getValue().asDouble();
                LOG.info("updating default lat");
            }
            else if(entry.getKey().equals("defaultLocationLon")) {
                p.defaultLocationLon = entry.getValue().asDouble();
                LOG.info("updating default lon");
            }
            else if(entry.getKey().equals("north")) {
                p.north = entry.getValue().asDouble();
            }
            else if(entry.getKey().equals("south")) {
                p.south = entry.getValue().asDouble();
            }
            else if(entry.getKey().equals("east")) {
                p.east = entry.getValue().asDouble();
            }
            else if(entry.getKey().equals("west")) {
                p.west = entry.getValue().asDouble();
            }
            else if(entry.getKey().equals("organizationId")) {
                p.organizationId = entry.getValue().asText();
            }
            else if(entry.getKey().equals("osmNorth")) {
                p.osmNorth = entry.getValue().asDouble();
            }
            else if(entry.getKey().equals("osmSouth")) {
                p.osmSouth = entry.getValue().asDouble();
            }
            else if(entry.getKey().equals("osmEast")) {
                p.osmEast = entry.getValue().asDouble();
            }
            else if(entry.getKey().equals("osmWest")) {
                p.osmWest = entry.getValue().asDouble();
            }
            else if(entry.getKey().equals("useCustomOsmBounds")) {
                p.useCustomOsmBounds = entry.getValue().asBoolean();
            }
            else if(entry.getKey().equals("defaultLanguage")) {
                p.defaultLanguage = entry.getValue().asText();
            }
            else if(entry.getKey().equals("defaultTimeZone")) {
                p.defaultTimeZone = entry.getValue().asText();
            }
            else if(entry.getKey().equals("autoFetchHour")) {
                p.autoFetchHour = entry.getValue().asInt();
                updateFetchSchedule = true;
            }
            else if(entry.getKey().equals("autoFetchMinute")) {
                p.autoFetchMinute = entry.getValue().asInt();
                updateFetchSchedule = true;
            }
            else if(entry.getKey().equals("autoFetchFeeds")) {
                p.autoFetchFeeds = entry.getValue().asBoolean();
                updateFetchSchedule = true;
            }

            // NOTE: the below keys require the full objects to be included in the request json,
            // otherwise the missing fields/sub-classes will be set to null
            else if(entry.getKey().equals("otpServers")) {
                p.otpServers = mapper.readValue(String.valueOf(entry.getValue()), new TypeReference<List<OtpServer>>(){});
            }
            else if (entry.getKey().equals("buildConfig")) {
                p.buildConfig = mapper.treeToValue(entry.getValue(), OtpBuildConfig.class);
            }
            else if (entry.getKey().equals("routerConfig")) {
                p.routerConfig = mapper.treeToValue(entry.getValue(), OtpRouterConfig.class);
            }
        }
        if (updateFetchSchedule) {
            // If auto fetch flag is turned on
            if (p.autoFetchFeeds){
                int interval = 1; // once per day interval
                DataManager.autoFetchMap.put(p.id, scheduleAutoFeedFetch(p.id, p.autoFetchHour, p.autoFetchMinute, interval, p.defaultTimeZone));
            }
            // otherwise, cancel any existing task for this id
            else{
                cancelAutoFetch(p.id);
            }
        }
    }

    /**
     * Helper function returns feed source if user has permission for specified action.
     * @param req spark Request object from API request
     * @param action action type (either "view" or "manage")
     * @return requested retrieveProject
     */
    private static Project requestProjectById(Request req, String action) {
        String id = req.params("id");
        if (id == null) {
            halt(SparkUtils.formatJSON("Please specify id param", 400));
        }
        return requestProject(req, Persistence.getProjectById(id), action);
    }
    private static Project requestProject(Request req, Project p, String action) {
        Auth0UserProfile userProfile = req.attribute("user");
        boolean publicFilter = req.pathInfo().matches(publicPath);

        // check for null retrieveProject
        if (p == null) {
            halt(400, SparkUtils.formatJSON("Project ID does not exist", 400));
            return null;
        }

        boolean authorized;
        switch (action) {
            // TODO: limit create action to app/org admins?
//            case "create":
//                authorized = userProfile.canAdministerOrganization(p.organizationId);
//                break;
            case "manage":
                authorized = userProfile.canAdministerProject(p.id, p.organizationId);
                break;
            case "view":
                // request only authorized if not via public path and user can view
                authorized = !publicFilter && userProfile.hasProject(p.id, p.organizationId);
                break;
            default:
                authorized = false;
                break;
        }

        // if requesting all projects via public route, include public feed sources
        if (publicFilter){
            p.feedSources = p.retrieveProjectFeedSources().stream()
                    .filter(fs -> fs.isPublic)
                    .collect(Collectors.toList());
        } else {
            p.feedSources = null;
            if (!authorized) {
                halt(403, SparkUtils.formatJSON("User not authorized to perform action on retrieveProject", 403));
                return null;
            }
        }
        // if we make it here, user has permission and it's a valid retrieveProject
        return p;
    }

    private static boolean downloadMergedFeed(Request req, Response res) throws IOException {
        Project project = requestProjectById(req, "view");
        Auth0UserProfile userProfile = req.attribute("user");
        // TODO: make this an authenticated call?
        MergeProjectFeedsJob mergeProjectFeedsJob = new MergeProjectFeedsJob(project, userProfile.getUser_id());
        DataManager.heavyExecutor.execute(mergeProjectFeedsJob);

        return true;
    }

    /**
     * Returns credentials that a client may use to then download a feed version. Functionality
     * changes depending on whether application.data.use_s3_storage config property is true.
     */
    public static Object getFeedDownloadCredentials(Request req, Response res) {
        Project project = requestProjectById(req, "view");

        // if storing feeds on s3, return temporary s3 credentials for that zip file
        if (DataManager.useS3) {
            return getS3Credentials(DataManager.awsRole, DataManager.feedBucket, "project/" + project.id + ".zip", Statement.Effect.Allow, S3Actions.GetObject, 900);
        } else {
            // when feeds are stored locally, single-use download token will still be used
            FeedDownloadToken token = new FeedDownloadToken(project);
            token.save();
            return token;
        }
    }

    private static boolean deployPublic(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        String id = req.params("id");
        if (id == null) {
            halt(400, "must provide retrieveProject id!");
        }
        Project p = Project.retrieve(id);

        if (p == null)
            halt(400, "no such retrieveProject!");

        // run as sync job; if it gets too slow change to async
        new MakePublicJob(p, userProfile.getUser_id()).run();
        return true;
    }

    private static Project thirdPartySync(Request req, Response res) throws Exception {
        Auth0UserProfile userProfile = req.attribute("user");
        String id = req.params("id");
        Project proj = Project.retrieve(id);

        String syncType = req.params("type");

        if (!userProfile.canAdministerProject(proj.id, proj.organizationId))
            halt(403);

        LOG.info("syncing with third party " + syncType);

        if(DataManager.feedResources.containsKey(syncType)) {
            DataManager.feedResources.get(syncType).importFeedsForProject(proj, req.headers("Authorization"));
            return proj;
        }

        halt(404);
        return null;
    }

    public static ScheduledFuture scheduleAutoFeedFetch (String id, int hour, int minute, int intervalInDays, String timezoneId){
        TimeUnit minutes = TimeUnit.MINUTES;
        try {
            // First cancel any already scheduled auto fetch task for this retrieveProject id.
            cancelAutoFetch(id);

            Project p = Project.retrieve(id);
            if (p == null) return null;

            ZoneId timezone;
            try {
                timezone = ZoneId.of(timezoneId);
            }catch(Exception e){
                timezone = ZoneId.of("America/New_York");
            }
            LOG.info("Scheduling auto-fetch for projectID: {}", p.id);

            // NOW in default timezone
            ZonedDateTime now = ZonedDateTime.ofInstant(Instant.now(), timezone);

            // SCHEDULED START TIME
            ZonedDateTime startTime = LocalDateTime.of(LocalDate.now(), LocalTime.of(hour, minute)).atZone(timezone);
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
            FetchProjectFeedsJob fetchProjectFeedsJob = new FetchProjectFeedsJob(p, "system");

            return DataManager.scheduler.scheduleAtFixedRate(fetchProjectFeedsJob, delayInMinutes, TimeUnit.DAYS.toMinutes(intervalInDays), minutes);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static void cancelAutoFetch(String id){
        Project p = Project.retrieve(id);
        if ( p != null && DataManager.autoFetchMap.get(p.id) != null) {
            LOG.info("Cancelling auto-fetch for projectID: {}", p.id);
            DataManager.autoFetchMap.get(p.id).cancel(true);
        }
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/project/:id", ProjectController::getProject, json::write);
        get(apiPrefix + "secure/project", ProjectController::getAllProjects, json::write);
        post(apiPrefix + "secure/project", ProjectController::createProject, json::write);
        put(apiPrefix + "secure/project/:id", ProjectController::updateProject, json::write);
        delete(apiPrefix + "secure/project/:id", ProjectController::deleteProject, json::write);
        get(apiPrefix + "secure/project/:id/thirdPartySync/:type", ProjectController::thirdPartySync, json::write);
        post(apiPrefix + "secure/project/:id/fetch", ProjectController::fetch, json::write);
        post(apiPrefix + "secure/project/:id/deployPublic", ProjectController::deployPublic, json::write);

        get(apiPrefix + "secure/project/:id/download", ProjectController::downloadMergedFeed);
        get(apiPrefix + "secure/project/:id/downloadtoken", ProjectController::getFeedDownloadCredentials, json::write);

        get(apiPrefix + "public/project/:id", ProjectController::getProject, json::write);
        get(apiPrefix + "public/project", ProjectController::getAllProjects, json::write);
        get(apiPrefix + "downloadprojectfeed/:token", ProjectController::downloadMergedFeedWithToken);
    }

    private static Object downloadMergedFeedWithToken(Request req, Response res) {
        FeedDownloadToken token = FeedDownloadToken.get(req.params("token"));

        if(token == null || !token.isValid()) {
            halt(400, "Feed download token not valid");
        }

        Project project = token.retrieveProject();

        token.delete();
        String fileName = project.id + ".zip";
        return downloadFile(FeedVersion.feedStore.getFeed(fileName), fileName, res);
    }

}
