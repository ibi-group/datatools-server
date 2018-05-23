package com.conveyal.datatools.manager.controllers.api;

import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.BuildTransportNetworkJob;
import com.conveyal.datatools.manager.jobs.CreateFeedVersionFromSnapshotJob;
import com.conveyal.datatools.manager.jobs.ProcessSingleFeedJob;
import com.conveyal.datatools.manager.models.FeedDownloadToken;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.Snapshot;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.HashUtils;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.conveyal.r5.analyst.PointSet;
import com.conveyal.r5.analyst.cluster.AnalystClusterRequest;
import com.conveyal.r5.analyst.cluster.ResultEnvelope;
import com.conveyal.r5.analyst.cluster.TaskStatistics;
import com.conveyal.r5.api.util.LegMode;
import com.conveyal.r5.api.util.TransitModes;
import com.conveyal.r5.profile.ProfileRequest;
import com.conveyal.r5.profile.RepeatedRaptorProfileRouter;
import com.conveyal.r5.profile.StreetMode;
import com.conveyal.r5.streets.LinkedPointSet;
import com.conveyal.r5.transit.TransportNetwork;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequestWrapper;

import static com.conveyal.datatools.common.status.MonitorableJob.JobType.BUILD_TRANSPORT_NETWORK;
import static com.conveyal.datatools.common.utils.S3Utils.getS3Credentials;
import static com.conveyal.datatools.common.utils.SparkUtils.downloadFile;
import static com.conveyal.datatools.common.utils.SparkUtils.formatJobMessage;
import static com.conveyal.datatools.common.utils.SparkUtils.haltWithMessage;
import static com.conveyal.datatools.manager.controllers.api.FeedSourceController.checkFeedSourcePermissions;
import static spark.Spark.*;

public class FeedVersionController  {

    // TODO use this instead of stringly typed permissions
    enum Permission {
        VIEW, MANAGE
    }

    public static final Logger LOG = LoggerFactory.getLogger(FeedVersionController.class);
    private static ObjectMapper mapper = new ObjectMapper();
    public static JsonManager<FeedVersion> json = new JsonManager<>(FeedVersion.class, JsonViews.UserInterface.class);

    /**
     * Grab the feed version for the ID supplied in the request.
     * If you pass in ?summarized=true, don't include the full tree of validation results, only the counts.
     */
    public static FeedVersion getFeedVersion (Request req, Response res) {
        FeedVersion feedVersion = requestFeedVersion(req, "view");
        return feedVersion;
    }

    /**
     * Get all feed versions for a given feedSource (whose ID is specified in the request).
     */
    public static Collection<FeedVersion> getAllFeedVersionsForFeedSource(Request req, Response res) {
        // Check permissions and get the FeedSource whose FeedVersions we want.
        FeedSource feedSource = requestFeedSourceById(req, "view");
        Collection<FeedVersion> feedVersions = feedSource.retrieveFeedVersions();
        return feedVersions;
    }

    public static FeedSource requestFeedSourceById(Request req, String action, String paramName) {
        String id = req.queryParams(paramName);
        if (id == null) {
            halt(SparkUtils.formatJSON("Please specify feedSourceId param", 400));
        }
        return checkFeedSourcePermissions(req, Persistence.feedSources.getById(id), action);
    }

    public static FeedSource requestFeedSourceById(Request req, String action) {
        return requestFeedSourceById(req, action, "feedSourceId");
    }

    /**
     * Upload a feed version directly. This is done behind Backbone's back, and as such uses
     * x-multipart-formdata rather than a json blob. This is done because uploading files in a JSON
     * blob is not pretty, and we don't really need to retrieveById the Backbone object directly; page re-render isn't
     * a problem.
     *
     * Auto-fetched feeds are no longer restricted from having directly-uploaded versions, so we're not picky about
     * that anymore.
     *
     * @return the job ID that allows monitoring progress of the load process
     */
    public static String createFeedVersionViaUpload(Request req, Response res) {

        Auth0UserProfile userProfile = req.attribute("user");
        FeedSource feedSource = requestFeedSourceById(req, "manage");
        FeedVersion latestVersion = feedSource.retrieveLatest();
        FeedVersion newFeedVersion = new FeedVersion(feedSource);
        newFeedVersion.retrievalMethod = FeedSource.FeedRetrievalMethod.MANUALLY_UPLOADED;


        // FIXME: Make the creation of new GTFS files generic to handle other feed creation methods, including fetching
        // by URL and loading from the editor.
        File newGtfsFile = new File(DataManager.getConfigPropertyAsText("application.data.gtfs"), newFeedVersion.id);
        try {
            // Bypass Spark's request wrapper which always caches the request body in memory that may be a very large
            // GTFS file. Also, the body of the request is the GTFS file instead of using multipart form data because
            // multipart form handling code also caches the request body.
            ServletInputStream inputStream = ((ServletRequestWrapper) req.raw()).getRequest().getInputStream();
            FileOutputStream fileOutputStream = new FileOutputStream(newGtfsFile);
            // Guava's ByteStreams.copy uses a 4k buffer (no need to wrap output stream), but does not close streams.
            ByteStreams.copy(inputStream, fileOutputStream);
            fileOutputStream.close();
            inputStream.close();
            if (newGtfsFile.length() == 0) {
                throw new IOException("No file found in request body.");
            }
            // Set last modified based on value of query param. This is determined/supplied by the client
            // request because this data gets lost in the uploadStream otherwise.
            Long lastModified = req.queryParams("lastModified") != null ? Long.valueOf(req.queryParams("lastModified")) : null;
            if (lastModified != null) newGtfsFile.setLastModified(lastModified);
            LOG.info("Last modified: {}", new Date(newGtfsFile.lastModified()));
            LOG.info("Saving feed from upload {}", feedSource);
        } catch (Exception e) {
            LOG.error("Unable to open input stream from uploaded file", e);
            haltWithMessage(400, "Unable to read uploaded feed");
        }

        // TODO: fix FeedVersion.hash() call when called in this context. Nothing gets hashed because the file has not been saved yet.
        // newFeedVersion.hash();
        newFeedVersion.hash = HashUtils.hashFile(newGtfsFile);

        // Check that the hashes of the feeds don't match, i.e. that the feed has changed since the last version.
        // (as long as there is a latest version, i.e. the feed source is not completely new)
        if (latestVersion != null && latestVersion.hash.equals(newFeedVersion.hash)) {
            // Uploaded feed matches latest. Delete GTFS file because it is a duplicate.
            LOG.error("Upload version {} matches latest version {}.", newFeedVersion.id, latestVersion.id);
            newGtfsFile.delete();
            LOG.warn("File deleted");

            // There is no need to delete the newFeedVersion because it has not yet been persisted to MongoDB.
            haltWithMessage(304, "Uploaded feed is identical to the latest version known to the database.");
        }

        newFeedVersion.setName(newFeedVersion.formattedTimestamp() + " Upload");
        // TODO newFeedVersion.fileTimestamp still exists

        // Must be handled by executor because it takes a long time.
        ProcessSingleFeedJob processSingleFeedJob = new ProcessSingleFeedJob(newFeedVersion, userProfile.getUser_id(), true);
        DataManager.heavyExecutor.execute(processSingleFeedJob);

        return formatJobMessage(processSingleFeedJob.jobId, "Feed version is processing.");
    }

    /**
     * HTTP API handler that converts an editor snapshot into a "published" data manager feed version.
     *
     * FIXME: How should we handle this for the SQL version of the application. One proposal might be to:
     *  1. "Freeze" the feed in the DB (making it read only).
     *  2. Run validation on the feed.
     *  3. Export a copy of the data to a GTFS file.
     *
     *  OR we could just export the feed to a file and then re-import it per usual. This seems like it's wasting time/energy.
     */
    public static boolean createFeedVersionFromSnapshot (Request req, Response res) {

        Auth0UserProfile userProfile = req.attribute("user");
        // TODO: Should the ability to create a feedVersion from snapshot be controlled by the 'edit-gtfs' privilege?
        FeedSource feedSource = requestFeedSourceById(req, "manage");
        Snapshot snapshot = Persistence.snapshots.getById(req.queryParams("snapshotId"));
        if (snapshot == null) {
            haltWithMessage(400, "Must provide valid snapshot ID");
        }
        FeedVersion feedVersion = new FeedVersion(feedSource);
        CreateFeedVersionFromSnapshotJob createFromSnapshotJob =
                new CreateFeedVersionFromSnapshotJob(feedVersion, snapshot, userProfile.getUser_id());
        DataManager.heavyExecutor.execute(createFromSnapshotJob);

        return true;
    }

    /**
     * Spark HTTP API handler that deletes a single feed version based on the ID in the request.
     */
    public static FeedVersion deleteFeedVersion(Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, "manage");
        version.delete();
        return version;
    }

    public static FeedVersion requestFeedVersion(Request req, String action) {
        return requestFeedVersion(req, action, req.params("id"));
    }

    public static FeedVersion requestFeedVersion(Request req, String action, String feedVersionId) {
        FeedVersion version = Persistence.feedVersions.getById(feedVersionId);
        if (version == null) {
            haltWithMessage(404, "Feed version ID does not exist");
        }
        // Performs permissions checks on the feed source this feed version belongs to, and halts if permission is denied.
        checkFeedSourcePermissions(req, version.parentFeedSource(), action);
        return version;
    }

    /**
     * This method returns isochrones generated by R5 for the provided request parameters (the actual request is
     * constructed in {@link #buildProfileRequest}). If a transport network does not exist for the feed version, an async
     * build job is kicked off. Otherwise, the transport network cache is checked for the network.
     */
    public static JsonNode getIsochrones(Request req, Response res) {
        if (!DataManager.isModuleEnabled("r5_network")) {
            halt(400, SparkUtils.formatJSON("Isochrone generation not enabled in this application."));
        }
        Auth0UserProfile userProfile = req.attribute("user");
        FeedVersion version = requestFeedVersion(req, "view");
        // Check server jobs to determine if build or read is in progress.
        checkForActiveTransportNetworkJob(version);
        TransportNetwork transportNetwork = null;
        if (!version.transportNetworkPath().exists()) {
            // If transport network does not exist, build it in async server job.
            BuildTransportNetworkJob buildTransportNetworkJob =
                    new BuildTransportNetworkJob(version, userProfile.getUser_id());
            DataManager.heavyExecutor.execute(buildTransportNetworkJob);
            // Set status to Accepted to indicate that the processing is not complete yet.
            res.status(HttpStatus.ACCEPTED_202);
            return SparkUtils.formatJobResponse(buildTransportNetworkJob.jobId, "Building transport network");
        }
        try {
            // Get transport network from cache.
            transportNetwork = DataManager.transportNetworkCache.getTransportNetwork(version.id);
            // Handle routing request if there is a transport network to route on.
            AnalystClusterRequest clusterRequest = buildProfileRequest(req);
            return getRouterResult(transportNetwork, clusterRequest);
        } catch (ExecutionException e) {
            e.printStackTrace();
            haltWithMessage(400, SparkUtils.formatJSON("Error accessing transport network."), e);
        }
        return null;
    }

    /**
     * Filters active jobs for any "build transport network" types for the provided version. If there is a match,
     * a halt is sent to the requester.
     */
    private static void checkForActiveTransportNetworkJob(FeedVersion version) {
        // Get list of jobs that are building transport networks for the feed version (should be at most only one).
        List<BuildTransportNetworkJob> buildJobs = StatusController.filterJobsByType(BUILD_TRANSPORT_NETWORK).stream()
                .filter(job -> ((BuildTransportNetworkJob) job).feedVersion.id.equals(version.id))
                .map(job -> (BuildTransportNetworkJob) job)
                .collect(Collectors.toList());

        if (buildJobs.size() > 0) {
            // Halt the request if there are active build jobs.
            haltWithMessage(202, "Please wait. Building transport network for version.");
        }
    }

    /**
     * Routes a profile request on the provided transport network and returns the resulting isochrones.
     */
    private static JsonNode getRouterResult(TransportNetwork transportNetwork, AnalystClusterRequest clusterRequest) {
        PointSet targets;
        if (transportNetwork.gridPointSet == null) {
            transportNetwork.rebuildLinkedGridPointSet();
        }
        targets = transportNetwork.gridPointSet;
        StreetMode mode = StreetMode.WALK;
        final LinkedPointSet linkedTargets = targets.link(transportNetwork.streetLayer, mode);
        RepeatedRaptorProfileRouter router = new RepeatedRaptorProfileRouter(transportNetwork, clusterRequest, linkedTargets, new TaskStatistics());
        ResultEnvelope result = router.route();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            JsonGenerator jsonGenerator = new JsonFactory().createGenerator(out);
            jsonGenerator.writeStartObject();
            result.avgCase.writeIsochrones(jsonGenerator);
            jsonGenerator.writeEndObject();
            jsonGenerator.close();
            out.close();
            String outString = new String( out.toByteArray(), StandardCharsets.UTF_8 );
            return mapper.readTree(outString);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static AnalystClusterRequest buildProfileRequest(Request req) {
        // required fields?
        Double fromLat = Double.valueOf(req.queryParams("fromLat"));
        Double fromLon = Double.valueOf(req.queryParams("fromLon"));
        Double toLat = Double.valueOf(req.queryParams("toLat"));
        Double toLon = Double.valueOf(req.queryParams("toLon"));
        // Defaults to today if no date passed in.
        LocalDate date = req.queryParams("date") != null
                ? LocalDate.parse(req.queryParams("date"), DateTimeFormatter.ISO_LOCAL_DATE)
                : LocalDate.now(); // format = 2011-12-30 (YYYY-MM-DD)

        // Optional fields with defaults
        Integer fromTime = req.queryParams("fromTime") != null
                ? Integer.valueOf(req.queryParams("fromTime"))
                : 9 * 3600; // 9am in seconds since midnight
        Integer toTime = req.queryParams("toTime") != null
                ? Integer.valueOf(req.queryParams("toTime"))
                : 10 * 3600; // 10am in seconds since midnight

        // build request with transit as default mode
        AnalystClusterRequest clusterRequest = new AnalystClusterRequest();
        clusterRequest.profileRequest = new ProfileRequest();
        clusterRequest.profileRequest.transitModes = EnumSet.of(TransitModes.TRANSIT);
        // Access and egress restricted to walking.
        clusterRequest.profileRequest.accessModes = EnumSet.of(LegMode.WALK);
        clusterRequest.profileRequest.egressModes = EnumSet.of(LegMode.WALK);
        clusterRequest.profileRequest.date = date;
        clusterRequest.profileRequest.fromLat = fromLat;
        clusterRequest.profileRequest.fromLon = fromLon;
        clusterRequest.profileRequest.toLat = toLat;
        clusterRequest.profileRequest.toLon = toLon;
        clusterRequest.profileRequest.fromTime = fromTime;
        clusterRequest.profileRequest.toTime = toTime;
        clusterRequest.profileRequest.zoneId = ZoneId.of("America/New_York");

        return clusterRequest;
    }

    public static Boolean renameFeedVersion (Request req, Response res) {
        FeedVersion v = requestFeedVersion(req, "manage");

        String name = req.queryParams("name");
        if (name == null) {
            halt(400, "Name parameter not specified");
        }

        Persistence.feedVersions.updateField(v.id, "name", name);
        return true;
    }

    private static Object downloadFeedVersionDirectly(Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, "view");
        return downloadFile(version.retrieveGtfsFile(), version.id, res);
    }

    /**
     * Returns credentials that a client may use to then download a feed version. Functionality
     * changes depending on whether application.data.use_s3_storage config property is true.
     */
    public static Object getFeedDownloadCredentials(Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, "view");

        // if storing feeds on s3, return temporary s3 credentials for that zip file
        if (DataManager.useS3) {
            return getS3Credentials(DataManager.awsRole, DataManager.feedBucket, FeedStore.s3Prefix + version.id, Statement.Effect.Allow, S3Actions.GetObject, 900);
        } else {
            // when feeds are stored locally, single-use download token will still be used
            FeedDownloadToken token = new FeedDownloadToken(version);
            Persistence.tokens.create(token);
            return token;
        }
    }

    /**
     * API endpoint that instructs application to validate a feed if validation does not exist for version.
     * FIXME!
     */
    private static JsonNode validate (Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, "manage");

        // FIXME: Update for sql-loader validation process?
        return null;
//        return version.retrieveValidationResult(true);
    }

    private static FeedVersion publishToExternalResource (Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, "manage");

        // notify any extensions of the change
        for(String resourceType : DataManager.feedResources.keySet()) {
            DataManager.feedResources.get(resourceType).feedVersionCreated(version, null);
        }
        // update published version ID on feed source
        Persistence.feedSources.update(version.feedSourceId, String.format("{publishedVersionId: %s}", version.id));
        return version;
    }

    /**
     * Download locally stored feed version with token supplied by this application. This method is only used when
     * useS3 is set to false. Otherwise, a direct download from s3 should be used.
     */
    private static Object downloadFeedVersionWithToken (Request req, Response res) {
        String tokenValue = req.params("token");
        FeedDownloadToken token = Persistence.tokens.getById(tokenValue);

        if(token == null || !token.isValid()) {
            halt(400, "Feed download token not valid");
        }

        // Fetch feed version to download.
        FeedVersion version = token.retrieveFeedVersion();
        if (version == null) {
            haltWithMessage(400, "Could not retrieve version to download");
        }
        // Remove token so that it cannot be used again for feed download
        Persistence.tokens.removeById(tokenValue);
        File file = version.retrieveGtfsFile();
        return downloadFile(file, version.id, res);
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/feedversion/:id", FeedVersionController::getFeedVersion, json::write);
        get(apiPrefix + "secure/feedversion/:id/download", FeedVersionController::downloadFeedVersionDirectly);
        get(apiPrefix + "secure/feedversion/:id/downloadtoken", FeedVersionController::getFeedDownloadCredentials, json::write);
//        get(apiPrefix + "secure/feedversion/:id/validation", FeedVersionController::getValidationResult, json::write);
        post(apiPrefix + "secure/feedversion/:id/validate", FeedVersionController::validate, json::write);
        get(apiPrefix + "secure/feedversion/:id/isochrones", FeedVersionController::getIsochrones, json::write);
        get(apiPrefix + "secure/feedversion", FeedVersionController::getAllFeedVersionsForFeedSource, json::write);
        post(apiPrefix + "secure/feedversion", FeedVersionController::createFeedVersionViaUpload, json::write);
        post(apiPrefix + "secure/feedversion/fromsnapshot", FeedVersionController::createFeedVersionFromSnapshot, json::write);
        put(apiPrefix + "secure/feedversion/:id/rename", FeedVersionController::renameFeedVersion, json::write);
        post(apiPrefix + "secure/feedversion/:id/publish", FeedVersionController::publishToExternalResource, json::write);
        delete(apiPrefix + "secure/feedversion/:id", FeedVersionController::deleteFeedVersion, json::write);

        get(apiPrefix + "public/feedversion", FeedVersionController::getAllFeedVersionsForFeedSource, json::write);
//        get(apiPrefix + "public/feedversion/:id/validation", FeedVersionController::getPublicValidationResult, json::write);
        get(apiPrefix + "public/feedversion/:id/downloadtoken", FeedVersionController::getFeedDownloadCredentials, json::write);

        get(apiPrefix + "downloadfeed/:token", FeedVersionController::downloadFeedVersionWithToken);

    }
}
