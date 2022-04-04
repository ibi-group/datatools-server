package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.common.utils.aws.CheckedAWSException;
import com.conveyal.datatools.common.utils.aws.S3Utils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.auth.Actions;
import com.conveyal.datatools.manager.jobs.CreateFeedVersionFromSnapshotJob;
import com.conveyal.datatools.manager.jobs.GisExportJob;
import com.conveyal.datatools.manager.jobs.MergeFeedsJob;
import com.conveyal.datatools.manager.jobs.feedmerge.MergeFeedsType;
import com.conveyal.datatools.manager.jobs.ProcessSingleFeedJob;
import com.conveyal.datatools.manager.models.FeedDownloadToken;
import com.conveyal.datatools.manager.models.FeedRetrievalMethod;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.FeedVersionSummary;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.Snapshot;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.JobUtils;
import com.conveyal.datatools.manager.utils.PersistenceUtils;
import com.conveyal.datatools.manager.utils.json.JsonManager;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static com.conveyal.datatools.common.utils.SparkUtils.copyRequestStreamIntoFile;
import static com.conveyal.datatools.common.utils.SparkUtils.downloadFile;
import static com.conveyal.datatools.common.utils.SparkUtils.formatJobMessage;
import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;
import static com.conveyal.datatools.manager.controllers.api.FeedSourceController.checkFeedSourcePermissions;
import static com.mongodb.client.model.Filters.eq;
import static com.conveyal.datatools.manager.jobs.feedmerge.MergeFeedsType.REGIONAL;
import static com.mongodb.client.model.Filters.in;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;
import static spark.Spark.put;

public class FeedVersionController  {

    private static final Logger LOG = LoggerFactory.getLogger(FeedVersionController.class);
    private static JsonManager<FeedVersion> json = new JsonManager<>(FeedVersion.class, JsonViews.UserInterface.class);

    /**
     * Grab the feed version for the ID supplied in the request.
     * If you pass in ?summarized=true, don't include the full tree of validation results, only the counts.
     */
    private static FeedVersion getFeedVersion (Request req, Response res) {
        return requestFeedVersion(req, Actions.VIEW);
    }

    /**
     * Get all feed version summaries for a given feedSource (whose ID is specified in the request).
     */
    private static Collection<FeedVersionSummary> getAllFeedVersionSummariesForFeedSource(Request req, Response res) {
        FeedSource feedSource = requestFeedSourceById(req, Actions.VIEW);
        return feedSource.retrieveFeedVersionSummaries();
    }

    /**
     * Get all feed versions for a given feedSource (whose ID is specified in the request).
     */
    private static Collection<FeedVersion> getAllFeedVersionsForFeedSource(Request req, Response res) {
        // Check permissions and get the FeedSource whose FeedVersions we want.
        FeedSource feedSource = requestFeedSourceById(req, Actions.VIEW);
        Auth0UserProfile userProfile = req.attribute("user");
        boolean isAdmin = userProfile.canAdministerProject(feedSource);
        return feedSource.retrieveFeedVersions().stream()
            .map(version -> cleanFeedVersionForNonAdmins(version, feedSource, isAdmin))
            .collect(Collectors.toList());
    }

    public static FeedSource requestFeedSourceById(Request req, Actions action, String paramName) {
        String id = req.queryParams(paramName);
        if (id == null) {
            logMessageAndHalt(req, 400, "Please specify feedSourceId param");
        }
        return checkFeedSourcePermissions(req, Persistence.feedSources.getById(id), action);
    }

    private static FeedSource requestFeedSourceById(Request req, Actions action) {
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
    private static String createFeedVersionViaUpload(Request req, Response res) {

        Auth0UserProfile userProfile = req.attribute("user");
        FeedSource feedSource = requestFeedSourceById(req, Actions.MANAGE);
        FeedVersion latestVersion = feedSource.retrieveLatest();
        FeedVersion newFeedVersion = new FeedVersion(feedSource, FeedRetrievalMethod.MANUALLY_UPLOADED);
        // Get path to GTFS file for storage.
        File newGtfsFile = FeedVersion.feedStore.getFeedFile(newFeedVersion.id);
        copyRequestStreamIntoFile(req, newGtfsFile);
        // Set last modified based on value of query param. This is determined/supplied by the client
        // request because this data gets lost in the uploadStream otherwise.
        Long lastModified = req.queryParams("lastModified") != null
            ? Long.valueOf(req.queryParams("lastModified"))
            : null;
        newFeedVersion.assignGtfsFileAttributes(newGtfsFile, lastModified);

        LOG.info("Last modified: {}", new Date(newGtfsFile.lastModified()));

        // Check that the hashes of the feeds don't match, i.e. that the feed has changed since the last version.
        // (as long as there is a latest version, the feed source is not completely new)
        if (newFeedVersion.isSameAs(latestVersion)) {
            // Uploaded feed matches latest. Delete GTFS file because it is a duplicate.
            LOG.error("Upload version {} matches latest version {}.", newFeedVersion.id, latestVersion.id);
            newGtfsFile.delete();
            LOG.warn("File deleted");

            // There is no need to delete the newFeedVersion because it has not yet been persisted to MongoDB.
            logMessageAndHalt(req, 304, "Uploaded feed is identical to the latest version known to the database.");
        }

        newFeedVersion.name = newFeedVersion.formattedTimestamp() + " Upload";
        // TODO newFeedVersion.fileTimestamp still exists

        // Must be handled by executor because it takes a long time.
        ProcessSingleFeedJob processSingleFeedJob = new ProcessSingleFeedJob(newFeedVersion, userProfile, true);
        JobUtils.heavyExecutor.execute(processSingleFeedJob);

        return formatJobMessage(processSingleFeedJob.jobId, "Feed version is processing.");
    }

    protected static FeedVersion cleanFeedVersionForNonAdmins(FeedVersion feedVersion, FeedSource feedSource, boolean isAdmin) {
        // Admin can view all feed labels, but a non-admin should only see those with adminOnly=false
        feedVersion.noteIds = Persistence.notes
            .getFiltered(PersistenceUtils.applyAdminFilter(in("_id", feedVersion.noteIds), isAdmin)).stream()
            .map(note -> note.id)
            .collect(Collectors.toList());
        return feedVersion;
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
    private static boolean createFeedVersionFromSnapshot (Request req, Response res) {

        Auth0UserProfile userProfile = req.attribute("user");
        // TODO: Should the ability to create a feedVersion from snapshot be controlled by the 'edit-gtfs' privilege?
        FeedSource feedSource = requestFeedSourceById(req, Actions.MANAGE);
        Snapshot snapshot = Persistence.snapshots.getById(req.queryParams("snapshotId"));
        if (snapshot == null) {
            logMessageAndHalt(req, 400, "Must provide valid snapshot ID");
        }
        CreateFeedVersionFromSnapshotJob createFromSnapshotJob =
            new CreateFeedVersionFromSnapshotJob(feedSource, snapshot, userProfile);
        JobUtils.heavyExecutor.execute(createFromSnapshotJob);

        return true;
    }

    /**
     * Spark HTTP API handler that deletes a single feed version based on the ID in the request.
     */
    private static FeedVersion deleteFeedVersion(Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, Actions.MANAGE);
        version.delete();
        return version;
    }

    private static FeedVersion requestFeedVersion(Request req, Actions action) {
        return requestFeedVersion(req, action, req.params("id"));
    }

    public static FeedVersion requestFeedVersion(Request req, Actions action, String feedVersionId) {
        FeedVersion version = Persistence.feedVersions.getById(feedVersionId);
        if (version == null) {
            logMessageAndHalt(req, 404, "Feed version ID does not exist");
            return null;
        }
        FeedSource feedSource = version.parentFeedSource();
        // Performs permissions checks on the feed source this feed version belongs to, and halts if permission is denied.
        checkFeedSourcePermissions(req, feedSource, action);
        Auth0UserProfile userProfile = req.attribute("user");
        boolean isAdmin = userProfile.canAdministerProject(feedSource);
        return cleanFeedVersionForNonAdmins(version, feedSource, isAdmin);
    }

    private static boolean renameFeedVersion (Request req, Response res) {
        FeedVersion v = requestFeedVersion(req, Actions.MANAGE);

        String name = req.queryParams("name");
        if (name == null) {
            logMessageAndHalt(req, 400, "Name parameter not specified");
        }

        Persistence.feedVersions.updateField(v.id, "name", name);
        return true;
    }

    private static HttpServletResponse downloadFeedVersionDirectly(Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, Actions.VIEW);
        return downloadFile(version.retrieveGtfsFile(), version.id, req, res);
    }

    /**
     * Returns credentials that a client may use to then download a feed version. Functionality
     * changes depending on whether application.data.use_s3_storage config property is true.
     */
    private static Object getDownloadCredentials(Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, Actions.VIEW);

        if (DataManager.useS3) {
            // Return pre-signed download link if using S3.
            return S3Utils.downloadObject(
                S3Utils.DEFAULT_BUCKET,
                S3Utils.DEFAULT_BUCKET_GTFS_FOLDER + version.id,
                false,
                req,
                res
            );
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
        FeedVersion version = requestFeedVersion(req, Actions.MANAGE);
        logMessageAndHalt(req, 400, "Validate endpoint not currently configured!");
        // FIXME: Update for sql-loader validation process?
        return null;
//        return version.retrieveValidationResult(true);
    }

    private static FeedVersion publishToExternalResource (Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, Actions.MANAGE);

        // notify any extensions of the change
        try {
            publishToExternalResource(version);
            return version;
        } catch (Exception e) {
            logMessageAndHalt(req, 500, "Could not publish feed.", e);
            return null;
        }
    }

    public static void publishToExternalResource(FeedVersion version) throws CheckedAWSException {
        for (String resourceType : DataManager.feedResources.keySet()) {
            DataManager.feedResources.get(resourceType).feedVersionCreated(version, null);
        }
        if (!DataManager.isExtensionEnabled("mtc")) {
            // update published version ID on feed source
            Persistence.feedSources.updateField(version.feedSourceId, "publishedVersionId", version.namespace);
        } else {
            // NOTE: If the MTC extension is enabled, the parent feed source's publishedVersionId will not be updated to the
            // version's namespace until the FeedUpdater has successfully downloaded the feed from the share S3 bucket.
            Date publishedDate = new Date();
            // Set "sent" timestamp to now and reset "processed" timestamp (in the case that it had previously been
            // published as the active version.
            version.sentToExternalPublisher = publishedDate;
            version.processedByExternalPublisher = null;
            Persistence.feedVersions.replace(version.id, version);
        }
    }

    /**
     * HTTP endpoint to initiate an export of a shapefile containing the stops or routes of one or
     * more feed versions. NOTE: the job ID returned must be used by the requester to download the
     * zipped shapefile once the job has completed.
     */
    private static String exportGis (Request req, Response res) throws IOException {
        String type = req.queryParams("type");
        Auth0UserProfile userProfile = req.attribute("user");
        List<String> feedIds = Arrays.asList(req.queryParams("feedId").split(","));
        File temp = File.createTempFile("gis_" + type, ".zip");
        // Create and run shapefile export.
        GisExportJob.ExportType exportType = GisExportJob.ExportType.valueOf(type);
        GisExportJob gisExportJob = new GisExportJob(exportType, temp, feedIds, userProfile);
        JobUtils.heavyExecutor.execute(gisExportJob);
        // Do not use S3 to store the file, which should only be stored ephemerally (until requesting
        // user has downloaded file).
        FeedDownloadToken token = new FeedDownloadToken(gisExportJob);
        Persistence.tokens.create(token);
        return SparkUtils.formatJobMessage(gisExportJob.jobId, "Generating shapefile.");
    }

    /**
     * Public HTTP endpoint to download a zipped shapefile of routes or stops for a set of feed
     * versions using the job ID that was used for initially creating the exported shapes.
     */
    private static HttpServletResponse downloadFeedVersionGis (Request req, Response res) {
        FeedDownloadToken token = Persistence.tokens.getOneFiltered(eq("jobId", req.params("jobId")));
        File file = new File(token.filePath);
        try {
            return downloadFile(file, file.getName(), req, res);
        } catch (Exception e) {
            logMessageAndHalt(req, 500,
                "Unknown error occurred while downloading feed version shapefile", e);
        } finally {
            if (!file.delete()) {
                LOG.error("Could not delete shapefile {}. Storage issues may occur.", token.filePath);
            } else {
                LOG.info("Deleted shapefile {} following download.", token.filePath);
            }
            // Delete token.
            Persistence.tokens.removeById(token.id);
        }
        return null;
    }

    /**
     * HTTP controller that handles merging multiple feed versions for a given feed source, with version IDs specified
     * in a comma-separated string in the feedVersionIds query parameter and merge type specified in mergeType query
     * parameter. NOTE: REGIONAL merge type should only be handled through {@link ProjectController#mergeProjectFeeds(Request, Response)}.
     */
    private static String mergeFeedVersions(Request req, Response res) {
        String[] versionIds = req.queryParams("feedVersionIds").split(",");
        // Try to parse merge type (null or bad value throws IllegalArgumentException).
        MergeFeedsType mergeType;
        try {
            mergeType = MergeFeedsType.valueOf(req.queryParams("mergeType"));
            if (mergeType.equals(REGIONAL)) {
                throw new IllegalArgumentException("Regional merge type is not permitted for this endpoint.");
            }
        } catch (IllegalArgumentException e) {
            logMessageAndHalt(req, 400, "Must provide valid merge type.", e);
            return null;
        }
        // Collect versions to merge (must belong to same feed source).
        Set<FeedVersion> versions = new HashSet<>();
        String feedSourceId = null;
        for (String id : versionIds) {
            FeedVersion v = Persistence.feedVersions.getById(id);
            if (v == null) {
                logMessageAndHalt(req,
                                  400,
                                  String.format("Must provide valid version ID. (No version exists for id=%s.)", id)
                );
            }
            // Store feed source id and check other versions for matching.
            if (feedSourceId == null) feedSourceId = v.feedSourceId;
            else if (!v.feedSourceId.equals(feedSourceId)) {
                logMessageAndHalt(req, 400, "Cannot merge versions with different parent feed sources.");
            }
            versions.add(v);
        }
        if (versionIds.length != 2) {
            logMessageAndHalt(req, 400, "Merging more than two versions is not currently supported.");
        }
        // Kick off merge feeds job.
        Auth0UserProfile userProfile = req.attribute("user");
        MergeFeedsJob mergeFeedsJob = new MergeFeedsJob(userProfile, versions, "merged", mergeType);
        JobUtils.heavyExecutor.execute(mergeFeedsJob);
        return SparkUtils.formatJobMessage(mergeFeedsJob.jobId, "Merging feed versions...");
    }

    /**
     * Download locally stored feed version with token supplied by this application. This method is only used when
     * useS3 is set to false. Otherwise, a direct download from s3 should be used.
     */
    private static HttpServletResponse downloadFeedVersionWithToken (Request req, Response res) {
        String tokenValue = req.params("token");
        FeedDownloadToken token = Persistence.tokens.getById(tokenValue);

        if(token == null || !token.isValid()) {
            LOG.error("Feed download token is invalid: {}", token);
            logMessageAndHalt(req, 400, "Feed download token not valid");
        }
        // Fetch feed version to download.
        FeedVersion version = token.retrieveFeedVersion();
        if (version == null) {
            logMessageAndHalt(req, 400, "Could not retrieve version to download");
        }
        LOG.info("Using token {} to download feed version {}", token.id, version.id);
        // Remove token so that it cannot be used again for feed download
        Persistence.tokens.removeById(tokenValue);
        File file = version.retrieveGtfsFile();
        return downloadFile(file, version.id, req, res);
    }

    public static void register (String apiPrefix) {
        // TODO: Might it be easier down the road to create a separate JSON view to request a "detailed" feed version
        // which would contain the full validationResult, so that a request for all versions does not become too large?
        // This might not be an issue because validation issues are queried separately.
        // TODO: We might need an endpoint to download a csv of all validation issues. This was supported in the
        // previous version of data tools.
        get(apiPrefix + "secure/feedversion/:id", FeedVersionController::getFeedVersion, json::write);
        get(apiPrefix + "secure/feedversion/:id/download", FeedVersionController::downloadFeedVersionDirectly);
        get(apiPrefix + "secure/feedversion/:id/downloadtoken", FeedVersionController::getDownloadCredentials, json::write);
        post(apiPrefix + "secure/feedversion/:id/validate", FeedVersionController::validate, json::write);
        get(apiPrefix + "secure/feedversionsummaries", FeedVersionController::getAllFeedVersionSummariesForFeedSource, json::write);
        get(apiPrefix + "secure/feedversion", FeedVersionController::getAllFeedVersionsForFeedSource, json::write);
        post(apiPrefix + "secure/feedversion", FeedVersionController::createFeedVersionViaUpload, json::write);
        post(apiPrefix + "secure/feedversion/shapes", FeedVersionController::exportGis, json::write);
        post(apiPrefix + "secure/feedversion/fromsnapshot", FeedVersionController::createFeedVersionFromSnapshot, json::write);
        put(apiPrefix + "secure/feedversion/:id/rename", FeedVersionController::renameFeedVersion, json::write);
        put(apiPrefix + "secure/feedversion/merge", FeedVersionController::mergeFeedVersions, json::write);
        post(apiPrefix + "secure/feedversion/:id/publish", FeedVersionController::publishToExternalResource, json::write);
        delete(apiPrefix + "secure/feedversion/:id", FeedVersionController::deleteFeedVersion, json::write);

        get(apiPrefix + "public/feedversion", FeedVersionController::getAllFeedVersionsForFeedSource, json::write);
        get(apiPrefix + "public/feedversionsummaries", FeedVersionController::getAllFeedVersionSummariesForFeedSource, json::write);
        get(apiPrefix + "public/feedversion/:id/downloadtoken", FeedVersionController::getDownloadCredentials, json::write);

        get(apiPrefix + "downloadfeed/:token", FeedVersionController::downloadFeedVersionWithToken);
        get(apiPrefix + "downloadshapes/:jobId", FeedVersionController::downloadFeedVersionGis, json::write);

    }
}
