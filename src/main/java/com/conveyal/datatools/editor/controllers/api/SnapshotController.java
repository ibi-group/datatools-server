package com.conveyal.datatools.editor.controllers.api;


import com.conveyal.datatools.common.utils.SparkUtils;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.conveyal.datatools.editor.jobs.CreateSnapshotJob;
import com.conveyal.datatools.editor.jobs.ExportSnapshotToGTFSJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.controllers.api.FeedSourceController;
import com.conveyal.datatools.manager.controllers.api.FeedVersionController;
import com.conveyal.datatools.manager.models.FeedDownloadToken;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.Snapshot;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

import spark.Request;
import spark.Response;

import static com.conveyal.datatools.common.utils.S3Utils.getS3Credentials;
import static com.conveyal.datatools.common.utils.SparkUtils.*;
import static spark.Spark.*;

/**
 * HTTP CRUD endpoints for managing snapshots, which are copies of GTFS feeds stored in the editor.
 */
public class SnapshotController {

    public static final Logger LOG = LoggerFactory.getLogger(SnapshotController.class);
    public static JsonManager<Snapshot> json =
            new JsonManager<>(Snapshot.class, JsonViews.UserInterface.class);

    /**
     * HTTP endpoint that returns a single snapshot for a specified ID.
     */
    public static Snapshot getSnapshotById(Request req, Response res) throws IOException {
        return getSnapshotFromRequest(req);
    }

    private static Snapshot getSnapshotFromRequest(Request req) {
        String id = req.params("id");
        if (id == null) haltWithError(400, "Must provide valid snapshot ID");
        // Check user permissions on feed source.
        FeedVersionController.requestFeedSourceById(req, "view", "feedId");
        return Persistence.snapshots.getById(id);
    }

    public static Collection<Snapshot> getSnapshots(Request req, Response res) throws IOException {
        // Get feed source and check user permissions.
        FeedSource feedSource = FeedVersionController.requestFeedSourceById(req, "view", "feedId");
        // FIXME Do we need a way to return all snapshots?
        // Is this used in GTFS Data Manager to retrieveById snapshots in bulk?

        // Return snapshots for feed source.
        return feedSource.retrieveSnapshots();
    }

    /**
     * HTTP endpoint that makes a snapshot copy of the current data loaded in the editor for a given feed source.
     */
    public static String createSnapshot (Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        // FIXME Take fields from request body for creating snapshot.
//        Document newSnapshotFields = Document.parse(req.body());
        FeedSource feedSource = FeedVersionController.requestFeedSourceById(req, "edit", "feedId");
        if (feedSource == null) haltWithError(400, "No feed source found for ID.");
        if (feedSource.editorNamespace == null) {
            haltWithError(400, "Must have feed in database before creating snapshot.");
        }
        // Create new non-buffer snapshot.
        CreateSnapshotJob createSnapshotJob =
                new CreateSnapshotJob(feedSource, feedSource.editorNamespace, userProfile.getUser_id(), false);
        // Begin asynchronous execution.
        DataManager.heavyExecutor.execute(createSnapshotJob);
        return SparkUtils.formatJSON("Creating snapshot.", 200);
    }

    /**
     * Create snapshot from feedVersion and load/import into editor database.
     */
    public static Object importSnapshot (Request req, Response res) {

        Auth0UserProfile userProfile = req.attribute("user");
        String feedVersionId = req.queryParams("feedVersionId");

        if(feedVersionId == null) {
            haltWithError(400, "No FeedVersion ID specified");
        }
        // FIXME Replace with feed version controller permissions check
        FeedVersion feedVersion = Persistence.feedVersions.getById(feedVersionId);
        if(feedVersion == null) {
            haltWithError(404, "Could not find FeedVersion with ID " + feedVersionId);
        }

        FeedSource feedSource = feedVersion.parentFeedSource();
        // check user's permission to import snapshot
        FeedSourceController.checkFeedSourcePermissions(req, feedSource, "edit");
        // Create and run snapshot job
        CreateSnapshotJob createSnapshotJob =
                new CreateSnapshotJob(feedSource, feedVersion.namespace, userProfile.getUser_id(), true);
        DataManager.heavyExecutor.execute(createSnapshotJob);
        haltWithOK("Importing snapshot...");
        return null;
    }

    // FIXME: Is this method used anywhere? Can we delete?
    public static Object updateSnapshot (Request req, Response res) {
        // FIXME
        haltWithError(400, "Method not implemented");
        return null;
    }

    /**
     * HTTP API method to "restore" snapshot (specified by ID param) to the active editor buffer.
     *
     * FIXME: Should this also save a copy of the snapshot before restoring (where the snapshot can be modified).
     */
    public static FeedSource restoreSnapshot (Request req, Response res) {
        // Get the snapshot ID to restore (set the namespace pointer)
        String id = req.params("id");
        // FIXME Ensure namespace id exists in database.
        // Retrieve feed source.
        FeedSource feedSource = FeedVersionController.requestFeedSourceById(req, "edit", "feedId");
        Snapshot snapshotToRestore = Persistence.snapshots.getById(id);
        if (snapshotToRestore == null) haltWithError(400, "Must specify valid snapshot ID");
        // Update editor namespace pointer.
        // FIXME: Should this make a copy of the namespace to preserve the "save point"
        if (snapshotToRestore.feedLoadResult == null) {
            haltWithError(400, "Failed to restore snapshot. No namespace found.");
        }
        FeedSource updatedFeedSource = Persistence.feedSources.updateField(
                feedSource.id, "editorNamespace", snapshotToRestore.feedLoadResult.uniqueIdentifier);
        LOG.info("Updated feedsource {} active buffer to {}", updatedFeedSource.id, updatedFeedSource.editorNamespace);
        // FIXME: Should return value be snapshot object?
        return updatedFeedSource;
    }

    /**
     * HTTP endpoint that triggers export of specified snapshot to GTFS. Returns a job ID with which to monitor the job
     * status. At the completion of the job, requester should fetch a download token for the snapshot.
     */
    private static String downloadSnapshotAsGTFS(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        String userId = userProfile.getUser_id();
        Snapshot snapshot = getSnapshotFromRequest(req);
        // Create and kick off export job.
        // FIXME: what if a snapshot is already written to S3?
        ExportSnapshotToGTFSJob exportSnapshotToGTFSJob = new ExportSnapshotToGTFSJob(userId,  snapshot);
        DataManager.heavyExecutor.execute(exportSnapshotToGTFSJob);
        return formatJobMessage(exportSnapshotToGTFSJob.jobId, "Exporting snapshot to GTFS.");
    }


    public static Object getSnapshotToken(Request req, Response res) {
        Snapshot snapshot = getSnapshotFromRequest(req);
        FeedDownloadToken token;
        String filePrefix = snapshot.feedSourceId + "_" + snapshot.snapshotTime;
        String key = "snapshots/" + filePrefix + ".zip";
        // if storing feeds on S3, first write the snapshot to GTFS file and upload to S3
        // this needs to be completed before the credentials are delivered, so that the client has
        // an actual object to download.
        // FIXME: use new FeedStore.
        if (DataManager.useS3) {
            if (!FeedStore.s3Client.doesObjectExist(DataManager.feedBucket, key)) {
                haltWithError(400, "Error downloading snapshot from S3. Object does not exist.");
            }
            return getS3Credentials(
                    DataManager.awsRole,
                    DataManager.feedBucket,
                    key,
                    Statement.Effect.Allow,
                    S3Actions.GetObject,
                    900
            );
        } else {
            // if not storing on s3, just use the token download method
            token = new FeedDownloadToken(snapshot);
            Persistence.tokens.create(token);
            return token;
        }
    }

    /**
     * HTTP endpoint to PERMANENTLY delete a snapshot given for the specified ID.
     */
    public static Snapshot deleteSnapshot(Request req, Response res) {
        // Get the snapshot ID to restore (set the namespace pointer)
        String id = req.params("id");
        // FIXME Ensure namespace id exists in database.
        // Check feed source permissions.
        FeedVersionController.requestFeedSourceById(req, "edit", "feedId");
        // Retrieve snapshot
        Snapshot snapshot = Persistence.snapshots.getById(id);
        if (snapshot == null) haltWithError(400, "Must provide valid snapshot ID.");
        try {
            Persistence.snapshots.removeById(snapshot.id);
            return snapshot;
        } catch (Exception e) {
            e.printStackTrace();
            haltWithError(400, "Unknown error deleting snapshot.", e);
            return null;
        }
    }

    /**
     * This method is used only when NOT storing feeds on S3. It will deliver a
     * snapshot file from the local storage if a valid token is provided.
     */
    private static Object downloadSnapshotWithToken (Request req, Response res) {
        // FIXME: Update for refactored FeedStore
        String id = req.params("token");
        FeedDownloadToken token = Persistence.tokens.getById(id);

        if(token == null || !token.isValid()) {
            halt(400, "Feed download token not valid");
        }

        Snapshot snapshot = token.retrieveSnapshot();
        Persistence.tokens.removeById(token.id);
        String fileName = snapshot.id + ".zip";
        return downloadFile(FeedVersion.feedStore.getFeed(fileName), fileName, res);
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/snapshot/:id", SnapshotController::getSnapshotById, json::write);
        options(apiPrefix + "secure/snapshot", (q, s) -> "");
        get(apiPrefix + "secure/snapshot", SnapshotController::getSnapshots, json::write);
        post(apiPrefix + "secure/snapshot", SnapshotController::createSnapshot, json::write);
        post(apiPrefix + "secure/snapshot/import", SnapshotController::importSnapshot, json::write);
        put(apiPrefix + "secure/snapshot/:id", SnapshotController::updateSnapshot, json::write);
        post(apiPrefix + "secure/snapshot/:id/restore", SnapshotController::restoreSnapshot, json::write);
        get(apiPrefix + "secure/snapshot/:id/download", SnapshotController::downloadSnapshotAsGTFS, json::write);
        get(apiPrefix + "secure/snapshot/:id/downloadtoken", SnapshotController::getSnapshotToken, json::write);
        delete(apiPrefix + "secure/snapshot/:id", SnapshotController::deleteSnapshot, json::write);

        get(apiPrefix + "downloadsnapshot/:token", SnapshotController::downloadSnapshotWithToken);
    }
}
