package com.conveyal.datatools.editor.controllers.api;


import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.common.utils.aws.S3Utils;
import com.conveyal.datatools.editor.jobs.CreateSnapshotJob;
import com.conveyal.datatools.editor.jobs.ExportSnapshotToGTFSJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.auth.Actions;
import com.conveyal.datatools.manager.controllers.api.FeedVersionController;
import com.conveyal.datatools.manager.jobs.CreateFeedVersionFromSnapshotJob;
import com.conveyal.datatools.manager.models.FeedDownloadToken;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.Snapshot;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.JobUtils;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.Collection;

import static com.conveyal.datatools.common.utils.SparkUtils.downloadFile;
import static com.conveyal.datatools.common.utils.SparkUtils.formatJobMessage;
import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.options;
import static spark.Spark.post;
import static spark.Spark.put;

/**
 * HTTP CRUD endpoints for managing snapshots, which are copies of GTFS feeds stored in the editor.
 */
public class SnapshotController {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotController.class);
    public static JsonManager<Snapshot> json =
            new JsonManager<>(Snapshot.class, JsonViews.UserInterface.class);

    /**
     * HTTP endpoint that returns a single snapshot for a specified ID.
     */
    private static Snapshot getSnapshotById(Request req, Response res) {
        return getSnapshotFromRequest(req);
    }

    /**
     * Wrapper method that checks requesting user's permissions on feed source (via feedId param) and returns snapshot
     * for ID param if the permissions check is OK.
     */
    private static Snapshot getSnapshotFromRequest(Request req) {
        String id = req.params("id");
        if (id == null) logMessageAndHalt(req, 400, "Must provide valid snapshot ID");
        // Check user permissions on feed source.
        FeedVersionController.requestFeedSourceById(req, Actions.VIEW, "feedId");
        return Persistence.snapshots.getById(id);
    }

    /**
     * HTTP endpoint that returns the list of snapshots for a given feed source.
     */
    private static Collection<Snapshot> getSnapshots(Request req, Response res) {
        // Get feed source and check user permissions.
        FeedSource feedSource = FeedVersionController.requestFeedSourceById(req, Actions.VIEW, "feedId");
        // FIXME Do we need a way to return all snapshots?
        // Is this used in GTFS Data Manager to retrieveById snapshots in bulk?

        // Return snapshots for feed source.
        return feedSource.retrieveSnapshots();
    }

    /**
     * HTTP endpoint that makes a snapshot copy of the current data loaded in the editor for a given feed source.
     */
    private static String createSnapshot (Request req, Response res) throws IOException {
        Auth0UserProfile userProfile = req.attribute("user");
        boolean publishNewVersion = Boolean.parseBoolean(
            req.queryParamOrDefault("publishNewVersion", Boolean.FALSE.toString())
        );
        FeedSource feedSource = FeedVersionController.requestFeedSourceById(req, Actions.EDIT, "feedId");
        // Take fields from request body for creating snapshot (i.e., feedId/feedSourceId, name, comment).
        Snapshot snapshot = json.read(req.body());
        // Ensure feed source ID and snapshotOf namespace is correct
        snapshot.feedSourceId = feedSource.id;
        snapshot.snapshotOf = feedSource.editorNamespace;
        snapshot.storeUser(userProfile);
        // If there is no active buffer for feed source, set boolean to update it to the new snapshot namespace.
        // Otherwise, creating a snapshot will just create a copy of the tables and leave the buffer untouched.
        boolean bufferIsEmpty = feedSource.editorNamespace == null;
        // Create new non-buffer snapshot.
        CreateSnapshotJob createSnapshotJob =
                new CreateSnapshotJob(userProfile, snapshot, bufferIsEmpty, !bufferIsEmpty, false);
        // Add publish feed version job if specified by request.
        if (publishNewVersion) {
            createSnapshotJob.addNextJob(new CreateFeedVersionFromSnapshotJob(feedSource, snapshot, userProfile));
        }
        // Begin asynchronous execution.
        JobUtils.heavyExecutor.execute(createSnapshotJob);
        return SparkUtils.formatJobMessage(createSnapshotJob.jobId, "Creating snapshot.");
    }

    /**
     * Create snapshot from feedVersion and load/import into editor database.
     */
    private static String importFeedVersionAsSnapshot(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        // Get feed version from request (and check permissions).
        String feedVersionId = req.queryParams("feedVersionId");
        FeedVersion feedVersion = FeedVersionController.requestFeedVersion(req, Actions.EDIT, feedVersionId);
        FeedSource feedSource = feedVersion.parentFeedSource();
        // Create and run snapshot job
        Snapshot snapshot = new Snapshot("Snapshot of " + feedVersion.name, feedSource.id, feedVersion.namespace);
        snapshot.storeUser(userProfile);
        // Only preserve buffer if there is already a namespace associated with the feed source and requester has
        // explicitly asked for it. Otherwise, let go of the buffer.
        boolean preserveBuffer = "true".equals(req.queryParams("preserveBuffer")) && feedSource.editorNamespace != null;
        CreateSnapshotJob createSnapshotJob =
                new CreateSnapshotJob(userProfile, snapshot, true, false, preserveBuffer);
        JobUtils.heavyExecutor.execute(createSnapshotJob);
        return formatJobMessage(createSnapshotJob.jobId, "Importing version as snapshot.");
    }

    // FIXME: Is this method used anywhere? Can we delete?
    private static Object updateSnapshot (Request req, Response res) {
        // FIXME
        logMessageAndHalt(req, 400, "Method not implemented");
        return null;
    }

    /**
     * HTTP API method to "restore" snapshot (specified by ID param) to the active editor buffer. This essentially makes
     * a copy of the namespace to preserve the "save point" and then updates the working buffer to point to the newly
     * created namespace.
     */
    private static String restoreSnapshot (Request req, Response res) {
        // Get the snapshot ID to restore (set the namespace pointer)
        String id = req.params("id");
        // FIXME Ensure namespace id exists in database?
        // Retrieve feed source.
        FeedSource feedSource = FeedVersionController.requestFeedSourceById(req, Actions.EDIT, "feedId");
        Snapshot snapshotToRestore = Persistence.snapshots.getById(id);
        if (snapshotToRestore == null) {
            logMessageAndHalt(req, 400, "Must specify valid snapshot ID");
        }
        // Update editor namespace pointer.
        if (snapshotToRestore.namespace == null) {
            logMessageAndHalt(req, 400, "Failed to restore snapshot. No namespace found.");
        }
        // Preserve existing editor buffer if requested. FIXME: should the request body also contain name and comments?
        boolean preserveBuffer = "true".equals(req.queryParams("preserveBuffer"));
        // Create and run snapshot job
        Auth0UserProfile userProfile = req.attribute("user");
        // FIXME what if the snapshot has not had any edits made to it? In this case, we would just be making copy upon
        // copy of a feed for no reason.
        String name = "Restore snapshot " + snapshotToRestore.name;
        Snapshot snapshot = new Snapshot(name, feedSource.id, snapshotToRestore.namespace);
        CreateSnapshotJob createSnapshotJob = new CreateSnapshotJob(userProfile, snapshot, true, false, preserveBuffer);
        JobUtils.heavyExecutor.execute(createSnapshotJob);
        return formatJobMessage(createSnapshotJob.jobId, "Restoring snapshot...");
    }

    /**
     * HTTP endpoint that triggers export of specified snapshot to GTFS. Returns a job ID with which to monitor the job
     * status. At the completion of the job, requester should fetch a download token for the snapshot.
     */
    private static String downloadSnapshotAsGTFS(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        Snapshot snapshot = getSnapshotFromRequest(req);
        // Create and kick off export job.
        // FIXME: what if a snapshot is already written to S3?
        ExportSnapshotToGTFSJob exportSnapshotToGTFSJob = new ExportSnapshotToGTFSJob(userProfile,  snapshot);
        JobUtils.heavyExecutor.execute(exportSnapshotToGTFSJob);
        return formatJobMessage(exportSnapshotToGTFSJob.jobId, "Exporting snapshot to GTFS.");
    }

    /**
     * Depending on the application's configuration, returns either temporary S3 credentials to download get an object
     * at the specified key OR a single-use download token to download a snapshot's GTFS file.
     */
    private static Object getSnapshotToken(Request req, Response res) {
        Snapshot snapshot = getSnapshotFromRequest(req);
        FeedDownloadToken token;
        String key = "snapshots/" + snapshot.id + ".zip";
        // if storing feeds on S3, first write the snapshot to GTFS file and upload to S3
        // this needs to be completed before the credentials are delivered, so that the client has
        // an actual object to download.
        // FIXME: use new FeedStore.
        if (DataManager.useS3) {
            // Return presigned download link if using S3.
            return S3Utils.downloadObject(S3Utils.DEFAULT_BUCKET, key, false, req, res);
        } else {
            // If not storing on s3, just use the token download method.
            token = new FeedDownloadToken(snapshot);
            Persistence.tokens.create(token);
            return token;
        }
    }

    /**
     * HTTP endpoint to PERMANENTLY delete a snapshot given for the specified ID.
     */
    private static Snapshot deleteSnapshot(Request req, Response res) {
        // Get the snapshot ID to restore (set the namespace pointer)
        String id = req.params("id");
        // FIXME Ensure namespace id exists in database.
        // Check feed source permissions.
        FeedSource feedSource = FeedVersionController.requestFeedSourceById(req, Actions.EDIT, "feedId");
        // Retrieve snapshot
        Snapshot snapshot = Persistence.snapshots.getById(id);
        if (snapshot == null) logMessageAndHalt(req, 400, "Must provide valid snapshot ID.");
        try {
            // Remove the snapshot and then renumber the snapshots
            snapshot.delete();
            feedSource.renumberSnapshots();
            // FIXME Are there references that need to be removed? E.g., what if the active buffer snapshot is deleted?
            // FIXME delete tables from database?
            return snapshot;
        } catch (Exception e) {
            logMessageAndHalt(req, 500, "Unknown error occurred while deleting snapshot.", e);
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
            logMessageAndHalt(req, 400, "Feed download token not valid");
        }

        Snapshot snapshot = token.retrieveSnapshot();
        Persistence.tokens.removeById(token.id);
        String fileName = snapshot.id + ".zip";
        return downloadFile(FeedVersion.feedStore.getFeed(fileName), fileName, req, res);
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/snapshot/:id", SnapshotController::getSnapshotById, json::write);
        options(apiPrefix + "secure/snapshot", (q, s) -> "");
        get(apiPrefix + "secure/snapshot", SnapshotController::getSnapshots, json::write);
        post(apiPrefix + "secure/snapshot", SnapshotController::createSnapshot, json::write);
        post(apiPrefix + "secure/snapshot/import", SnapshotController::importFeedVersionAsSnapshot, json::write);
        put(apiPrefix + "secure/snapshot/:id", SnapshotController::updateSnapshot, json::write);
        post(apiPrefix + "secure/snapshot/:id/restore", SnapshotController::restoreSnapshot, json::write);
        get(apiPrefix + "secure/snapshot/:id/download", SnapshotController::downloadSnapshotAsGTFS, json::write);
        get(apiPrefix + "secure/snapshot/:id/downloadtoken", SnapshotController::getSnapshotToken, json::write);
        delete(apiPrefix + "secure/snapshot/:id", SnapshotController::deleteSnapshot, json::write);

        get(apiPrefix + "downloadsnapshot/:token", SnapshotController::downloadSnapshotWithToken);
    }
}
