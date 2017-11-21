package com.conveyal.datatools.editor.controllers.api;


import com.conveyal.datatools.common.utils.SparkUtils;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.datastore.GlobalTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.jobs.ProcessGtfsSnapshotExport;
import com.conveyal.datatools.editor.jobs.ProcessGtfsSnapshotMerge;
import com.conveyal.datatools.editor.models.Snapshot;
import com.conveyal.datatools.editor.models.transit.Stop;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.controllers.api.FeedSourceController;
import com.conveyal.datatools.manager.models.FeedDownloadToken;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import org.mapdb.Fun;
import org.mapdb.Fun.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.conveyal.datatools.editor.utils.JacksonSerializers;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import spark.HaltException;
import spark.Request;
import spark.Response;

import static com.conveyal.datatools.common.utils.S3Utils.getS3Credentials;
import static com.conveyal.datatools.common.utils.SparkUtils.downloadFile;
import static com.conveyal.datatools.editor.models.Snapshot.writeSnapshotAsGtfs;
import static spark.Spark.*;


public class SnapshotController {

    public static final Logger LOG = LoggerFactory.getLogger(SnapshotController.class);
    public static JsonManager<Snapshot> json =
            new JsonManager<>(Snapshot.class, JsonViews.UserInterface.class);

    public static Object getSnapshot(Request req, Response res) throws IOException {
        String id = req.params("id");
        String feedId= req.queryParams("feedId");

        GlobalTx gtx = null;
        try {
            gtx = VersionedDataStore.getGlobalTx();
            if (id != null) {
                Tuple2<String, Integer> sid = JacksonSerializers.Tuple2IntDeserializer.deserialize(id);
                if (gtx.snapshots.containsKey(sid))
                    return gtx.snapshots.get(sid);
                else
                    halt(404);
            }
            else {
                if (feedId == null)
                    feedId = req.session().attribute("feedId");

                Collection<Snapshot> snapshots;
                if (feedId == null) {
                    // if it's still null just give them everything
                    // this is used in GTFS Data Manager to retrieveById snapshots in bulk
                    // TODO this allows any authenticated user to fetch GTFS data for any agency
                    return new ArrayList<>(gtx.snapshots.values());
                }
                else {
                    // check view permissions
                    FeedSourceController.checkFeedSourcePermissions(req, Persistence.feedSources.getById(feedId), "view");
                    return gtx.snapshots.subMap(new Tuple2(feedId, null), new Tuple2(feedId, Fun.HI)).values()
                            .stream()
                            .collect(Collectors.toList());
                }
            }
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } finally {
            if (gtx != null) gtx.rollbackIfOpen();
        }
        return null;
    }

    public static Object createSnapshot (Request req, Response res) {
        GlobalTx gtx = null;
        try {
            // create a dummy snapshot from which to retrieveById values
            Snapshot original = Base.mapper.readValue(req.body(), Snapshot.class);
            Snapshot s = VersionedDataStore.takeSnapshot(original.feedId, original.name, original.comment);
            s.validFrom = original.validFrom;
            s.validTo = original.validTo;
            gtx = VersionedDataStore.getGlobalTx();

            // the snapshot we have just taken is now current; make the others not current
            Collection<Snapshot> snapshots = gtx.snapshots.subMap(new Tuple2(s.feedId, null), new Tuple2(s.feedId, Fun.HI)).values();
            for (Snapshot o : snapshots) {
                if (o.id.equals(s.id))
                    continue;

                Snapshot cloned = o.clone();
                cloned.current = false;
                gtx.snapshots.put(o.id, cloned);
            }

            gtx.commit();

            return s;
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (IOException e) {
            e.printStackTrace();
            halt(400);
        } finally {
            if (gtx != null) gtx.rollbackIfOpen();
        }
        return null;
    }

    /**
     * Create snapshot from feedVersion and load/import into editor database.
     */
    public static Boolean importSnapshot (Request req, Response res) {

        Auth0UserProfile userProfile = req.attribute("user");
        String feedVersionId = req.queryParams("feedVersionId");

        if(feedVersionId == null) {
            halt(400, SparkUtils.formatJSON("No FeedVersion ID specified", 400));
        }

        FeedVersion feedVersion = Persistence.feedVersions.getById(feedVersionId);
        if(feedVersion == null) {
            halt(404, SparkUtils.formatJSON("Could not find FeedVersion with ID " + feedVersionId, 404));
        }

        FeedSource feedSource = feedVersion.parentFeedSource();
        // check user's permission to import snapshot
        FeedSourceController.checkFeedSourcePermissions(req, feedSource, "edit");

        ProcessGtfsSnapshotMerge processGtfsSnapshotMergeJob =
                new ProcessGtfsSnapshotMerge(feedVersion, userProfile.getUser_id());

        DataManager.heavyExecutor.execute(processGtfsSnapshotMergeJob);

        halt(200, "{status: \"ok\"}");
        return null;
    }

    public static Object updateSnapshot (Request req, Response res) {
        String id = req.params("id");
        GlobalTx gtx = null;
        try {
            Snapshot s = Base.mapper.readValue(req.body(), Snapshot.class);

            Tuple2<String, Integer> sid = JacksonSerializers.Tuple2IntDeserializer.deserialize(id);

            if (s == null || s.id == null || !s.id.equals(sid)) {
                LOG.warn("snapshot ID not matched, not updating: {}, {}", s.id, id);
                halt(400);
            }

            gtx = VersionedDataStore.getGlobalTx();

            if (!gtx.snapshots.containsKey(s.id)) {
                halt(404);
            }

            gtx.snapshots.put(s.id, s);
            gtx.commit();
            return s;
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (IOException e) {
            e.printStackTrace();
            halt(400);
        } finally {
            if (gtx != null) gtx.rollbackIfOpen();
        }
        return null;
    }

    public static Object restoreSnapshot (Request req, Response res) {
        String id = req.params("id");
        Tuple2<String, Integer> decodedId = null;
        try {
            decodedId = JacksonSerializers.Tuple2IntDeserializer.deserialize(id);
        } catch (IOException e1) {
            halt(400);
        }

        GlobalTx gtx = VersionedDataStore.getGlobalTx();
        Snapshot local;
        try {
            if (!gtx.snapshots.containsKey(decodedId)) {
                halt(404);
            }

            local = gtx.snapshots.get(decodedId);

            List<Stop> stops = VersionedDataStore.restore(local);

            // the snapshot we have just restored is now current; make the others not current
            // TODO: add this loop back in... taken out in order to compile
            Collection<Snapshot> snapshots = Snapshot.getSnapshots(local.feedId);
            for (Snapshot o : snapshots) {
                if (o.id.equals(local.id))
                    continue;

                Snapshot cloned = o.clone();
                cloned.current = false;
                gtx.snapshots.put(o.id, cloned);
            }

            Snapshot clone = local.clone();
            clone.current = true;
            gtx.snapshots.put(local.id, clone);
            gtx.commit();

            return stops;
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
        } finally {
            gtx.rollbackIfOpen();
        }
        return json;
    }

    /** Export a snapshot as GTFS */
    public static Object getSnapshotToken(Request req, Response res) {
        String id = req.params("id");
        Tuple2<String, Integer> decodedId;
        FeedDownloadToken token;

        // attempt to deserialize id
        try {
            decodedId = JacksonSerializers.Tuple2IntDeserializer.deserialize(id);
        } catch (IOException e1) {
            halt(400);
            return null;
        }

        GlobalTx gtx = VersionedDataStore.getGlobalTx();
        Snapshot snapshot;
        String filePrefix;
        String key;

        // check that snapshot exists for the id
        try {
            if (!gtx.snapshots.containsKey(decodedId)) {
                halt(404);
                return null;
            }
            snapshot = gtx.snapshots.get(decodedId);
            filePrefix = snapshot.feedId + "_" + snapshot.snapshotTime;
            key = "snapshots/" + filePrefix + ".zip";

            // ensure user has permission to download snapshot, otherwise halt them
            FeedSourceController.checkFeedSourcePermissions(req, Persistence.feedSources.getById(snapshot.feedId), "view");
        } finally {
            gtx.rollbackIfOpen();
        }
        // if storing feeds on S3, first write the snapshot to GTFS file and upload to S3
        // this needs to be completed before the credentials are delivered, so that the client has
        // an actual object to download.
        if (DataManager.useS3) {
            if (!FeedStore.s3Client.doesObjectExist(DataManager.feedBucket, key)) {
                File file;
                try {
                    File tDir = new File(System.getProperty("java.io.tmpdir"));
                    file = File.createTempFile(filePrefix, ".zip");
                    file.deleteOnExit();
                    writeSnapshotAsGtfs(snapshot.id, file);
                    try {
                        LOG.info("Uploading snapshot to S3 {}", key);
                        FeedStore.s3Client.putObject(new PutObjectRequest(
                                DataManager.feedBucket, key, file));
                        file.delete();
                    } catch (AmazonServiceException ase) {
                        LOG.error("Error uploading snapshot to S3", ase);
                    }
                } catch (Exception e) {
                    LOG.error("Unable to create temp file for snapshot", e);
                }
            }
            return getS3Credentials(DataManager.awsRole, DataManager.feedBucket, key, Statement.Effect.Allow, S3Actions.GetObject, 900);
        } else {
            // if not storing on s3, just use the token download method
            token = new FeedDownloadToken(snapshot);
            Persistence.tokens.create(token);
            return token;
        }
    }

    public static Object deleteSnapshot(Request req, Response res) {
        String id = req.params("id");
        Tuple2<String, Integer> decodedId;
        try {
            decodedId = JacksonSerializers.Tuple2IntDeserializer.deserialize(id);
        } catch (IOException e1) {
            halt(400);
            return null;
        }

        GlobalTx gtx = VersionedDataStore.getGlobalTx();

        gtx.snapshots.remove(decodedId);
        gtx.commit();

        return true;
    }

    /**
     * This method is used only when NOT storing feeds on S3. It will deliver a
     * snapshot file from the local storage if a valid token is provided.
     */
    private static Object downloadSnapshotWithToken (Request req, Response res) {
        String id = req.params("token");
        FeedDownloadToken token = Persistence.tokens.getById(id);

        if(token == null || !token.isValid()) {
            halt(400, "Feed download token not valid");
        }

        Snapshot snapshot = token.retrieveSnapshot();
        File file = null;

        try {
            file = File.createTempFile("snapshot", ".zip");
            writeSnapshotAsGtfs(snapshot.id, file);
            file.deleteOnExit();
        } catch (Exception e) {
            e.printStackTrace();
            String message = "Unable to create temp file for snapshot";
            LOG.error(message);
        }
        Persistence.tokens.removeById(token.id);
        return downloadFile(file, snapshot.feedId + "_" + snapshot.snapshotTime + ".zip", res);
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/snapshot/:id", SnapshotController::getSnapshot, json::write);
        options(apiPrefix + "secure/snapshot", (q, s) -> "");
        get(apiPrefix + "secure/snapshot", SnapshotController::getSnapshot, json::write);
        post(apiPrefix + "secure/snapshot", SnapshotController::createSnapshot, json::write);
        post(apiPrefix + "secure/snapshot/import", SnapshotController::importSnapshot, json::write);
        put(apiPrefix + "secure/snapshot/:id", SnapshotController::updateSnapshot, json::write);
        post(apiPrefix + "secure/snapshot/:id/restore", SnapshotController::restoreSnapshot, json::write);
        get(apiPrefix + "secure/snapshot/:id/downloadtoken", SnapshotController::getSnapshotToken, json::write);
        delete(apiPrefix + "secure/snapshot/:id", SnapshotController::deleteSnapshot, json::write);

        get(apiPrefix + "downloadsnapshot/:token", SnapshotController::downloadSnapshotWithToken);
    }
}
