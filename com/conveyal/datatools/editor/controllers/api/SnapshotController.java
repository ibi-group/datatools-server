package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.datastore.GlobalTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.jobs.ProcessGtfsSnapshotExport;
import com.conveyal.datatools.editor.models.Snapshot;
import com.conveyal.datatools.editor.models.transit.Stop;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import org.mapdb.Fun;
import org.mapdb.Fun.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.conveyal.datatools.editor.utils.JacksonSerializers;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import spark.Request;
import spark.Response;

import static spark.Spark.*;


public class SnapshotController {

    public static final Logger LOG = LoggerFactory.getLogger(SnapshotController.class);
    public static JsonManager<Snapshot> json =
            new JsonManager<>(Snapshot.class, JsonViews.UserInterface.class);



    public static Object getSnapshot(Request req, Response res) throws IOException {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");

        GlobalTx gtx = VersionedDataStore.getGlobalTx();
        String json = null;
        try {
            if (id != null) {
                Tuple2<String, Integer> sid = JacksonSerializers.Tuple2IntDeserializer.deserialize(id);
                if (gtx.snapshots.containsKey(sid))
                    json = Base.toJson(gtx.snapshots.get(sid), false);
                else
                    halt(404);
            }
            else {
                if (feedId == null)
                    feedId = req.session().attribute("feedId");

                Collection<Snapshot> snapshots;
                if (feedId == null) {
                    // if it's still null just give them everything
                    // this is used in GTFS Data Manager to get snapshots in bulk
                    // TODO this allows any authenticated user to fetch GTFS data for any agency
                    snapshots = gtx.snapshots.values();
                }
                else {
                    snapshots = gtx.snapshots.subMap(new Tuple2(feedId, null), new Tuple2(feedId, Fun.HI)).values();
                }

                json = Base.toJson(snapshots, false);
            }
        } finally {
            gtx.rollback();
        }
        return json;
    }

    public static Object createSnapshot (Request req, Response res) {
        GlobalTx gtx = null;
        try {
            // create a dummy snapshot from which to get values
            Snapshot original = Base.mapper.readValue(req.body(), Snapshot.class);
            Snapshot s = VersionedDataStore.takeSnapshot(original.feedId, original.name, original.comment);
            s.validFrom = original.validFrom;
            s.validTo = original.validTo;
            gtx = VersionedDataStore.getGlobalTx();

            // the snapshot we have just taken is now current; make the others not current
            // TODO: add this loop back in... taken out in order to compile
//            for (Snapshot o : gtx.snapshots.subMap(new Tuple2(s.feedId, null), new Tuple2(s.feedId, Fun.HI)).values()) {
//                if (o.id.equals(s.id))
//                    continue;
//
//                Snapshot cloned = o.clone();
//                cloned.current = false;
//                gtx.snapshots.put(o.id, cloned);
//            }

            gtx.commit();

            return Base.toJson(s, false);
        } catch (IOException e) {
            e.printStackTrace();
            halt(400);
            if (gtx != null) gtx.rollbackIfOpen();
        }
        return null;
    }

    public static Object updateSnapshot (Request req, Response res) {
        String id = req.params("id");
        GlobalTx gtx = null;
        try {
            Snapshot s = Base.mapper.readValue(req.body(), Snapshot.class);

            Tuple2<String, Integer> sid = JacksonSerializers.Tuple2IntDeserializer.deserialize(id);

            if (s == null || s.id == null || !s.id.equals(sid)) {
                LOG.warn("snapshot ID not matched, not updating: %s, %s", s.id, id);
                halt(400);
            }

            gtx = VersionedDataStore.getGlobalTx();

            if (!gtx.snapshots.containsKey(s.id)) {
                gtx.rollback();
                halt(404);
            }

            gtx.snapshots.put(s.id, s);

            gtx.commit();

            return Base.toJson(s, false);
        } catch (IOException e) {
            e.printStackTrace();
            if (gtx != null) gtx.rollbackIfOpen();
            halt(400);
        }
        return null;
    }

    public static Object restoreSnapshot (Request req, Response res) {
        String id = req.params("id");
        String json = null;
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
//            for (Snapshot o : gtx.snapshots.subMap(new Tuple2(local.feedId, null), new Tuple2(local.feedId, Fun.HI)).values()) {
//                if (o.id.equals(local.id))
//                    continue;
//
//                Snapshot cloned = o.clone();
//                cloned.current = false;
//                gtx.snapshots.put(o.id, cloned);
//            }

            Snapshot clone = local.clone();
            clone.current = true;
            gtx.snapshots.put(local.id, clone);
            gtx.commit();

            json = Base.toJson(stops, false);
        } catch (IOException e) {
            e.printStackTrace();
            halt(400);
        } finally {
            gtx.rollbackIfOpen();
        }
        return json;
    }

    /** Export a snapshot as GTFS */
    public static Object exportSnapshot (Request req, Response res) {
        String id = req.params("id");
        Tuple2<String, Integer> decodedId;
        try {
            decodedId = JacksonSerializers.Tuple2IntDeserializer.deserialize(id);
        } catch (IOException e1) {
            halt(400);
            return null;
        }

        GlobalTx gtx = VersionedDataStore.getGlobalTx();
        Snapshot local;
        try {
            if (!gtx.snapshots.containsKey(decodedId)) {
                halt(404);
                return null;
            }

            local = gtx.snapshots.get(decodedId);

            File out = new File(DataManager.config.get("application.publicDataDirectory").asText(), "gtfs_" + ".zip");

            new ProcessGtfsSnapshotExport(local, out).run();

//            redirect(Play.configuration.getProperty("application.appBase") + "/public/data/"  + out.getName());
        } finally {
            gtx.rollbackIfOpen();
        }
        return null;
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/snapshot/:id", SnapshotController::getSnapshot, json::write);
        options(apiPrefix + "secure/snapshot", (q, s) -> "");
        get(apiPrefix + "secure/snapshot", SnapshotController::getSnapshot, json::write);
        post(apiPrefix + "secure/snapshot", SnapshotController::createSnapshot, json::write);
        put(apiPrefix + "secure/snapshot/:id", SnapshotController::updateSnapshot, json::write);
//        delete(apiPrefix + "secure/snapshot/:id", SnapshotController::deleteSnapshot, json::write);
    }
}
