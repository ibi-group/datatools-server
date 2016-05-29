package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.editor.controllers.Application;
import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.datastore.GlobalTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.jobs.ProcessGtfsSnapshotExport;
import com.conveyal.datatools.editor.models.Snapshot;
import com.conveyal.datatools.editor.models.transit.Agency;
import org.mapdb.Fun.Tuple2;
import com.conveyal.datatools.editor.utils.JacksonSerializers;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import spark.Request;
import spark.Response;

import static spark.Spark.*;

public class ManagerApiController {

    // todo: Auth0 authentication


//    public static void setCORS()  {
//        Http.Header origin = new Http.Header();
//        origin.name = "Access-Control-Allow-Origin";
//        origin.values = new ArrayList<String>();
//        origin.values.add("*");
//        Http.Response.current().headers.put("Access-Control-Allow-Origin",origin);
//
//        Http.Header headers = new Http.Header();
//        headers.name = "Access-Control-Allow-Headers";
//        headers.values = new ArrayList<String>();
//        headers.values.add("Origin, X-Requested-With, Content-Type, Accept, Authorization");
//        Http.Response.current().headers.put("Access-Control-Allow-Headers",headers);
//    }
//
//    public static void options() {
//    }
//
//    public static void getSnapshot(String sourceId, String id) throws IOException {
//        GlobalTx gtx = VersionedDataStore.getGlobalTx();
//
//        System.out.println("getSnapshot for " +sourceId);
//        try {
//            if (id != null) {
//                Tuple2<String, Integer> sid = JacksonSerializers.Tuple2IntDeserializer.deserialize(id);
//                if (gtx.snapshots.containsKey(sid))
//                    renderJSON(Base.toJson(gtx.snapshots.get(sid), false));
//                else
//                    halt(404);
//
//                return;
//            } else {
//                Collection<Snapshot> snapshots = new ArrayList<>();
//                Collection<Snapshot> allSnapshots;
//
//                allSnapshots = gtx.snapshots.values();
//                for(Snapshot snapshot : allSnapshots) {
//                    Agency agency = gtx.agencies.get(snapshot.feedId);
//                    if(agency == null || agency.sourceId == null) continue;
//                    if(agency.sourceId.equals(sourceId)) {
//                        System.out.println("found!");
//                        snapshots.add(snapshot);
//                    }
//                }
//
//                renderJSON(Base.toJson(snapshots, false));
//            }
//        } finally {
//            gtx.rollback();
//        }
//    }
//
//    /** Export a snapshot as GTFS */
//    public static void exportSnapshot (String id) {
//        Tuple2<String, Integer> decodedId;
//        try {
//            decodedId = JacksonSerializers.Tuple2IntDeserializer.deserialize(id);
//        } catch (IOException e1) {
//            halt(400);
//            return;
//        }
//
//        GlobalTx gtx = VersionedDataStore.getGlobalTx();
//        Snapshot local;
//        try {
//            if (!gtx.snapshots.containsKey(decodedId)) {
//                halt(404);
//                return;
//            }
//
//            local = gtx.snapshots.get(decodedId);
//
//            File out = new File(Play.configuration.getProperty("application.publicDataDirectory"), "gtfs_" + Application.nextExportId.incrementAndGet() + ".zip");
//
//            new ProcessGtfsSnapshotExport(local, out).run();
//
//            redirect(Play.configuration.getProperty("application.appBase") + "/public/data/"  + out.getName());
//        } finally {
//            gtx.rollbackIfOpen();
//        }
//    }

}