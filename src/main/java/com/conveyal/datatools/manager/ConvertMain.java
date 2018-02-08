package com.conveyal.datatools.manager;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.editor.datastore.GlobalTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.jobs.ConvertEditorMapDBToSQL;
import com.conveyal.datatools.editor.models.Snapshot;
import com.conveyal.datatools.manager.controllers.DumpController;
import com.conveyal.datatools.manager.controllers.api.StatusController;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.apache.commons.io.FileUtils;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.mapdb.Fun;

import java.io.File;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.conveyal.datatools.manager.DataManager.initializeApplication;

public class ConvertMain {
    // Feed ID constants for testing.
    private static final String CORTLAND_FEED_ID = "c5bdff54-82fa-47ce-ad6e-3c6517563992";
    public static final String AMTRAK_FEED_ID = "be5b775b-6811-4522-bbf6-1a408e7cf3f8";
    public static void main(String[] args) throws Exception {

        // Migration code!

        // First, set up application.
        initializeApplication(args);

        long startTime = System.currentTimeMillis();
        // STEP 1: Load in JSON dump into MongoDB (args 0 and 1 are the config files)
        String jsonString = FileUtils.readFileToString(new File(args[2]), Charset.defaultCharset());
        // FIXME: Do we still need to map certain project fields?
        DumpController.load(jsonString);

        // STEP 2: For each feed version, load GTFS in Postgres and validate.
        DumpController.validateAll(true, true, null);

        // STEP 3: For each feed source in MongoDB, load all snapshots (and current editor buffer) into Postgres DB.
        // STEP 3A: For each snapshot/editor DB, create a snapshot Mongo object for the feed source with the FeedLoadResult.
        migrateEditorFeeds(null);
//        migrateSingleSnapshot(null);
        System.out.println("Done queueing!!!!!!!!");
        while (!StatusController.filterActiveJobs(StatusController.getAllJobs()).isEmpty()) {
            // While there are still active jobs, continue waiting.
            ConcurrentHashSet<MonitorableJob> activeJobs = StatusController.filterActiveJobs(StatusController.getAllJobs());
            System.out.println(String.format("There are %d jobs still active. Checking for completion again in 5 seconds...", activeJobs.size()));
            System.out.println(String.join(", ", activeJobs.stream().map(job -> job.name).collect(Collectors.toList())));
            Thread.sleep(5000);
        }
        long durationInMillis = System.currentTimeMillis() - startTime;
        System.out.println(String.format("MIGRATION COMPLETED IN %d SECONDS.", TimeUnit.MILLISECONDS.toSeconds(durationInMillis)));
        System.exit(0);
    }

    public static boolean migrateEditorFeeds (String filterFeedId) {
        // Open the Editor MapDB and write a snapshot to the SQL database.
        GlobalTx gtx = VersionedDataStore.getGlobalTx();
        try {
            long startTime = System.currentTimeMillis();
            int count = 0;
            int snapshotCount = gtx.snapshots.values().size();
            System.out.println(snapshotCount + " snapshots to convert");

            Set<String> feedSourcesEncountered = new HashSet<>();
            // Iterate over the provided snapshots and convert each one. Note: this will skip snapshots for feed IDs that
            // don't exist as feed sources in MongoDB.
            for (Map.Entry<Fun.Tuple2<String, Integer>, Snapshot> entry : gtx.snapshots.entrySet()) {
                Snapshot snapshot = entry.getValue();
                Fun.Tuple2<String, Integer> key = entry.getKey();
                // Get feed source from MongoDB.
                FeedSource feedSource = Persistence.feedSources.getById(key.a);
                if (feedSource != null) {
                    // Only migrate the feeds that have a feed source record in the MongoDB.
                    if (filterFeedId != null && !filterFeedId.equals(key.a)) {
                        // If filter feed ID is provided and the current feed ID doesn't match, skip.
//                        System.out.println("skipping feed. ID doesn't match filter value. id: " + key.a);
                        continue;
                    }
                    if (!feedSourcesEncountered.contains(feedSource.id)) {
                        // If this is the first feed encountered, load the editor buffer.
                        ConvertEditorMapDBToSQL convertEditorBufferToSQL = new ConvertEditorMapDBToSQL(snapshot.id.a, null);
                        DataManager.heavyExecutor.execute(convertEditorBufferToSQL);
                    }
                    ConvertEditorMapDBToSQL convertEditorMapDBToSQL = new ConvertEditorMapDBToSQL(snapshot.id.a, snapshot.id.b);
                    DataManager.heavyExecutor.execute(convertEditorMapDBToSQL);
                    System.out.println(count + "/" + snapshotCount + " snapshot conversion queued");
                    feedSourcesEncountered.add(feedSource.id);
                    count++;
                } else {
                    System.out.println("Not converting snapshot. Feed source Id does not exist in application data" + key.a);
                }
            }
            long duration = System.currentTimeMillis() - startTime;
            System.out.println("Converting " + snapshotCount + " snapshots took " + TimeUnit.MILLISECONDS.toMinutes(duration) + " minutes");
            return true;
        } catch (Exception e) {
            System.out.println("Migrating editor feeds FAILED");
            return false;
        } finally {
            gtx.rollbackIfOpen();
        }
    }

    public static boolean migrateSingleSnapshot (Fun.Tuple2<String, Integer> decodedId) {
        // Open the Editor MapDB and write a snapshot to the SQL database.
        GlobalTx gtx = VersionedDataStore.getGlobalTx();
        Snapshot local;
        if (decodedId == null) {
            // Cortland
            decodedId = new Fun.Tuple2<>(CORTLAND_FEED_ID, 13);
        }
        if (!gtx.snapshots.containsKey(decodedId)) {
            System.out.println("Could not find snapshot in global database");
            return false;
        }
        local = gtx.snapshots.get(decodedId);
        new ConvertEditorMapDBToSQL(local.id.a, local.id.b).run();
        return true;
    }
}
