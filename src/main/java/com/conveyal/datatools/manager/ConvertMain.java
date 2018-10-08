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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.conveyal.datatools.manager.DataManager.initializeApplication;
import static com.conveyal.datatools.manager.DataManager.registerRoutes;

/**
 * Main method to run the data migration process from the v2 MapDB based application to the v3 Mongo and SQL-based
 * application. The program first seeds the MongoDB with data from a JSON dump of the manager MapDB database. It then
 * loads/validates each feed version into the SQL database, and finally it migrates the Editor MapDB to SQL. The JSON
 * dump file is provided as a program argument. The Editor MapDB directory is specified in the server.yml config file at
 * "application.data.editor_mapdb". This is all run as MonitorableJobs executed through the application's thread pool
 * executor. Once all jobs are queued, The application runs on a loop until there are no more active jobs in the jobs
 * list.
 *
 * Run instructions:
 *
 * java -Xmx6G -cp datatools.jar com.conveyal.datatools.manager.ConvertMain /path/to/env.yml /path/to/server.yml /path/to/dump.json
 *
 * An optional fourth argument can be provided to force the application to reprocess (load/validate) feed versions that
 * have already been processed.
 *
 * The primary method to run this migration is:
 * 1. First run the above java command to migrate the JSON dump and convert the editor mapdb to new snapshots.
 * 2. Next run the following java command to clean up the snapshots (the snapshots imported from the JSON dump are not
 *    updated during the editor MapDB conversion. Rather, MongoDB records are created separately, so the JSON-sourced
 *    duplicate records need to be deleted and the newly generate records updated with the JSON data):
 *      java -Xmx6G -cp datatools.jar com.conveyal.datatools.manager.ConvertMain /path/to/env.yml /path/to/server.yml updateSnapshotMetadata=true /path/to/dump.json
 *
 */
public class ConvertMain {
    // Feed ID constants for testing.
    private static final String CORTLAND_FEED_ID = "c5bdff54-82fa-47ce-ad6e-3c6517563992";
    public static final String AMTRAK_FEED_ID = "be5b775b-6811-4522-bbf6-1a408e7cf3f8";
    public static void main(String[] args) throws Exception {

        // Migration code!

        // First, set up application.
        initializeApplication(args);
        // Register HTTP endpoints so that the status endpoint is available during migration.
        registerRoutes();

        long startTime = System.currentTimeMillis();

        boolean snapshotsOnly = args.length > 2 && "snapshotsOnly=true".equals(args[2]);
        boolean updateSnapshotMetadata = args.length > 2 && "updateSnapshotMetadata=true".equals(args[2]);

        // FIXME remove migrateSingleSnapshot (just for local testing)
//        migrateSingleSnapshot(null);
        if (updateSnapshotMetadata) {
            String jsonString = FileUtils.readFileToString(new File(args[3]), Charset.defaultCharset());
            boolean result = DumpController.updateSnapshotMetadata(jsonString);
            if (result) {
                System.out.println("Snapshot metadata update successful!");
            }
            // Done.
            System.exit(0);
        } else if (!snapshotsOnly) {
            // STEP 1: Load in JSON dump into MongoDB (args 0 and 1 are the config files)
            String jsonString = FileUtils.readFileToString(new File(args[2]), Charset.defaultCharset());
            // FIXME: Do we still need to map certain project fields?
            DumpController.load(jsonString);

            // STEP 2: For each feed version, load GTFS in Postgres and validate.
            boolean force = args.length > 3 && "true".equals(args[3]);
            DumpController.validateAll(true, force, null);
        } else {
            System.out.println("Skipping JSON load and feed version load/validation due to snapshotsOnly flag");
        }

        // STEP 3: For each feed source in MongoDB, load all snapshots (and current editor buffer) into Postgres DB.
        // STEP 3A: For each snapshot/editor DB, create a snapshot Mongo object for the feed source with the FeedLoadResult.
        migrateEditorFeeds();
        System.out.println("Done queueing!!!!!!!!");
        int totalJobs = StatusController.getAllJobs().size();
        while (!StatusController.filterActiveJobs(StatusController.getAllJobs()).isEmpty()) {
            // While there are still active jobs, continue waiting.
            ConcurrentHashSet<MonitorableJob> activeJobs = StatusController.filterActiveJobs(StatusController.getAllJobs());
            System.out.println(String.format("%d/%d jobs still active. Checking for completion again in 5 seconds...", activeJobs.size(), totalJobs));
//            System.out.println(String.join(", ", activeJobs.stream().map(job -> job.name).collect(Collectors.toList())));
            int jobsInExecutor = ((ThreadPoolExecutor) DataManager.heavyExecutor).getActiveCount();
            System.out.println(String.format("Jobs in thread pool executor: %d", jobsInExecutor));
            System.out.println(String.format("Jobs completed by executor: %d", ((ThreadPoolExecutor) DataManager.heavyExecutor).getCompletedTaskCount()));
            Thread.sleep(5000);
        }
        long durationInMillis = System.currentTimeMillis() - startTime;
        System.out.println(String.format("MIGRATION COMPLETED IN %d SECONDS.", TimeUnit.MILLISECONDS.toSeconds(durationInMillis)));
        System.exit(0);
    }

    public static boolean migrateEditorFeeds (String ...feedIdsToSkip) {
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
                String feedSourceId = key.a;
                // Get feed source from MongoDB.
                FeedSource feedSource = Persistence.feedSources.getById(feedSourceId);
                if (feedSource != null) {
                    // Only migrate the feeds that have a feed source record in the MongoDB.
                    if (feedIdsToSkip != null && Arrays.asList(feedIdsToSkip).contains(feedSourceId)) {
                        // If list of feed IDs to skip is provided and the current feed ID matches, skip it.
                        System.out.println("Skipping feed. ID found in list to skip. id: " + feedSourceId);
                        continue;
                    }
                    if (!feedSourcesEncountered.contains(feedSource.id)) {
                        // If this is the first feed encountered, load the editor buffer.
                        ConvertEditorMapDBToSQL convertEditorBufferToSQL = new ConvertEditorMapDBToSQL(snapshot.id.a, null);
                        DataManager.heavyExecutor.execute(convertEditorBufferToSQL);
                        count++;
                    }
                    ConvertEditorMapDBToSQL convertEditorMapDBToSQL = new ConvertEditorMapDBToSQL(snapshot.id.a, snapshot.id.b);
                    DataManager.heavyExecutor.execute(convertEditorMapDBToSQL);
                    System.out.println(count + "/" + snapshotCount + " snapshot conversion queued");
                    feedSourcesEncountered.add(feedSource.id);
                    count++;
                } else {
                    System.out.println("Not converting snapshot. Feed source Id does not exist in application data" + feedSourceId);
                }
            }
//            long duration = System.currentTimeMillis() - startTime;
//            System.out.println("Converting " + snapshotCount + " snapshots took " + TimeUnit.MILLISECONDS.toMinutes(duration) + " minutes");
            return true;
        } catch (Exception e) {
            System.out.println("Migrating editor feeds FAILED");
            e.printStackTrace();
            return false;
        } finally {
            gtx.rollbackIfOpen();
        }
    }

    public static boolean migrateSingleSnapshot (Fun.Tuple2<String, Integer> decodedId) {
        if (decodedId == null) {
            // Use Cortland if no feed provided
            decodedId = new Fun.Tuple2<>(CORTLAND_FEED_ID, 12);
        }
        new ConvertEditorMapDBToSQL(decodedId.a, decodedId.b).run();
        return true;
    }
}
