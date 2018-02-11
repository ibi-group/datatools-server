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
 * java -Xmx6G -cp datatools.jar:lib/* com.conveyal.datatools.manager.ConvertMain /path/to/env.yml /path/to/server.yml /path/to/dump.json
 *
 * An optional fourth argument can be provided to force the application to reprocess (load/validate) feed versions that
 * have already been processed.
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

        if (!snapshotsOnly) {
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
        migrateEditorFeeds(
    "033cd499-a215-4d13-b00d-24c9c4bf9cd0",
                    "05195828-4d50-4419-b22e-8c5c99cf78d9",
                    "063a7951-b698-40d6-adb8-92cf65439e17",
                    "0aa7999d-65fa-457e-b8ba-1b0d50e72bf1",
                    "0ec4aeb1-01d5-45cc-97a1-d424ade5f78b",
                    "1eef5b51-001f-41a4-9a71-47689a65477b",
                    "225f831b-fa0e-4138-a62d-13048e9fd3a3",
                    "24e99790-211d-4f92-b1d2-147e6f3d5040",
                    "27d40e61-4764-45f6-b9dd-86b3bf11591e",
                    "29cf9b11-20f1-48fb-8021-323b90e4ee0c",
                    "2a279c63-b22f-4851-9409-ceed8162bdcd",
                    "313bfca7-0436-4bcc-bb25-777b895dfc5f",
                    "3631a150-1b0c-4445-b474-4445ba1ca2d9",
                "3a5b3817-106a-490b-8814-fe1165cba249",
                "3c9c2c79-851b-45cc-b042-bfb7746e4fe0",
        "3d09ebde-0383-47fe-906d-34aa19e90067",
        "3d1048ac-8f99-4167-bfe8-a20f6bf10e11",
        "3effd49a-3b7a-46bb-8453-950496682dc6",
        "40695f32-635d-4793-892b-f7ed97d28d24",
        "4393e20e-3d9a-41c6-813f-a2baf86d9e0f",
        "43c31f6e-0835-46ef-a68f-452f509dcda8",
        "46d8975f-6706-4f76-aa9d-67368461529a",
        "4787c168-6a14-4fe2-b3ba-c0eb7a093885",
        "4a93db99-93a8-4ae9-b681-4da13a150296",
        "527b7a58-6488-4fd8-b4f2-3ee65cde6fc1",
        "56364c3c-7802-42a2-857e-148f8b3623c5",
        "593526e8-2270-4649-9109-325f011a2b23",
                "5ab746a6-a22a-4d58-8567-611e03e718a8",
        "5dfac884-9e86-44f1-a041-2fa1da57b3dc",
        "6394c514-c860-40a0-9eb8-d96e8dbd2be7",
        "63dcf8a5-b2c6-470e-849f-36b72022af16",
        "65867fea-c4d7-40a9-952d-192c20a8a67a",
        "6853a46c-f886-470f-917a-6905711b17cd",
        "6b133109-17e2-44bf-a729-8ee6ea13c6a6",
        "706eac84-8c1a-475e-b31b-bd9247980062",
        "74eb8a63-c7bd-4ea0-8361-e58c87acd7d7",
        "7a10a8ac-d448-4fab-9779-9599c63d5c96",
        "7a3b91f9-e233-4289-909a-3754db0270e0",
        "7cea9981-4c45-4e11-962a-cf648d9922c8",
        "7d0706f9-5334-4006-9a75-2c5558a5029f",
        "80584468-a98a-4d4b-81c7-f41910c08a5a",
        "86513906-cb98-44fb-9e18-a50ca11f7a67",
        "874846ab-c796-4f68-b48c-f6a26659d98c",
        "87a2b218-f748-408b-b1e9-5a4c282f8974",
        "87c64274-bcf2-470f-99de-d14d065dff99",
        "8cc6e641-2263-4925-b78a-bea574e81517",
        "8db7f031-73b6-4f26-8708-4b4a14f3d3ba",
        "8ff67add-9cd3-48f2-98c9-961dd6f331d7",
        "90029cc1-383a-414a-bd8d-44127e4cc1ff",
        "927e6b67-3c1c-4252-ae12-d4a856689dc9",
        "94515905-4dbc-412f-92d8-678bd8f577c1",
        "96c53f2e-9494-4f08-9891-39f85286ad9b",
        "9978c06b-dabc-4c37-8ff0-14cd7028f9b1",
        "a08cf935-3092-481e-af6c-b97a47565dc4",
                "a1ef7e3c-f4ad-4ad1-88f2-2e6ad1e532f6",
                "a2d12948-3ca4-440f-9ffb-9afad86fcde5",
                "a4aa8e9e-e777-44b0-ba82-918bae582068",
                "a8cd1ac3-07e3-4b1a-b5e5-06d4770d4772",
        "aa30019b-fbc7-4f85-9ba5-8367a8f3e19e",
        "aa469242-cfa3-4790-b59a-b91e227a6967",
        "aa6cf70d-43d9-4567-aac3-9f74d2fe76ec",
        "aaf93ff6-e56d-45ca-8ca0-0c850e38f7e6",
        "abb8b78b-2710-4870-8aec-8ea66775cc93",
        "ac400211-bad0-4911-86e6-7ca49f15d96b",
        "ad06e0ce-6ab2-435d-847d-401877bcf203",
        "ad37dc59-12a3-4087-bb07-2e5ff4ad7d26",
        "af29e078-6344-4781-a701-668be4193b50",
        "af9abe7d-e63c-4096-8d81-bea7b6820324",
        "b0aa6698-e751-4a5b-a633-76832131bfb9",
        "b106d49a-0ede-46f2-abe3-cdcb163d6771",
        "b50befa0-cab1-4b8a-950f-93304e2661c1",
        "b8893bbc-ae8c-4186-9323-346da10b8ad2",
        "be5b775b-6811-4522-bbf6-1a408e7cf3f8",
        "c27664d6-1e6a-443d-aa32-ca247777f7c8",
        "c30a7db4-62a7-4775-a4d1-7eb1105d72d2",
                "c3ffe8d4-7331-4ac2-90e0-b59fd5c5b014",
                "c5bdff54-82fa-47ce-ad6e-3c6517563992",
                "c7c32d42-bb2c-42a1-9145-ea8e0df5a9df",
                "c80ed7f6-e374-405a-89e8-dfe34bc734a6",
                "c9084cd3-1682-4426-ae42-49f50de74145",
        "c97061f2-f2d1-4133-8a71-30423f0dab7b",
        "ca418447-35a5-4bce-8ced-d230616e7ff5",
        "cd1c3566-0cac-44f6-95a9-968672b9b82a",
        "d053be11-ca43-4df9-a31b-7e6915818eb3",
        "d286fb60-eb14-4457-ba97-82a9a16d000e",
        "d369e596-8ed3-4b17-ba4b-b8c2b5a234c0",
        "d9a31dc1-7af1-4ab2-8f2f-147020eccdda"
        );
//        migrateSingleSnapshot(null);
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
                // Get feed source from MongoDB.
                FeedSource feedSource = Persistence.feedSources.getById(key.a);
                if (feedSource != null) {
                    // Only migrate the feeds that have a feed source record in the MongoDB.
                    if (feedIdsToSkip != null && Arrays.asList(feedIdsToSkip).contains(key.a)) {
                        // If list of feed IDs to skip is provided and the current feed ID matches, skip it.
                        System.out.println("Skipping feed. ID found in list to skip. id: " + key.a);
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
                    System.out.println("Not converting snapshot. Feed source Id does not exist in application data" + key.a);
                }
            }
//            long duration = System.currentTimeMillis() - startTime;
//            System.out.println("Converting " + snapshotCount + " snapshots took " + TimeUnit.MILLISECONDS.toMinutes(duration) + " minutes");
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
