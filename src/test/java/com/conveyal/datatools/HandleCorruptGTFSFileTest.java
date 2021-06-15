package com.conveyal.datatools;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.jobs.LoadFeedJob;
import com.conveyal.datatools.manager.jobs.ProcessSingleFeedJob;
import com.conveyal.datatools.manager.jobs.ValidateFeedJob;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.conveyal.datatools.TestUtils.createFeedVersionAndAssignGtfsFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Run test to handle a corrupt GTFS file gracefully.
 */
public class HandleCorruptGTFSFileTest {
    private static FeedSource mockFeedSource;

    @BeforeAll
    public static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();
    }

    @AfterAll
    public static void tearDown() {
        mockFeedSource.delete();
    }

    @Test
    public void canHandleCorruptGTFSFile() {
        mockFeedSource = new FeedSource("Corrupt");
        int schemaCountBeforeFeedIsLoaded = TestUtils.countSchemaInDb();
        Persistence.feedSources.create(mockFeedSource);
        FeedVersion feedVersion = createFeedVersionAndAssignGtfsFile(mockFeedSource, "corrupt-gtfs-file.zip");
        ProcessSingleFeedJob job = TestUtils.createProcessSingleFeedJob(feedVersion);
        job.run();
        // Verify that no schema namespace was created (check uniqueIdentifier as well as pre-/post-load schema count).
        assertNull(feedVersion.feedLoadResult.uniqueIdentifier);
        assertEquals(schemaCountBeforeFeedIsLoaded, TestUtils.countSchemaInDb());
        // Ensure that LoadFeedJob was properly failed and that ValidateFeedJob was never started.
        for (MonitorableJob subJob : job.subJobs) {
            assertTrue(subJob.status.error);
            if (subJob instanceof LoadFeedJob) {
                assertEquals(
                    "Could not load feed due to java.util.zip.ZipException: error in opening zip file",
                    subJob.status.message
                );
            }
            if (subJob instanceof ValidateFeedJob) {
                assertEquals("Task cancelled due to error in LoadFeedJob task", subJob.status.message);
            }
        }
    }
}
