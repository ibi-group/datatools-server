package com.conveyal.datatools;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.jobs.LoadFeedJob;
import com.conveyal.datatools.manager.jobs.ProcessSingleFeedJob;
import com.conveyal.datatools.manager.jobs.ValidateFeedJob;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
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
    private static Project mockProject;

    @BeforeAll
    public static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();
    }

    @AfterAll
    public static void tearDown() {
        mockProject.delete();
    }

    @Test
    public void canHandleCorruptGTFSFile() {
        mockProject = new Project();
        Persistence.projects.create(mockProject);
        mockFeedSource = new FeedSource("Corrupt");
        mockFeedSource.projectId = mockProject.id;
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
                    "Could not load feed due to java.util.zip.ZipException: zip END header not found",
                    subJob.status.message
                );
            }
            if (subJob instanceof ValidateFeedJob) {
                assertEquals("Task cancelled due to error in LoadFeedJob task", subJob.status.message);
            }
        }
    }
}
