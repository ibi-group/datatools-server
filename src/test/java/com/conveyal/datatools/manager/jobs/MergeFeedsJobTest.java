package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.LoadFeedTest;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the various {@link MergeFeedsJob} merge types.
 */
public class MergeFeedsJobTest {
    private static FeedVersion bartVersion1;
    private static FeedVersion bartVersion2;
    private static FeedVersion calTrainVersion;
    private static Project project;
    /**
     * Prepare and start a testing-specific web server
     */
    @BeforeClass
    public static void setUp() {
        // start server if it isn't already running
        DatatoolsTest.setUp();
        // Create a project, feed sources, and feed versions to merge.
        project = new Project();
        project.name = String.format("Test %s", new Date().toString());
        Persistence.projects.create(project);
        FeedSource bart = new FeedSource("BART");
        bart.projectId = project.id;
        Persistence.feedSources.create(bart);
        bartVersion1 = createFeedVersion(bart, "bart_old.zip");
        bartVersion2 = createFeedVersion(bart, "bart_new.zip");
        FeedSource caltrain = new FeedSource("Caltrain");
        caltrain.projectId = project.id;
        Persistence.feedSources.create(caltrain);
        calTrainVersion = createFeedVersion(caltrain, "caltrain_gtfs.zip");
    }

    /**
     * Ensures that a regional feed merge will produce a feed that includes all entities from each feed.
     */
    @Test
    public void canMergeRegional() {
        Set<FeedVersion> versions = new HashSet<>();
        versions.add(bartVersion1);
        versions.add(calTrainVersion);
        MergeFeedsJob mergeFeedsJob = new MergeFeedsJob("test", versions, project.id, MergeFeedsType.REGIONAL);
        // Run the job in this thread (we're not concerned about concurrency here).
        mergeFeedsJob.run();
        // Create a new feed source/version for the merged feed, so we can easily analyze its contents.
        FeedSource source = new FeedSource("Merged feed");
        source.projectId = project.id;
        Persistence.feedSources.create(source);
        File feed = FeedVersion.feedStore.getFeed(project.id + ".zip");
        FeedVersion mergedVersion = createFeedVersion(source, feed);
        // Ensure the feed has the row counts we expect.
        assertEquals(
            "trips count for merged feed should equal sum of trips for versions merged.",
            bartVersion1.feedLoadResult.trips.rowCount + calTrainVersion.feedLoadResult.trips.rowCount,
            mergedVersion.feedLoadResult.trips.rowCount
        );
        assertEquals(
            "routes count for merged feed should equal sum of routes for versions merged.",
            bartVersion1.feedLoadResult.routes.rowCount + calTrainVersion.feedLoadResult.routes.rowCount,
            mergedVersion.feedLoadResult.routes.rowCount
            );
        assertEquals(
            "stops count for merged feed should equal sum of stops for versions merged.",
            mergedVersion.feedLoadResult.stops.rowCount,
            bartVersion1.feedLoadResult.stops.rowCount + calTrainVersion.feedLoadResult.stops.rowCount
        );
        assertEquals(
            "agency count for merged feed should equal sum of agency for versions merged.",
            mergedVersion.feedLoadResult.agency.rowCount,
            bartVersion1.feedLoadResult.agency.rowCount + calTrainVersion.feedLoadResult.agency.rowCount
        );
        assertEquals(
            "stopTimes count for merged feed should equal sum of stopTimes for versions merged.",
            mergedVersion.feedLoadResult.stopTimes.rowCount,
            bartVersion1.feedLoadResult.stopTimes.rowCount + calTrainVersion.feedLoadResult.stopTimes.rowCount
        );
        assertEquals(
            "calendar count for merged feed should equal sum of calendar for versions merged.",
            mergedVersion.feedLoadResult.calendar.rowCount,
            bartVersion1.feedLoadResult.calendar.rowCount + calTrainVersion.feedLoadResult.calendar.rowCount
        );
        assertEquals(
            "calendarDates count for merged feed should equal sum of calendarDates for versions merged.",
            mergedVersion.feedLoadResult.calendarDates.rowCount,
            bartVersion1.feedLoadResult.calendarDates.rowCount + calTrainVersion.feedLoadResult.calendarDates.rowCount
        );
    }

    /**
     * Ensures that an MTC merge of feeds with duplicate trip IDs will fail.
     */
    @Test
    public void mergeMTCShouldFailOnDuplicateTrip() {
        Set<FeedVersion> versions = new HashSet<>();
        versions.add(bartVersion1);
        versions.add(bartVersion2);
        MergeFeedsJob mergeFeedsJob = new MergeFeedsJob("test", versions, "merged_output", MergeFeedsType.MTC);
        // Run the job in this thread (we're not concerned about concurrency here).
        mergeFeedsJob.run();
        // Result should fail.
        assertEquals(
            "Merge feeds job should fail due to duplicate trip IDs.",
            true,
            mergeFeedsJob.mergeFeedsResult.failed
        );
    }

    /**
     * Tests that the MTC merge strategy will successfully merge BART feeds. Note: this test turns off
     * {@link MergeFeedsJob#failOnDuplicateTripId} in order to force the merge to succeed even though there are duplicate
     * trips contained within.
     */
    @Test
    public void canMergeBARTFeeds() {
        Set<FeedVersion> versions = new HashSet<>();
        versions.add(bartVersion1);
        versions.add(bartVersion2);
        MergeFeedsJob mergeFeedsJob = new MergeFeedsJob("test", versions, "merged_output", MergeFeedsType.MTC);
        // This time, turn off the failOnDuplicateTripId flag.
        mergeFeedsJob.failOnDuplicateTripId = false;
        mergeFeedsJob.run();
        // Result should succeed this time.
        assertEquals(
            "Merged feed trip count should equal expected value.",
            4552,
            mergeFeedsJob.mergedVersion.feedLoadResult.trips.rowCount
        );
        assertEquals(
            "Merged feed route count should equal expected value.",
            9,
            mergeFeedsJob.mergedVersion.feedLoadResult.routes.rowCount
        );
    }

    /**
     * Utility function to create a feed version during tests. Note: this is intended to run the job in the same thread,
     * so that tasks can run synchronously.
     */
    public static FeedVersion createFeedVersion(FeedSource source, String gtfsFileName) {
        File gtfsFile = new File(LoadFeedTest.class.getResource(gtfsFileName).getFile());
        return createFeedVersion(source, gtfsFile);
    }

    /**
     * Utility function to create a feed version during tests. Note: this is intended to run the job in the same thread,
     * so that tasks can run synchronously.
     */
    public static FeedVersion createFeedVersion(FeedSource source, File gtfsFile) {
        FeedVersion version = new FeedVersion(source);
        InputStream is;
        try {
            is = new FileInputStream(gtfsFile);
            version.newGtfsFile(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
        ProcessSingleFeedJob processSingleFeedJob = new ProcessSingleFeedJob(version, "test", true);
        // Run in same thread.
        processSingleFeedJob.run();
        return version;
    }

}
