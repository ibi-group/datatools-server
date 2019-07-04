package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.TestUtils;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static com.conveyal.datatools.TestUtils.createFeedVersion;
import static org.junit.Assert.assertEquals;

/**
 * Tests for the various {@link MergeFeedsJob} merge types.
 */
public class MergeFeedsJobTest {
    private static final Logger LOG = LoggerFactory.getLogger(MergeFeedsJobTest.class);
    private static FeedVersion bartVersion1;
    private static FeedVersion bartVersion2;
    private static FeedVersion calTrainVersion;
    private static Project project;
    private static FeedVersion napaVersion;

    /**
     * Prepare and start a testing-specific web server
     */
    @BeforeClass
    public static void setUp() throws IOException {
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
        FeedSource napa = new FeedSource("Napa");
        napa.projectId = project.id;
        Persistence.feedSources.create(napa);
        napaVersion = createFeedVersion(napa, "napa-no-agency-id.zip");
    }

    /**
     * Ensures that a regional feed merge will produce a feed that includes all entities from each feed.
     */
    @Test
    public void canMergeRegional() throws SQLException {
        // Set up list of feed versions to merge.
        Set<FeedVersion> versions = new HashSet<>();
        versions.add(bartVersion1);
        versions.add(calTrainVersion);
        versions.add(napaVersion);
        MergeFeedsJob mergeFeedsJob = new MergeFeedsJob("test", versions, project.id, MergeFeedsType.REGIONAL);
        // Run the job in this thread (we're not concerned about concurrency here).
        mergeFeedsJob.run();
        // Create a new feed source/version for the merged feed, so we can easily analyze its contents.
        FeedSource source = new FeedSource("Merged feed");
        source.projectId = project.id;
        Persistence.feedSources.create(source);
        File feed = FeedVersion.feedStore.getFeed(project.id + ".zip");
        LOG.info("Regional merged file: {}", feed.getAbsolutePath());
        FeedVersion mergedVersion = createFeedVersion(source, feed);
        // Ensure the feed has the row counts we expect.
        assertEquals(
            "trips count for merged feed should equal sum of trips for versions merged.",
            bartVersion1.feedLoadResult.trips.rowCount + calTrainVersion.feedLoadResult.trips.rowCount + napaVersion.feedLoadResult.trips.rowCount,
            mergedVersion.feedLoadResult.trips.rowCount
        );
        assertEquals(
            "routes count for merged feed should equal sum of routes for versions merged.",
            bartVersion1.feedLoadResult.routes.rowCount + calTrainVersion.feedLoadResult.routes.rowCount + napaVersion.feedLoadResult.routes.rowCount,
            mergedVersion.feedLoadResult.routes.rowCount
            );
        assertEquals(
            "stops count for merged feed should equal sum of stops for versions merged.",
            mergedVersion.feedLoadResult.stops.rowCount,
            bartVersion1.feedLoadResult.stops.rowCount + calTrainVersion.feedLoadResult.stops.rowCount + napaVersion.feedLoadResult.stops.rowCount
        );
        assertEquals(
            "agency count for merged feed should equal sum of agency for versions merged.",
            mergedVersion.feedLoadResult.agency.rowCount,
            bartVersion1.feedLoadResult.agency.rowCount + calTrainVersion.feedLoadResult.agency.rowCount + napaVersion.feedLoadResult.agency.rowCount
        );
        assertEquals(
            "stopTimes count for merged feed should equal sum of stopTimes for versions merged.",
            mergedVersion.feedLoadResult.stopTimes.rowCount,
            bartVersion1.feedLoadResult.stopTimes.rowCount + calTrainVersion.feedLoadResult.stopTimes.rowCount + napaVersion.feedLoadResult.stopTimes.rowCount
        );
        assertEquals(
            "calendar count for merged feed should equal sum of calendar for versions merged.",
            mergedVersion.feedLoadResult.calendar.rowCount,
            bartVersion1.feedLoadResult.calendar.rowCount + calTrainVersion.feedLoadResult.calendar.rowCount + napaVersion.feedLoadResult.calendar.rowCount
        );
        assertEquals(
            "calendarDates count for merged feed should equal sum of calendarDates for versions merged.",
            mergedVersion.feedLoadResult.calendarDates.rowCount,
            bartVersion1.feedLoadResult.calendarDates.rowCount + calTrainVersion.feedLoadResult.calendarDates.rowCount + napaVersion.feedLoadResult.calendarDates.rowCount
        );
        // Ensure there are no referential integrity errors, duplicate ID, or wrong number of
        // fields errors.
        TestUtils.assertThatFeedHasNoErrorsOfType(
            mergedVersion.namespace,
            NewGTFSErrorType.REFERENTIAL_INTEGRITY.toString(),
            NewGTFSErrorType.DUPLICATE_ID.toString(),
            NewGTFSErrorType.WRONG_NUMBER_OF_FIELDS.toString()
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
    public void canMergeBARTFeeds() throws SQLException {
        Set<FeedVersion> versions = new HashSet<>();
        versions.add(bartVersion1);
        versions.add(bartVersion2);
        MergeFeedsJob mergeFeedsJob = new MergeFeedsJob("test", versions, "merged_output", MergeFeedsType.MTC);
        // This time, turn off the failOnDuplicateTripId flag.
        mergeFeedsJob.failOnDuplicateTripId = false;
        mergeFeedsJob.run();
        // Result should succeed this time.
        assertEquals(
            "Merged directions count should equal expected value.",
            2, // Magic number represents expected number of lines after merge.
            mergeFeedsJob.mergeFeedsResult.linesPerTable.get("directions").intValue()
        );
        assertEquals(
            "Merged feed trip count should equal expected value.",
            4552, // Magic number represents the number of trips in the merged BART feed.
            mergeFeedsJob.mergedVersion.feedLoadResult.trips.rowCount
        );
        assertEquals(
            "Merged feed route count should equal expected value.",
            9, // Magic number represents the number of routes in the merged BART feed.
            mergeFeedsJob.mergedVersion.feedLoadResult.routes.rowCount
        );
        // Ensure there are no referential integrity errors or duplicate ID errors.
        TestUtils.assertThatFeedHasNoErrorsOfType(
            mergeFeedsJob.mergedVersion.namespace,
            NewGTFSErrorType.REFERENTIAL_INTEGRITY.toString(),
            NewGTFSErrorType.DUPLICATE_ID.toString()
        );
    }
}
