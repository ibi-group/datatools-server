package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.TestUtils;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.feedmerge.MergeFeedsType;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static com.conveyal.datatools.TestUtils.assertThatFeedHasNoErrorsOfType;
import static com.conveyal.datatools.TestUtils.createFeedVersion;
import static com.conveyal.datatools.TestUtils.createFeedVersionFromGtfsZip;
import static com.conveyal.datatools.TestUtils.zipFolderFiles;
import static com.conveyal.datatools.manager.models.FeedRetrievalMethod.MANUALLY_UPLOADED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for the various {@link MergeFeedsJob} merge types.
 */
public class FlexMergeFeedsJobTest extends UnitTest {
    private static final Logger LOG = LoggerFactory.getLogger(FlexMergeFeedsJobTest.class);
    private static final Auth0UserProfile user = Auth0UserProfile.createTestAdminUser();
    private static FeedVersion fakeAgencyWithFlexVersion1;
    private static FeedVersion fakeAgencyWithFlexVersion2;
    private static Project project;

    /**
     * Prepare and start a testing-specific web server
     */
    @BeforeAll
    public static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();
        Auth0Connection.setAuthDisabled(true);

        // Create a project, feed sources, and feed versions to merge.
        project = new Project();
        project.name = String.format("Test %s", new Date());
        Persistence.projects.create(project);

        FeedSource flexAgencyA = new FeedSource("FLEX-AGENCY-A", project.id, MANUALLY_UPLOADED);
        Persistence.feedSources.create(flexAgencyA);
        fakeAgencyWithFlexVersion1 = TestUtils.createFeedVersion(flexAgencyA, TestUtils.zipFolderFiles("fake-agency-with-flex-version-1"));

        FeedSource flexAgencyB = new FeedSource("FLEX-AGENCY-B", project.id, MANUALLY_UPLOADED);
        Persistence.feedSources.create(flexAgencyB);
        fakeAgencyWithFlexVersion2 = TestUtils.createFeedVersion(flexAgencyB, TestUtils.zipFolderFiles("fake-agency-with-flex-version-2"));
    }

    /**
     * Delete project on tear down (feed sources/versions will also be deleted).
     */
    @AfterAll
    public static void tearDown() {
        if (project != null) {
            project.delete();
        }
        Auth0Connection.setAuthDisabled(Auth0Connection.getDefaultAuthDisabled());
    }

    /**
     * Ensures that a regional feed merge will produce a feed that includes all entities from each feed.
     */
    @Test
    void canMergeRegional() throws SQLException {
        // Set up list of feed versions to merge.
        Set<FeedVersion> versions = new HashSet<>();
        versions.add(fakeAgencyWithFlexVersion1);
        versions.add(fakeAgencyWithFlexVersion2);
        FeedVersion mergedVersion = regionallyMergeVersions(versions);

        // Ensure the feed has the row counts we expect.
        assertEquals(
            fakeAgencyWithFlexVersion1.feedLoadResult.trips.rowCount + fakeAgencyWithFlexVersion2.feedLoadResult.trips.rowCount,
            mergedVersion.feedLoadResult.trips.rowCount,
            "trips count for merged feed should equal sum of trips for versions merged."
        );
        assertEquals(
            fakeAgencyWithFlexVersion1.feedLoadResult.routes.rowCount + fakeAgencyWithFlexVersion2.feedLoadResult.routes.rowCount,
            mergedVersion.feedLoadResult.routes.rowCount,
            "routes count for merged feed should equal sum of routes for versions merged."
            );
        assertEquals(
            mergedVersion.feedLoadResult.stops.rowCount,
            fakeAgencyWithFlexVersion1.feedLoadResult.stops.rowCount + fakeAgencyWithFlexVersion2.feedLoadResult.stops.rowCount,
            "stops count for merged feed should equal sum of stops for versions merged."
        );
        assertEquals(
            mergedVersion.feedLoadResult.agency.rowCount,
            fakeAgencyWithFlexVersion1.feedLoadResult.agency.rowCount + fakeAgencyWithFlexVersion2.feedLoadResult.agency.rowCount,
            "agency count for merged feed should equal sum of agency for versions merged."
        );
        assertEquals(
            mergedVersion.feedLoadResult.stopTimes.rowCount,
            fakeAgencyWithFlexVersion1.feedLoadResult.stopTimes.rowCount + fakeAgencyWithFlexVersion2.feedLoadResult.stopTimes.rowCount,
            "stopTimes count for merged feed should equal sum of stopTimes for versions merged."
        );
        assertEquals(
            mergedVersion.feedLoadResult.calendar.rowCount,
            fakeAgencyWithFlexVersion1.feedLoadResult.calendar.rowCount + fakeAgencyWithFlexVersion2.feedLoadResult.calendar.rowCount,
            "calendar count for merged feed should equal sum of calendar for versions merged."
        );
        assertEquals(
            mergedVersion.feedLoadResult.calendarDates.rowCount,
            fakeAgencyWithFlexVersion1.feedLoadResult.calendarDates.rowCount + fakeAgencyWithFlexVersion2.feedLoadResult.calendarDates.rowCount,
            "calendarDates count for merged feed should equal sum of calendarDates for versions merged."
        );
        assertEquals(
            mergedVersion.feedLoadResult.area.rowCount,
        fakeAgencyWithFlexVersion1.feedLoadResult.area.rowCount + fakeAgencyWithFlexVersion2.feedLoadResult.area.rowCount,
            "area count for merged feed should equal sum of area for versions merged."
        );
        assertEquals(
            mergedVersion.feedLoadResult.bookingRules.rowCount,
            fakeAgencyWithFlexVersion1.feedLoadResult.bookingRules.rowCount + fakeAgencyWithFlexVersion2.feedLoadResult.bookingRules.rowCount,
            "bookingRules count for merged feed should equal sum of area for versions merged."
        );
        assertEquals(
            mergedVersion.feedLoadResult.stopAreas.rowCount,
            fakeAgencyWithFlexVersion1.feedLoadResult.stopAreas.rowCount + fakeAgencyWithFlexVersion2.feedLoadResult.stopAreas.rowCount,
            "stopAreas count for merged feed should equal sum of area for versions merged."
        );
        assertEquals(
            mergedVersion.feedLoadResult.locations.rowCount,
            fakeAgencyWithFlexVersion1.feedLoadResult.locations.rowCount,
            "Merged versions contain the same locations, only one set of locations should remain after merge."
        );
        assertEquals(
            mergedVersion.feedLoadResult.locationShapes.rowCount,
            fakeAgencyWithFlexVersion1.feedLoadResult.locationShapes.rowCount,
            "Merged versions contain the same location shapes, only one set of location shapes should remain after merge."
        );
        // Ensure there are no referential integrity errors, duplicate ID, or wrong number of
        // fields errors.
        assertThatFeedHasNoErrorsOfType(
            mergedVersion.namespace,
            NewGTFSErrorType.REFERENTIAL_INTEGRITY.toString(),
            NewGTFSErrorType.DUPLICATE_ID.toString(),
            NewGTFSErrorType.WRONG_NUMBER_OF_FIELDS.toString()
        );
    }

    /**
     * Merges a set of FeedVersions and then creates a new FeedSource and FeedVersion of the merged feed.
     */
    private FeedVersion regionallyMergeVersions(Set<FeedVersion> versions) {
        MergeFeedsJob mergeFeedsJob = new MergeFeedsJob(user, versions, project.id, MergeFeedsType.REGIONAL);
        // Run the job in this thread (we're not concerned about concurrency here).
        mergeFeedsJob.run();
        LOG.info("Regional merged file: {}", mergeFeedsJob.mergedVersion.retrieveGtfsFile().getAbsolutePath());
        return mergeFeedsJob.mergedVersion;
    }
}
