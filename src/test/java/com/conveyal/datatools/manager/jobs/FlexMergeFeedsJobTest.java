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
import com.conveyal.gtfs.loader.FeedLoadResult;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.loader.TableLoadResult;
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

        FeedLoadResult r1 = fakeAgencyWithFlexVersion1.feedLoadResult;
        FeedLoadResult r2 = fakeAgencyWithFlexVersion2.feedLoadResult;
        FeedLoadResult merged = mergedVersion.feedLoadResult;

        // Ensure the feed has the row counts we expect.
        assertRowCount(r1.agency, r2.agency, merged.agency, "Agency count for merged feed should equal sum of agency for versions merged.");
        assertRowCount(r1.area, r2.area, merged.area, "Area count for merged feed should equal sum of area for versions merged.");
        assertRowCount(r1.attributions, r2.attributions, merged.attributions, "Attributions count for merged feed should equal sum of attributions for versions merged.");
        assertRowCount(r1.bookingRules, r2.bookingRules, merged.bookingRules, "Booking rules count for merged feed should equal sum of booking rules for versions merged.");
        assertRowCount(r1.calendar, r2.calendar, merged.calendar, "Calendar count for merged feed should equal sum of calendar for versions merged.");
        assertRowCount(r1.calendarDates, r2.calendarDates, merged.calendarDates, "Calendar dates count for merged feed should equal sum of calendar dates for versions merged.");
        assertRowCount(r1.fareAttributes, r2.fareAttributes, merged.fareAttributes, "Fare attributes count for merged feed should equal sum of fare attributes for versions merged.");
        assertRowCount(r1.fareRules, r2.fareRules, merged.fareRules, "Fare rules count for merged feed should equal sum of fare rules for versions merged.");
        assertRowCount(r1.frequencies, r2.frequencies, merged.frequencies, "Frequencies count for merged feed should equal sum of frequencies for versions merged.");
        assertRowCount(r1.locations, r2.locations, merged.locations, "Locations count for merged feed should equal sum of locations for versions merged.");
        assertRowCount(r1.locationShapes, r2.locationShapes, merged.locationShapes, "Location shapes count for merged feed should equal sum of location shapes for versions merged.");
        assertRowCount(r1.routes, r2.routes, merged.routes, "Routes count for merged feed should equal sum of routes for versions merged.");
        assertRowCount(r1.shapes, r2.shapes, merged.shapes, "Shapes count for merged feed should equal sum of shapes for versions merged.");
        assertRowCount(r1.stops, r2.stops, merged.stops, "Stops count for merged feed should equal sum of stops for versions merged.");
        assertRowCount(r1.stopAreas, r2.stopAreas, merged.stopAreas, "Stop areas count for merged feed should equal sum of stop areas for versions merged.");
        assertRowCount(r1.stopTimes, r2.stopTimes, merged.stopTimes, "Stop times count for merged feed should equal sum of stopTimes for versions merged.");
        assertRowCount(r1.trips, r2.trips, merged.trips, "Trips count for merged feed should equal sum of trips for versions merged.");
        assertRowCount(r1.translations, r2.translations, merged.translations, "Translations count for merged feed should equal sum of translations for versions merged.");

        // Ensure there are no referential integrity errors, duplicate ID, or wrong number of fields errors.
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

    /**
     * Helper method to confirm that the sum of the two feed table rows match the merged feed table rows.
     */
    private void assertRowCount(TableLoadResult feedOne, TableLoadResult feedTwo, TableLoadResult feedMerged, String message) {
        assertEquals(feedOne.rowCount + feedTwo.rowCount, feedMerged.rowCount, message);
    }
}
