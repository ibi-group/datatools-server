package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedRetrievalMethod;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.Snapshot;
import com.conveyal.datatools.manager.models.transform.DeleteRecordsTransformation;
import com.conveyal.datatools.manager.models.transform.FeedTransformRules;
import com.conveyal.datatools.manager.models.transform.FeedTransformation;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.models.transform.ReplaceFileFromStringTransformation;
import com.conveyal.datatools.manager.models.transform.ReplaceFileFromVersionTransformation;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.conveyal.datatools.TestUtils.appendDate;
import static com.conveyal.datatools.TestUtils.assertThatSqlCountQueryYieldsExpectedCount;
import static com.conveyal.datatools.TestUtils.createFeedVersion;
import static com.conveyal.datatools.TestUtils.zipFolderFiles;
import static com.conveyal.datatools.manager.models.FeedRetrievalMethod.MANUALLY_UPLOADED;
import static com.conveyal.datatools.manager.models.FeedRetrievalMethod.VERSION_CLONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

public class ArbitraryTransformJobTest extends UnitTest {
    private static final Logger LOG = LoggerFactory.getLogger(ArbitraryTransformJob.class);
    private static final Auth0UserProfile user = Auth0UserProfile.createTestAdminUser();
    private static Project project;
    private static FeedSource feedSource;
    private FeedVersion sourceVersion;
    private FeedVersion targetVersion;

    /**
     * Initialize Data Tools and set up a simple feed source and project.
     */
    @BeforeClass
    public static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();

        // Create a project, feed sources, and feed versions to merge.
        project = new Project();
        project.name = appendDate("Test");
        Persistence.projects.create(project);

        // Bart
        feedSource = new FeedSource(appendDate("Test Feed"), project.id, MANUALLY_UPLOADED);
        Persistence.feedSources.create(feedSource);
    }

    /**
     * Clean up test database after tests finish.
     */
    @AfterClass
    public static void tearDown() {
        // Project delete cascades to feed sources.
        project.delete();
    }

    /**
     * Run set up before each test. This just resets the feed source transformation properties.
     */
    @Before
    public void setUpTest() {
        feedSource = Persistence.feedSources.getById(feedSource.id);
        feedSource.transformRules = new ArrayList<>();
        Persistence.feedSources.replace(feedSource.id, feedSource);
    }

    /**
     * Run tear down after each test. This just deletes the feed versions that were used in the test.
     */
    @After
    public void tearDownTest() {
        // Clean up
        if (sourceVersion != null) sourceVersion.delete();
        if (targetVersion != null) targetVersion.delete();
    }

    /**
     * Test that a {@link ReplaceFileFromVersionTransformation} will successfully add a GTFS+ file found in the source version
     * into the target version's GTFS file.
     */
    @Test
    public void canReplaceGtfsPlusFileFromVersion() throws IOException {
        final String table = "stop_attributes";
        // Create source version (folder contains stop_attributes file).
        sourceVersion = createFeedVersion(
            feedSource,
            zipFolderFiles("fake-agency-with-only-calendar")
        );
        // Replace file transformation runs before feed is loaded into database.
        FeedTransformation transformation = ReplaceFileFromVersionTransformation.create(sourceVersion.id, table);
        FeedTransformRules transformRules = new FeedTransformRules(transformation);
        feedSource.transformRules.add(transformRules);
        Persistence.feedSources.replace(feedSource.id, feedSource);
        // Create target version (note: GTFS folder has no stop_attributes.txt file).
        targetVersion = createFeedVersion(
            feedSource,
            zipFolderFiles("fake-agency-with-only-calendar-dates")
        );
        // Check that new version has stop_attributes file
        ZipFile zip = new ZipFile(targetVersion.retrieveGtfsFile());
        ZipEntry entry = zip.getEntry(table + ".txt");
        assertThat(entry, Matchers.notNullValue());
        // TODO Verify that stop_attributes file matches source file exactly?
    }

    @Test
    public void canDeleteTrips() throws IOException {
        // Add delete trips transformation.
        List<String> routeIds = new ArrayList<>();
        // Collect route_id values.
        routeIds.add("1");
        // Store the number of trips that run on the route_ids here.
        int numberOfTripsForRoutes = 1;
        FeedTransformation transformation = DeleteRecordsTransformation.create(
            "trips",
            "route_id",
            routeIds
        );
        FeedTransformRules transformRules = new FeedTransformRules(transformation);
        feedSource.transformRules.add(transformRules);
        Persistence.feedSources.replace(feedSource.id, feedSource);
        // Load feed.
        sourceVersion = createFeedVersion(
            feedSource,
            zipFolderFiles("fake-agency-with-only-calendar")
        );
        // Fetch snapshot where modifications were made and create new version from it.
        Snapshot snapshotWithModifications = feedSource.retrieveSnapshots().iterator().next();
        CreateFeedVersionFromSnapshotJob newVersionJob = new CreateFeedVersionFromSnapshotJob(feedSource, snapshotWithModifications, user);
        newVersionJob.run();
        // Grab the modified version and check that the trips count matches expectation.
        FeedVersion newVersion = feedSource.retrieveLatest();
        assertEquals(
            "trips count for transformed feed should be decreased by the # of records matched by the query",
            sourceVersion.feedLoadResult.trips.rowCount - numberOfTripsForRoutes,
            newVersion.feedLoadResult.trips.rowCount
        );
    }

    @Test
    public void canCloneZipFileAndTransform() throws IOException, SQLException {
        // Generate random UUID for feedId, which gets placed into the csv data.
        final String feedId = UUID.randomUUID().toString();
        final String feedInfoContent = generateFeedInfo(feedId);
        FeedTransformation transformation = ReplaceFileFromStringTransformation.create(feedInfoContent, "feed_info");
        // Create transform rules for VERSION_CLONE retrieval method.
        FeedTransformRules transformRules = new FeedTransformRules(transformation, FeedRetrievalMethod.VERSION_CLONE);
        feedSource.transformRules.add(transformRules);
        Persistence.feedSources.replace(feedSource.id, feedSource);
        // Create new version. This will handle processing the initial version, cloning that version, and applying the
        // transformations to the cloned version. It will conclude once the cloned version is processed.
        sourceVersion = createFeedVersion(
            feedSource,
            zipFolderFiles("fake-agency-with-only-calendar")
        );
        // Grab the cloned version and check assertions.
        targetVersion = feedSource.retrieveLatest();
        LOG.info("Checking canCloneZipFileAndTransform assertions.");
        assertEquals(
            "Cloned version number should increment by one over original version.",
            sourceVersion.version + 1,
            targetVersion.version
        );
        assertEquals(
            "Cloned version retrieval method should be VERSION_CLONE",
            targetVersion.retrievalMethod,
            VERSION_CLONE
        );
        assertEquals(
            "feed_info.txt row count should equal input csv data # of rows",
            2, // Magic number should match row count in string produced by generateFeedInfo
            targetVersion.feedLoadResult.feedInfo.rowCount
        );
        // Check for presence of new feedId in database (one record).
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.feed_info WHERE feed_id = '%s'",
                targetVersion.namespace,
                feedId
            ),
            1
        );
    }

    @Test
    public void replaceGtfsPlusFileFailsIfSourceIsMissing() throws IOException {
        sourceVersion = createFeedVersion(
            feedSource,
            zipFolderFiles("fake-agency-with-only-calendar")
        );
        // Replace file transformation runs before feed is loaded into database.
        // Note: stop_attributes.txt is a GTFS+ file found in source feed version.
        FeedTransformation transformation = ReplaceFileFromVersionTransformation.create(sourceVersion.id, "realtime_routes");
        FeedTransformRules transformRules = new FeedTransformRules(transformation);
        feedSource.transformRules.add(transformRules);
        Persistence.feedSources.replace(feedSource.id, feedSource);
        // Create new target version (note: bart_new.zip GTFS file has been stripped of stop_attributes.txt)
        targetVersion = createFeedVersion(
            feedSource,
            zipFolderFiles("fake-agency-with-only-calendar-dates")
        );
        // TODO Check that new version has stop_attributes file that matches source version's copy.
        assertThat(targetVersion.validationResult, Matchers.nullValue());
    }

    @Test
    public void canReplaceFeedInfo() throws SQLException, IOException {
        // Generate random UUID for feedId, which gets placed into the csv data.
        final String feedId = UUID.randomUUID().toString();
        final String feedInfoContent = generateFeedInfo(feedId);
        sourceVersion = createFeedVersion(
            feedSource,
            zipFolderFiles("fake-agency-with-only-calendar")
        );
        FeedTransformation transformation = ReplaceFileFromStringTransformation.create(feedInfoContent, "feed_info");
        FeedTransformRules transformRules = new FeedTransformRules(transformation);
        feedSource.transformRules.add(transformRules);
        Persistence.feedSources.replace(feedSource.id, feedSource);
        // Create new target version (note: the folder has no stop_attributes.txt file)
        targetVersion = createFeedVersion(
            feedSource,
            zipFolderFiles("fake-agency-with-only-calendar-dates")
        );
        LOG.info("Checking assertions.");
        assertEquals(
            "feed_info.txt row count should equal input csv data # of rows",
            2, // Magic number should match row count in string produced by generateFeedInfo
            targetVersion.feedLoadResult.feedInfo.rowCount
        );
        // Check for presence of new feedId in database (one record).
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.feed_info WHERE feed_id = '%s'",
                targetVersion.namespace,
                feedId
            ),
            1
        );
    }

    private static String generateFeedInfo(String feedId) {
        // Add feed_info csv data (purposefully with two rows, even though this is not valid GTFS).
        return String.format(
            "feed_id,feed_publisher_name,feed_publisher_url,feed_lang\n%s,BART,https://www.bart.gov/,en\n2,abc,https://example.com",
            feedId
        );
    }
}
