package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedTransformation;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.models.ReplaceFileFromStringTransformation;
import com.conveyal.datatools.manager.models.ReplaceFileTransformation;
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
import java.util.Date;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.conveyal.datatools.TestUtils.assertThatSqlCountQueryYieldsExpectedCount;
import static com.conveyal.datatools.TestUtils.createFeedVersionFromGtfsZip;
import static com.conveyal.datatools.manager.models.FeedSource.FeedRetrievalMethod.MANUALLY_UPLOADED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ArbitraryFeedTransformJobTest extends UnitTest {
    private static final Logger LOG = LoggerFactory.getLogger(ArbitraryFeedTransformJob.class);
    private static Project project;
    private static FeedSource bart;
    private FeedVersion bartVersion;
    private FeedVersion newBartVersion;

    /**
     * Initialize Data Tools and set up a simple feed source and project.
     */
    @BeforeClass
    public static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();

        // Create a project, feed sources, and feed versions to merge.
        project = new Project();
        project.name = String.format("Test %s", new Date().toString());
        Persistence.projects.create(project);

        // Bart
        bart = new FeedSource("BART", project.id, MANUALLY_UPLOADED);
        Persistence.feedSources.create(bart);
    }

    /**
     * Clean up test database after tests finish.
     */
    @AfterClass
    public static void tearDown() {
        bart.delete();
        project.delete();
    }

    /**
     * Run set up before each test. This just resets the feed source transformation properties.
     */
    @Before
    public void setUpTest() {
        bart = Persistence.feedSources.getById(bart.id);
        bart.transformations = new ArrayList<>();
        bart.transformInPlace = true;
        Persistence.feedSources.replace(bart.id, bart);
    }

    /**
     * Run tear down after each test. This just deletes the feed versions that were used in the test.
     *
     * Note: this assumes that each test will create a feed version for bartVersion and newBartVersion.
     */
    @After
    public void tearDownTest() {
        // Clean up
        bartVersion.delete();
        newBartVersion.delete();
    }

    @Test
    public void canReplaceGtfsPlusFile() throws IOException {
        final String table = "stop_attributes";
        bartVersion = createFeedVersionFromGtfsZip(bart, "bart_old.zip");
        // Replace file transformation runs before feed is loaded into database.
        // Note: stop_attributes.txt is a GTFS+ file found in BART's feed.
        FeedTransformation transformation = ReplaceFileTransformation.create(bartVersion.id, table);
        bart.transformations.add(transformation);
        Persistence.feedSources.replace(bart.id, bart);
        // Create new BART version (note: bart_new.zip GTFS file has been stripped of stop_attributes.txt)
        newBartVersion = createFeedVersionFromGtfsZip(bart, "bart_new.zip");
        // Check that new version has stop_attributes file
        ZipFile zip = new ZipFile(newBartVersion.retrieveGtfsFile());
        ZipEntry entry = zip.getEntry(table + ".txt");
        assertThat(entry, Matchers.notNullValue());
        // TODO Verify that stop_attributes file matches source file exactly?
    }

    @Test
    public void replaceGtfsPlusFileFailsIfSourceIsMissing() {
        bartVersion = createFeedVersionFromGtfsZip(bart, "bart_new.zip");
        // Replace file transformation runs before feed is loaded into database.
        // Note: stop_attributes.txt is a GTFS+ file found in BART's feed.
        FeedTransformation transformation = ReplaceFileTransformation.create(bartVersion.id, "stop_attributes");
        bart.transformations.add(transformation);
        Persistence.feedSources.replace(bart.id, bart);
        // Create new BART version (note: bart_new.zip GTFS file has been stripped of stop_attributes.txt)
        newBartVersion = createFeedVersionFromGtfsZip(bart, "bart_old.zip");
        // TODO Check that new version has stop_attributes file that matches source version's copy.
        assertThat(newBartVersion.validationResult, Matchers.nullValue());
    }

    @Test
    public void canReplaceFeedInfo() throws SQLException {
        // Generate random UUID for feedId, which gets placed into the csv data.
        final String feedId = UUID.randomUUID().toString();
        bartVersion = createFeedVersionFromGtfsZip(bart, "bart_old.zip");
        // Add feed_info csv data (purposefully with two rows, even though this is not valid GTFS).
        final String feedInfoContent = String.format(
            "feed_id,feed_publisher_name,feed_publisher_url,feed_lang\n%s,BART,https://www.bart.gov/,en\n2,abc,https://example.com",
            feedId
        );
        FeedTransformation transformation = ReplaceFileFromStringTransformation.create(feedInfoContent, "feed_info");
        bart.transformations.add(transformation);
        Persistence.feedSources.replace(bart.id, bart);
        // Create new BART version (note: bart_new.zip GTFS file has been stripped of
        // stop_attributes.txt)
        newBartVersion = createFeedVersionFromGtfsZip(bart, "bart_new.zip");
        LOG.info("Checking assertions.");
        assertEquals(
            "feed_info.txt row count should equal input csv data # of rows",
            2, // Magic number should match row count in feed_info csv string above.
            newBartVersion.feedLoadResult.feedInfo.rowCount
        );
        // Check for presence of new feedId in database (one record).
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.feed_info WHERE feed_id = '%s'",
                newBartVersion.namespace,
                feedId
            ),
            1
        );

    }
}
