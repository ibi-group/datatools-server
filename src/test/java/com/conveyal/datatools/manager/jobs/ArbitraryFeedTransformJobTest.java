package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedTransformation;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.conveyal.datatools.TestUtils.createFeedVersionFromGtfsZip;
import static com.conveyal.datatools.manager.models.FeedSource.FeedRetrievalMethod.MANUALLY_UPLOADED;
import static com.conveyal.datatools.manager.models.FeedTransformation.defaultReplaceFileTransform;
import static org.junit.Assert.assertThat;

public class ArbitraryFeedTransformJobTest extends UnitTest {
    private static final Logger LOG = LoggerFactory.getLogger(ArbitraryFeedTransformJob.class);
    private static Auth0UserProfile user = Auth0UserProfile.createTestAdminUser();
    private static Project project;
    private static FeedSource bart;

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

        // Bart
        bart = new FeedSource("BART", project.id, MANUALLY_UPLOADED);
        Persistence.feedSources.create(bart);
    }

    @Test
    public void canReplaceGtfsPlusFile() throws IOException {
        String table = "stop_attributes";
        FeedVersion bartVersion = createFeedVersionFromGtfsZip(bart, "bart_old.zip");
        // Update feed source properties with transformations.
        bart.transformations = new ArrayList<>();
        bart.transformInPlace = true;
        // Replace file transformation runs before feed is loaded into database.
        // Note: stop_attributes.txt is a GTFS+ file found in BART's feed.
        FeedTransformation transformation = defaultReplaceFileTransform(bartVersion.id, table);
        bart.transformations.add(transformation);
        Persistence.feedSources.replace(bart.id, bart);
        // Create new BART version (note: bart_new.zip GTFS file has been stripped of stop_attributes.txt)
        FeedVersion newBartVersion = createFeedVersionFromGtfsZip(bart, "bart_new.zip");
        // Check that new version has stop_attributes file
        ZipFile zip = new ZipFile(newBartVersion.retrieveGtfsFile());
        ZipEntry entry = zip.getEntry(table + ".txt");
        assertThat(entry, Matchers.notNullValue());
        // TODO Verify that stop_attributes file matches source file exactly?
        // Clean up
        bartVersion.delete();
        newBartVersion.delete();
    }

    @Test
    public void replaceGtfsPlusFileFailsIfSourceIsMissing() {
        FeedVersion bartVersion = createFeedVersionFromGtfsZip(bart, "bart_new.zip");
        // Update feed source properties with transformations.
        bart.transformations = new ArrayList<>();
        bart.transformInPlace = true;
        // Replace file transformation runs before feed is loaded into database.
        // Note: stop_attributes.txt is a GTFS+ file found in BART's feed.
        FeedTransformation transformation = defaultReplaceFileTransform(bartVersion.id, "stop_attributes");
        bart.transformations.add(transformation);
        Persistence.feedSources.replace(bart.id, bart);
        // Create new BART version (note: bart_new.zip GTFS file has been stripped of stop_attributes.txt)
        FeedVersion newBartVersion = createFeedVersionFromGtfsZip(bart, "bart_old.zip");
        // TODO Check that new version has stop_attributes file that matches source version's copy.
        assertThat(newBartVersion.validationResult, Matchers.nullValue());
    }
}
