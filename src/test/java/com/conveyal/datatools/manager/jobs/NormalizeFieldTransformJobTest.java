package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.models.transform.FeedTransformRules;
import com.conveyal.datatools.manager.models.transform.FeedTransformation;
import com.conveyal.datatools.manager.models.transform.NormalizeFieldTransformation;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.conveyal.datatools.TestUtils.*;
import static com.conveyal.datatools.manager.models.FeedRetrievalMethod.MANUALLY_UPLOADED;
import static org.junit.jupiter.api.Assertions.*;

// TODO: Refactor, the structure of this class is very similar to ArbitraryTransformJobTest
public class NormalizeFieldTransformJobTest extends UnitTest {
    private static Project project;
    private static FeedSource feedSource;
    private FeedVersion targetVersion;

    /**
     * Initialize Data Tools and set up a simple feed source and project.
     */
    @BeforeAll
    public static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();

        // Create a project, feed sources, and feed versions to merge.
        project = new Project();
        project.name = appendDate("Test");
        Persistence.projects.create(project);

        // Feed source.
        feedSource = new FeedSource(appendDate("Normalize Field Test Feed"), project.id, MANUALLY_UPLOADED);
        Persistence.feedSources.create(feedSource);
    }

    /**
     * Clean up test database after tests finish.
     */
    @AfterAll
    public static void tearDown() {
        // Project delete cascades to feed sources.
        project.delete();
    }

    /**
     * Run set up before each test. This just resets the feed source transformation properties.
     */
    //@BeforeEach
    public void setUpTest() {
        feedSource = Persistence.feedSources.getById(feedSource.id);
        feedSource.transformRules = new ArrayList<>();
        Persistence.feedSources.replace(feedSource.id, feedSource);
    }

    /**
     * Run tear down after each test. This just deletes the feed versions that were used in the test.
     */
    @AfterEach
    public void tearDownTest() {
        // Clean up
        if (targetVersion != null) targetVersion.delete();
    }

    /**
     * Test that a {@link NormalizeFieldTransformation} will successfully complete.
     */
    // TODO: Refactor, this code is similar structure to test with replace file.
    @Test
    public void canNormalizeField() throws IOException {
        final String table = "routes";
        final String fieldName = "route_long_name";

        // Perform transformation before feed is loaded into database.
        // In this test, we replace "Route" with the "Rte" abbreviation in routes.txt.
        FeedTransformation transformation = NormalizeFieldTransformation.create(table, fieldName, null, "Route => Rte");
        FeedTransformRules transformRules = new FeedTransformRules(transformation);
        feedSource.transformRules = new ArrayList<>();
        feedSource.transformRules.add(transformRules);
        Persistence.feedSources.replace(feedSource.id, feedSource);
        // Create target version.
        targetVersion = createFeedVersion(
            feedSource,
            zipFolderFiles("fake-agency-with-only-calendar")
        );

        // Check that new version has routes table modified.
        ZipFile zip = new ZipFile(targetVersion.retrieveGtfsFile());
        ZipEntry entry = zip.getEntry(table + ".txt");
        assertNotNull(entry);

        // Scan the first data row in routes.txt and check that the substitution was done.
        InputStream stream = zip.getInputStream(entry);
        InputStreamReader streamReader = new InputStreamReader(stream);
        BufferedReader reader = new BufferedReader(streamReader);
        try {
            String[] columns = reader.readLine().split(",");
            int fieldIndex = ArrayUtils.indexOf(columns, fieldName);

            String row1 = reader.readLine();
            String[] row1Fields = row1.split(",");
            assertTrue(row1Fields[fieldIndex].startsWith("Rte "), row1);
        } finally {
            reader.close();
            streamReader.close();
            stream.close();
        }
    }
}
