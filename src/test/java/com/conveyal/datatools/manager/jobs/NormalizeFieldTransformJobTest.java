package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.TestUtils;
import com.conveyal.datatools.manager.models.FeedRetrievalMethod;
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
import java.util.Date;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.conveyal.datatools.TestUtils.*;
import static org.junit.jupiter.api.Assertions.*;

public class NormalizeFieldTransformJobTest extends DatatoolsTest {
    private static final String TABLE_NAME = "routes";
    private static final String FIELD_NAME = "route_long_name";
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

        // Create a project.
        project = createProject();

        // Create transform.
        // In this test class, as an illustration, we replace "Route" with the "Rte" abbreviation in routes.txt.
        FeedTransformation transformation = NormalizeFieldTransformation.create(TABLE_NAME, FIELD_NAME, null, "Route => Rte");
        FeedTransformRules transformRules = new FeedTransformRules(transformation);

        // Create feed source with above transform.
        feedSource = new FeedSource("Normalize Field Test Feed", project.id, FeedRetrievalMethod.MANUALLY_UPLOADED);
        feedSource.deployable = false;
        feedSource.transformRules.add(transformRules);
        Persistence.feedSources.create(feedSource);
    }

    /**
     * Clean up test database after tests finish.
     */
    @AfterAll
    public static void tearDown() {
        // Project delete cascades to feed sources.
        project.delete();
        feedSource.delete();
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
    @Test
    public void canNormalizeField1() throws IOException {
        canNormalizeField();
    }

    /**
     * Second test used locally that checks that the correct ZIP file is picked up after being written.
     * (This sort-of replicates the CI system running two builds in succession.)
     */
    @Test
    public void canNormalizeField2() throws IOException {
        if (!TestUtils.isCi()) {
            canNormalizeField();
        }
    }

    // TODO: Refactor, this code is similar structure to test with replace file.
    public void canNormalizeField() throws IOException {
        // Create target version.
        targetVersion = createFeedVersion(
            feedSource,
            zipFolderFiles("fake-agency-with-only-calendar")
        );

        // Check that new version has routes table modified.
        ZipFile zip = new ZipFile(targetVersion.retrieveGtfsFile());
        ZipEntry entry = zip.getEntry(TABLE_NAME + ".txt");
        assertNotNull(entry);

        // Scan the first data row in routes.txt and check that the substitution was done.
        InputStream stream = zip.getInputStream(entry);
        InputStreamReader streamReader = new InputStreamReader(stream);
        BufferedReader reader = new BufferedReader(streamReader);
        try {
            String[] columns = reader.readLine().split(",");
            int fieldIndex = ArrayUtils.indexOf(columns, FIELD_NAME);

            String row1 = reader.readLine();
            String[] row1Fields = row1.split(",");
            assertTrue(row1Fields[fieldIndex].startsWith("Rte "), row1);
        } finally {
            reader.close();
            streamReader.close();
            stream.close();
            zip.close();
        }
    }

    // FIXME: Refactor (almost same code as AutoDeployJobTest)
    private static Project createProject() {
        Project project = new Project();
        project.name = String.format("Test Project %s", new Date().toString());
        Persistence.projects.create(project);
        return project;
    }
}
