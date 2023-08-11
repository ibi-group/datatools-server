package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.manager.models.FeedRetrievalMethod;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.models.transform.FeedTransformRules;
import com.conveyal.datatools.manager.models.transform.FeedTransformZipTarget;
import com.conveyal.datatools.manager.models.transform.FeedTransformation;
import com.conveyal.datatools.manager.models.transform.NormalizeFieldTransformation;
import com.conveyal.datatools.manager.models.transform.NormalizeFieldTransformationTest;
import com.conveyal.datatools.manager.models.transform.Substitution;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.google.common.collect.Lists;
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
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.conveyal.datatools.TestUtils.createFeedVersion;
import static com.conveyal.datatools.TestUtils.zipFolderFiles;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


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
     * Run tear down after each test. This just deletes the feed versions that were used in the test.
     */
    @AfterEach
    public void tearDownTest() {
        // Clean up
        if (targetVersion != null) targetVersion.delete();
    }

    /**
     * Test that a {@link NormalizeFieldTransformation} will successfully complete.
     * FIXME: On certain Windows machines, this test fails.
     */
    @Test
    void canNormalizeField() throws IOException {
        TransformationCase route = new TransformationCase("routes", "route_long_name", "Route", "Rte");
        TransformationCase bookingRules = new TransformationCase("booking_rules", "pickup_message", "message", "msg");
        TransformationCase areas = new TransformationCase("areas", "area_name", "area", "location");
        // Create transformations.
        initializeFeedSource(List.of(
            createTransformation(bookingRules),
            createTransformation(route),
            createTransformation(areas)
        ));

        // Create target version that the transform will operate on.
        targetVersion = createFeedVersion(
            feedSource,
            zipFolderFiles("fake-agency-for-field-normalizing")
        );

            try (ZipFile zip = new ZipFile(targetVersion.retrieveGtfsFile())) {
            // Check that new version has expected modifications.
            checkTableForModification(zip, route);
            checkTableForModification(zip, bookingRules);
            checkTableForModification(zip, areas);
        }
    }

    private void checkTableForModification(ZipFile zip, TransformationCase transformationCase) throws IOException {
        // Check that the new version has been modified.
        ZipEntry entry = zip.getEntry(transformationCase.table + ".txt");
        assertNotNull(entry);

        // Scan the first data row and check that the substitution that was defined in the set-up was done.
        try (
            InputStream stream = zip.getInputStream(entry);
            InputStreamReader streamReader = new InputStreamReader(stream);
            BufferedReader reader = new BufferedReader(streamReader)
        ) {
            String[] columns = reader.readLine().split(",");
            int fieldIndex = ArrayUtils.indexOf(columns, transformationCase.fieldName);

            String row1 = reader.readLine();
            assertNotNull(row1, String.format("First row in table %s is null!", transformationCase.table));
            String[] row1Fields = row1.split(",");
            assertTrue(row1Fields[fieldIndex].startsWith(transformationCase.replacement), row1);
        }
    }

    /**
     * Test that a {@link NormalizeFieldTransformation} will fail if invalid substitution patterns are provided.
     */
    @Test
    void canHandleInvalidSubstitutionPatterns() throws IOException {
        // Create transform.
        // In this test, we provide an invalid pattern '\Cir\b' (instead of '\bCir\b'),
        // when trying to replace e.g. 'Piedmont Cir' with 'Piedmont Circle'.
        FeedTransformation<FeedTransformZipTarget> transformation = NormalizeFieldTransformationTest.createTransformation(
            TABLE_NAME, FIELD_NAME, null, Lists.newArrayList(
                new Substitution("\\Cir\\b", "Circle")
            )
        );
        initializeFeedSource(List.of(transformation));

        // Create target version that the transform will operate on.
        targetVersion = createFeedVersion(
            feedSource,
            zipFolderFiles("fake-agency-with-only-calendar")
        );

        // Errors in the substitution definitions should result in an error.
        // (There is no visibility to the underlying ZIP transform job.)
        assertTrue(targetVersion.hasCriticalErrors());
    }

    private FeedTransformation<FeedTransformZipTarget> createTransformation(TransformationCase transformationCase) {
        return NormalizeFieldTransformationTest
            .createTransformation(
                transformationCase.table,
                transformationCase.fieldName,
                null,
                Lists.newArrayList(
                    new Substitution(transformationCase.pattern, transformationCase.replacement)
                )
            );
    }

    /**
     * Create and persist a feed source using the given transformation.
     */
    private static void initializeFeedSource(List<FeedTransformation<FeedTransformZipTarget>> transformations) {

        // Create feed source with above transform.
        feedSource = new FeedSource("Normalize Field Test Feed", project.id, FeedRetrievalMethod.MANUALLY_UPLOADED);
        feedSource.deployable = false;
        for (FeedTransformation<FeedTransformZipTarget> transformation : transformations) {
            feedSource.transformRules.add(new FeedTransformRules(transformation));
        }
        Persistence.feedSources.create(feedSource);
    }

    // FIXME: Refactor (almost same code as AutoDeployJobTest in PR #361,
    //        and some common code with PersistenceTest#createProject).
    private static Project createProject() {
        Project project = new Project();
        project.name = String.format("Test Project %s", new Date());
        Persistence.projects.create(project);
        return project;
    }

    private static class TransformationCase {
        public String table;
        public String fieldName;
        public String pattern;
        public String replacement;

        public TransformationCase(String table, String fieldName, String pattern, String replacement) {
            this.table = table;
            this.fieldName = fieldName;
            this.pattern = pattern;
            this.replacement = replacement;
        }
    }
}
