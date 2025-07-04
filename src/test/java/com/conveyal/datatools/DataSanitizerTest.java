package com.conveyal.datatools;

import com.conveyal.datatools.common.utils.aws.CheckedAWSException;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.DataSanitizer;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.jobs.ProcessSingleFeedJob;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.GTFS;
import com.conveyal.gtfs.util.InvalidNamespaceException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import static com.conveyal.datatools.TestUtils.appendDate;
import static com.conveyal.datatools.TestUtils.createFeedVersion;
import static com.conveyal.datatools.TestUtils.zipFolderFiles;
import static com.conveyal.datatools.manager.models.FeedRetrievalMethod.MANUALLY_UPLOADED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataSanitizerTest {

    private static Project project;
    private static FeedVersion feedVersionOrphan;
    private static FeedVersion feedVersionWithParent;
    private static String orphanedDBSchema;

    @BeforeAll
    static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();
        Auth0Connection.setAuthDisabled(true);
        ProcessSingleFeedJob.VALIDATE_MOBILITY_DATA = false;
        project = new Project();
        project.name = appendDate("Test");
        Persistence.projects.create(project);
        FeedSource feedSource = new FeedSource(appendDate("Test Feed"), project.id, MANUALLY_UPLOADED);
        Persistence.feedSources.create(feedSource);
        FeedSource feedSourceParent = new FeedSource(appendDate("Test Feed 2"), project.id, MANUALLY_UPLOADED);
        Persistence.feedSources.create(feedSourceParent);
        feedVersionOrphan = createFeedVersion(
            feedSource,
            zipFolderFiles("fake-agency-with-only-calendar")
        );
        feedVersionWithParent = createFeedVersion(
            feedSourceParent,
            zipFolderFiles("fake-agency-with-only-calendar")
        );
        FeedVersion feedVersionWithOrphanDBSchema = createFeedVersion(
            feedSourceParent,
            zipFolderFiles("fake-agency-with-only-calendar")
        );
        orphanedDBSchema = feedVersionWithOrphanDBSchema.namespace;
        // Delete feed source to orphan feed version.
        Persistence.feedSources.removeById(feedSource.id);
        // Delete feed version to orphan DB schema.
        Persistence.feedVersions.removeById(feedVersionWithOrphanDBSchema.id);
    }

    @AfterAll
    static void tearDown() throws SQLException, InvalidNamespaceException, CheckedAWSException {
        Auth0Connection.setAuthDisabled(false);
        ProcessSingleFeedJob.VALIDATE_MOBILITY_DATA = true;
        project.delete();
        FeedVersion feedVersion = Persistence.feedVersions.getById(feedVersionOrphan.id);
        if (feedVersion != null) {
            feedVersionOrphan.deleteOrphan();
        }
        try {
            GTFS.delete(orphanedDBSchema, DataManager.GTFS_DATA_SOURCE);
        } catch (Exception e) {
          // Do nothing. Schema removed in unit test.
        }
    }

    @Test
    void canIdentifyOrphanedFeedVersion() {
        assertEquals(1, DataSanitizer.sanitizeFeedVersions(false));
        assertNotNull(Persistence.feedVersions.getById(feedVersionOrphan.id));
        assertNotNull(Persistence.feedVersions.getById(feedVersionWithParent.id));
    }

    @Test
    void canRemoveOrphanedFeedVersion() {
        assertEquals(1, DataSanitizer.sanitizeFeedVersions(true));
        assertNull(Persistence.feedVersions.getById(feedVersionOrphan.id));
        assertNotNull(Persistence.feedVersions.getById(feedVersionWithParent.id));
    }

    @Test
    void canIdentifyOrphanedDBSchemas() {
        // Other schemas will exist. For this test, just make sure the list contains the test orphaned schema.
        Set<String> orphanedSchemas = DataSanitizer.getOrphanedDBSchemas(new HashSet<>());
        assertTrue(orphanedSchemas.contains(orphanedDBSchema));
    }

    @Test
    void canRemoveOrphanedDBSchema() {
        assertEquals(1, DataSanitizer.deleteOrphanedDBSchemas(Set.of(orphanedDBSchema)));
    }
}