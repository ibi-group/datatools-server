package com.conveyal.datatools.manager;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.jobs.ProcessSingleFeedJob;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.GTFS;
import com.mongodb.client.model.Sorts;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static com.conveyal.datatools.TestUtils.appendDate;
import static com.conveyal.datatools.TestUtils.createFeedVersion;
import static com.conveyal.datatools.TestUtils.zipFolderFiles;
import static com.conveyal.datatools.manager.models.FeedRetrievalMethod.MANUALLY_UPLOADED;
import static com.mongodb.client.model.Filters.eq;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataSanitizerTest {

    @TempDir
    Path directory;

    private static Project project;
    private static FeedVersion feedVersionOrphan;
    private static FeedVersion feedVersionWithParent;
    private static FeedSource feedSourceParent;
    private static FeedSource feedSourceWithObsoleteFeedVersion;
    private static String orphanedDBSchema;

    @BeforeAll
    static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();
        Auth0Connection.setAuthDisabled(true);
        ProcessSingleFeedJob.ENABLE_ADDITIONAL_VALIDATION = false;
        project = new Project();
        project.name = appendDate("Test");
        Persistence.projects.create(project);
        FeedSource feedSource = new FeedSource(appendDate("Test Feed"), project.id, MANUALLY_UPLOADED);
        Persistence.feedSources.create(feedSource);
        feedSourceParent = new FeedSource(appendDate("Test Feed 2"), project.id, MANUALLY_UPLOADED);
        feedSourceParent.editorNamespace = "active-editor-namespace";
        Persistence.feedSources.create(feedSourceParent);
        feedSourceWithObsoleteFeedVersion = new FeedSource(appendDate("Test Feed 3"), project.id, MANUALLY_UPLOADED);
        Persistence.feedSources.create(feedSourceWithObsoleteFeedVersion);
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

        for (int i=0; i<5; i++) {
            createFeedVersion(
                feedSourceWithObsoleteFeedVersion,
                zipFolderFiles("fake-agency-with-only-calendar")
            );
        }
    }

    @AfterAll
    static void tearDown() {
        Auth0Connection.setAuthDisabled(false);
        ProcessSingleFeedJob.ENABLE_ADDITIONAL_VALIDATION = true;
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
        assertEquals(1, DataSanitizer.sanitizeOrphanedFeedVersions(false));
        assertNotNull(Persistence.feedVersions.getById(feedVersionOrphan.id));
        assertNotNull(Persistence.feedVersions.getById(feedVersionWithParent.id));
    }

    @Test
    void canRemoveOrphanedFeedVersion() {
        assertEquals(1, DataSanitizer.sanitizeOrphanedFeedVersions(true));
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
    void canIdentifyActiveDBSchemas() {
        // Other schemas will exist. For this test, just make sure the list contains the feed source editor namespace.
        Set<String> activeDBSchemas = DataSanitizer.getActiveDBSchemas();
        assertTrue(activeDBSchemas.contains(feedSourceParent.editorNamespace));
    }

    @Test
    void canRemoveOrphanedDBSchema() {
        assertEquals(1, DataSanitizer.deleteOrphanedDBSchemas(Set.of(orphanedDBSchema)));
    }

    @Test
    void canAuditFeedVersions() {
        List<DataSanitizer.FeedVersionAudit> result = DataSanitizer.feedVersionAudit();
        assertTrue(result.stream().anyMatch(audit -> audit.projectName.equals(project.name)));
        assertTrue(result.stream().anyMatch(audit -> audit.feedSourceName.equals(feedSourceParent.name)));
        assertTrue(result.stream().anyMatch(audit -> audit.feedSourceName.equals(feedSourceWithObsoleteFeedVersion.name)));
    }

    @Test
    void canDeleteObsoleteFeedVersions() {
        // From the initial four feed versions, keep the two most recent.
        int deleted = DataSanitizer.deleteObsoleteFeedVersions(
            feedSourceWithObsoleteFeedVersion.id,
            2,
            false
        );
        assertEquals(3, deleted);
        Collection<FeedVersion> feedVersions = Persistence.feedVersions.getFiltered(
            eq("feedSourceId", feedSourceWithObsoleteFeedVersion.id),
            Sorts.descending("version")
        );
        // Feed versions are renumbered after being deleted. Should no longer have versions 3, 4 and 5.
        for (FeedVersion feedVersion : feedVersions) {
            assertTrue(feedVersion.version <= 2);
        }
    }

    @Test
    void shouldPreserveExistingVersionsAndDeleteOnlyOrphanCaches() throws Exception {
        FeedVersion existing = new FeedVersion();
        existing.id = UUID.randomUUID() + ".zip";
        Persistence.feedVersions.create(existing);
        try {
            Path activeCache = Files.createFile(directory.resolve(existing.id + ".db"));
            Path activeCompanion = Files.createFile(directory.resolve(existing.id + ".db.p"));
            String orphanId = UUID.randomUUID() + ".zip";
            Path orphanCache = Files.createFile(directory.resolve(orphanId + ".db"));
            Path orphanCompanion = Files.createFile(directory.resolve(orphanId + ".db.p"));
            Path loneCompanion = Files.createFile(directory.resolve(UUID.randomUUID() + ".zip.db.p"));
            Path loneCache = Files.createFile(directory.resolve(UUID.randomUUID() + ".zip.db"));
            Path originalZip = Files.createFile(directory.resolve(orphanId));
            Path unrelated = Files.createFile(directory.resolve("unrelated.db"));
            Path nested = Files.createDirectories(directory.resolve("nested"));
            Path nestedCache = Files.createFile(nested.resolve("nested.zip.db"));
            Path namedDirectory = Files.createDirectory(directory.resolve("directory.zip.db"));

            assertEquals(4, DataSanitizer.sanitizeOrphanedCaches(directory, false));
            assertTrue(Files.exists(orphanCache), "Dry run must preserve the cache.");
            assertTrue(Files.exists(orphanCompanion));
            assertTrue(Files.exists(loneCompanion));
            assertTrue(Files.exists(loneCache));

            assertEquals(4, DataSanitizer.sanitizeOrphanedCaches(directory, true));
            assertFalse(Files.exists(orphanCache));
            assertFalse(Files.exists(orphanCompanion));
            assertFalse(Files.exists(loneCompanion));
            assertFalse(Files.exists(loneCache));
            assertTrue(Files.exists(activeCache));
            assertTrue(Files.exists(activeCompanion));
            assertTrue(Files.exists(originalZip));
            assertTrue(Files.exists(unrelated));
            assertTrue(Files.exists(nestedCache));
            assertTrue(Files.isDirectory(namedDirectory));
            assertNotNull(Persistence.feedVersions.getById(existing.id));
            assertEquals(0, DataSanitizer.sanitizeOrphanedCaches(directory, true));
        } finally {
            Persistence.feedVersions.removeById(existing.id);
        }
    }

    @Test
    void shouldHandleMissingDirectory() throws Exception {
        Path missing = directory.resolve("missing");
        assertEquals(0, DataSanitizer.sanitizeOrphanedCaches(missing, false));
        assertEquals(0, DataSanitizer.sanitizeOrphanedCaches(missing, true));
        assertFalse(Files.exists(missing));
    }
}