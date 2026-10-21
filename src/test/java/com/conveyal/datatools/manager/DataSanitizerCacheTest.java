package com.conveyal.datatools.manager;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataSanitizerCacheTest extends UnitTest {
    @TempDir
    Path directory;

    @BeforeAll
    static void setUp() throws Exception {
        DatatoolsTest.setUp();
    }

    @ParameterizedTest(name = "orphan cache cleanup with GTFS+ enabled={0}")
    @ValueSource(booleans = {true, false})
    void shouldPreserveExistingVersionsAndDeleteOnlyOrphanCaches(boolean enabled) throws Exception {
        String previousSetting = DataManager.getConfigPropertyAsText("modules.gtfsplus.enabled");
        FeedVersion existing = new FeedVersion();
        existing.id = UUID.randomUUID() + ".zip";
        Persistence.feedVersions.create(existing);
        try {
            DataManager.overrideConfigProperty("modules.gtfsplus.enabled", Boolean.toString(enabled));
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
            DataManager.overrideConfigProperty("modules.gtfsplus.enabled", previousSetting);
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
