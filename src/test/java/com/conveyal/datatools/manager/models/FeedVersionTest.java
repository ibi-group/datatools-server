package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.util.InvalidNamespaceException;
import com.conveyal.gtfs.validator.ValidationResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.Date;
import java.util.stream.Stream;

import static com.conveyal.datatools.TestUtils.assertThatSqlCountQueryYieldsExpectedCount;
import static com.conveyal.datatools.TestUtils.createFeedSource;
import static com.conveyal.datatools.TestUtils.createFeedVersionFromGtfsZip;
import static com.conveyal.datatools.TestUtils.createProject;
import static com.conveyal.datatools.manager.DataManager.GTFS_DATA_SOURCE;
import static com.conveyal.gtfs.util.Util.ensureValidNamespace;
import static com.mongodb.client.model.Filters.eq;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FeedVersionTest extends UnitTest {
    private static Project project;
    private static FeedSource feedSource;

    /** Initialize application for tests to run. */
    @BeforeAll
    static void setUp() throws Exception {
        // start server if it isn't already running
        DatatoolsTest.setUp();
        Auth0Connection.setAuthDisabled(true);

        project = createProject(String.format("Test project %s", new Date()));
        feedSource = createFeedSource("Test feed source", project);
    }

    @AfterAll
    static void tearDown() {
        Auth0Connection.setAuthDisabled(Auth0Connection.getDefaultAuthDisabled());
        if (project != null) {
            project.delete();
        }
    }

    /**
     * Future feed detection.
     */
    @ParameterizedTest
    @MethodSource("futureFeedCases")
    void canDetermineFutureFeed(int daysOffsetFromToday, boolean isFuture, String caseName) {
        FeedVersion feedVersion1 = new FeedVersion(feedSource);
        feedVersion1.validationResult = new ValidationResult();
        LocalDate date = LocalDate.now().plusDays(daysOffsetFromToday);
        feedVersion1.validationResult.firstCalendarDate = date;
        feedVersion1.validationResult.lastCalendarDate = date.plusDays(3);

        assertEquals(
            isFuture,
            feedVersion1.isFuture(),
            String.format("Feed starting '%s' is %s in the future.", caseName, isFuture ? "indeed" : "not")
        );
    }

    private static Stream<Arguments> futureFeedCases() {
        return Stream.of(
            Arguments.of(0, false, "today"),
            Arguments.of(-1, false, "yesterday"),
            Arguments.of(-2, false, "the day before yesterday"),
            Arguments.of(1, true, "tomorrow"),
            Arguments.of(2, true, "the day after tomorrow")
        );
    }

    /**
     * Future feed detection.
     */
    @Test
    void canDetermineFutureFeedNullFirstCalendarDate() {
        FeedVersion feedVersion1 = new FeedVersion(feedSource);
        feedVersion1.validationResult = new ValidationResult();
        assertFalse(feedVersion1.isFuture(), "null firstCalendarDate is not future.");
    }

    /**
     * Make sure FeedVersionIDs are always unique, even if created at the same second.
     * See https://github.com/ibi-group/datatools-server/issues/251
     */
    @Test
    void canCreateUniqueFeedVersionIDs() {
        // Create a project, feed sources, and feed versions to merge.
        // create two feedVersions immediately after each other which should end up having unique IDs
        FeedVersion feedVersion1 = new FeedVersion(feedSource);
        FeedVersion feedVersion2 = new FeedVersion(feedSource);
        assertThat(feedVersion1.id, not(equalTo(feedVersion2.id)));
    }

    /**
     * Detect feeds with fatal exceptions (a blocking issue for publishing).
     */
    @Test
    void canDetectBlockingFatalExceptionsForPublishing() {
        FeedVersion feedVersion1 = new FeedVersion(feedSource);
        feedVersion1.validationResult = new ValidationResult();
        feedVersion1.validationResult.fatalException = "A fatal exception occurred";

        assertThat(feedVersion1.hasBlockingIssuesForPublishing(), equalTo(true));
    }

    /**
     * Detect feeds with blocking error types that prevents publishing, per
     * https://github.com/ibi-group/datatools-ui/blob/dev/lib/manager/util/version.js#L79.
     */
    @ParameterizedTest
    @EnumSource(value = NewGTFSErrorType.class, names = { 
        "ILLEGAL_FIELD_VALUE",
        "MISSING_COLUMN",
        "REFERENTIAL_INTEGRITY",
        "SERVICE_WITHOUT_DAYS_OF_WEEK",
        "TABLE_MISSING_COLUMN_HEADERS",
        "TABLE_IN_SUBDIRECTORY",
        "WRONG_NUMBER_OF_FIELDS"
    })
    void canDetectBlockingErrorTypesForPublishing(NewGTFSErrorType errorType) throws InvalidNamespaceException, SQLException {
        FeedVersion feedVersion1 = createFeedVersionFromGtfsZip(feedSource, "bart_old_lite.zip");

        // Add blocking error types to feed version
        try (Connection connection = GTFS_DATA_SOURCE.getConnection()) {
            SQLErrorStorage errorStorage = new SQLErrorStorage(connection, feedVersion1.namespace + ".", false);
            errorStorage.storeError(NewGTFSError.forFeed(errorType, null));
            errorStorage.commitAndClose();
        }

        assertThat(feedVersion1.hasBlockingIssuesForPublishing(), equalTo(true));
    }

    /**
     * {@link FeedSource::publishedVersionId} should be unset if it references a feed version being deleted.
     */
    @Test
    void shouldDeletePublishedVersionIdWhenDeletingVersion() {
        final String NAMESPACE = "published_namespace";

        FeedSource storedFeedSource = Persistence.feedSources.getOneFiltered(eq("projectId", project.id));
        String feedSourceId = storedFeedSource.id;

        // Create a feed version linked to the feed source.
        FeedVersion feedVersion1 = new FeedVersion(feedSource);
        feedVersion1.namespace = NAMESPACE;
        feedVersion1.feedSourceId = feedSourceId;
        Persistence.feedVersions.create(feedVersion1);

        // Other feed version linked to the feed source, without effect to the published version.
        FeedVersion feedVersion2 = new FeedVersion(feedSource);
        feedVersion2.namespace = "other_namespace";
        feedVersion2.feedSourceId = feedSourceId;
        Persistence.feedVersions.create(feedVersion2);

        // Set publishedVersionId of FeedSource as the namespace of the feed version, per FeedVersionController logic.
        Persistence.feedSources.updateField(feedSourceId, "publishedVersionId", NAMESPACE);

        // publishedVersionId should be set at this point.
        assertThat(getPubVersionId(feedSourceId), equalTo(NAMESPACE));

        // Deleting feedVersion2 should not touch publishedVersionId.
        feedVersion2.delete();
        assertThat(getPubVersionId(feedSourceId), equalTo(NAMESPACE));

        // Deleting feedVersion1 should unset publishedVersionId.
        feedVersion1.delete();
        assertThat(getPubVersionId(feedSourceId), equalTo(null));

        assertThat(Persistence.feedVersions.getById(feedVersion1.id), nullValue());
        assertThat(Persistence.feedVersions.getById(feedVersion2.id), nullValue());
        // Delete the feed versions manually as a result.
        Persistence.feedVersions.removeById(feedVersion1.id);
        Persistence.feedVersions.removeById(feedVersion2.id);
    }

    @ParameterizedTest(name = "{0}: GTFS+ enabled={1}, cache exists={2}, companion exists={3}")
    @MethodSource("cacheDeletionCases")
    void shouldDeleteVersionResources(
        String deletionMode,
        boolean gtfsPlusEnabled,
        boolean cacheExists,
        boolean companionExists
    ) throws Exception {
        String previousSetting = DataManager.getConfigPropertyAsText("modules.gtfsplus.enabled");
        FeedSource source = createFeedSource("Cache cleanup test", project);
        FeedVersion version = new FeedVersion(source);
        Persistence.feedVersions.create(version);
        Path zip = FeedVersion.feedStore.getFeedFile(version.id).toPath();
        Path cacheDirectory = FeedStore.basePath.toPath().resolve(DataManager.GTFS_PLUS_SUBDIR);
        Path cache = cacheDirectory.resolve(version.id + ".db");
        Path companion = cacheDirectory.resolve(version.id + ".db.p");
        Path unrelated = cacheDirectory.resolve(version.id + ".unrelated");
        try {
            DataManager.overrideConfigProperty("modules.gtfsplus.enabled", Boolean.toString(gtfsPlusEnabled));
            FeedVersion.feedStore.newFeed(version.id, new ByteArrayInputStream(new byte[] {1}), null);
            Files.createDirectories(cacheDirectory);
            if (cacheExists) {
                Files.write(cache, new byte[] {1});
            }
            if (companionExists) {
                Files.write(companion, new byte[] {1});
            }
            Files.write(unrelated, new byte[] {1});
            assertTrue(Files.exists(zip));
            assertNotNull(Persistence.feedVersions.getById(version.id));
            assertEquals(cacheExists, Files.exists(cache));
            assertEquals(companionExists, Files.exists(companion));
            if ("orphan".equals(deletionMode)) {
                // Remove only the parent document to exercise the actual orphan-deletion path.
                Persistence.feedSources.removeById(source.id);
                version.deleteOrphan();
            } else if ("source".equals(deletionMode)) {
                source.delete();
            } else {
                version.delete();
            }

            assertAll(
                () -> assertNull(Persistence.feedVersions.getById(version.id)),
                () -> assertFalse(Files.exists(zip), "Original GTFS ZIP must be deleted."),
                () -> assertFalse(Files.exists(cache), "MapDB cache must be deleted."),
                () -> assertFalse(Files.exists(companion), "MapDB companion must be deleted."),
                () -> assertTrue(Files.exists(unrelated), "Unrelated files must be preserved.")
            );
            if ("source".equals(deletionMode)) {
                assertNull(Persistence.feedSources.getById(source.id));
            }
        } finally {
            DataManager.overrideConfigProperty("modules.gtfsplus.enabled", previousSetting);
            Files.deleteIfExists(cache);
            Files.deleteIfExists(companion);
            Files.deleteIfExists(unrelated);
            Files.deleteIfExists(zip);
            Persistence.feedVersions.removeById(version.id);
            if (Persistence.feedSources.getById(source.id) != null) {
                source.delete();
            }
        }
    }

    @ParameterizedTest(name = "delete feed version: missing registry entry={0}, missing schema={1}")
    @CsvSource({"false, false", "true, false", "false, true", "true, true"})
    void shouldDeleteFeedVersionWithMissingPostgresData(
        boolean removeRegistryEntry,
        boolean dropSchema
    ) throws SQLException, InvalidNamespaceException {

        FeedVersion feedVersion = createFeedVersionFromGtfsZip(feedSource, "bart_old_lite.zip");
        String namespace = feedVersion.namespace;
        ensureValidNamespace(namespace);
        String registryCountSql = String.format("SELECT count(*) FROM feeds WHERE namespace = '%s'", namespace);
        String schemaCountSql = String.format(
            "SELECT count(*) FROM information_schema.schemata WHERE schema_name = '%s'", namespace
        );

        try {
            assertNotNull(
                Persistence.feedVersions.getById(feedVersion.id),
                "The feed version must exist before deletion."
            );
            assertThatSqlCountQueryYieldsExpectedCount(registryCountSql, 1);
            assertThatSqlCountQueryYieldsExpectedCount(schemaCountSql, 1);

            try (
                Connection connection = GTFS_DATA_SOURCE.getConnection();
                Statement statement = connection.createStatement()
            ) {
                if (removeRegistryEntry) {
                    assertEquals(1, statement.executeUpdate(
                        String.format("DELETE FROM feeds WHERE namespace = '%s'", namespace)
                    ));
                }
                if (dropSchema) {
                    statement.executeUpdate(String.format("DROP SCHEMA %s CASCADE", namespace));
                }
                connection.commit();
            }

            assertThatSqlCountQueryYieldsExpectedCount(registryCountSql, removeRegistryEntry ? 0 : 1);
            assertThatSqlCountQueryYieldsExpectedCount(schemaCountSql, dropSchema ? 0 : 1);
            assertNotNull(
                Persistence.feedVersions.getById(feedVersion.id),
                "Removing SQL data must leave the feed version."
            );

            feedVersion.delete();

            assertNull(
                Persistence.feedVersions.getById(feedVersion.id),
                "Deleting a feed version must remove its MongoDB record even when PostgreSQL data is missing."
            );
            assertThatSqlCountQueryYieldsExpectedCount(schemaCountSql, 0);
        } finally {
            if (Persistence.feedVersions.getById(feedVersion.id) != null) {
                feedVersion.delete();
            }
        }
    }

    private static Stream<Arguments> cacheDeletionCases() {
        return Stream.of("version", "orphan", "source").flatMap(mode ->
            Stream.of(true, false).flatMap(enabled ->
                Stream.of(
                    Arguments.of(mode, enabled, false, false),
                    Arguments.of(mode, enabled, true, false),
                    Arguments.of(mode, enabled, false, true),
                    Arguments.of(mode, enabled, true, true)
                )
            )
        );
    }

    String getPubVersionId(String feedSourceId) {
        return Persistence.feedSources.getById(feedSourceId).publishedVersionId;
    }
}
