package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.util.InvalidNamespaceException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.conveyal.datatools.TestUtils.assertThatSqlCountQueryYieldsExpectedCount;
import static com.conveyal.datatools.TestUtils.createFeedSource;
import static com.conveyal.datatools.TestUtils.createFeedVersionFromGtfsZip;
import static com.conveyal.datatools.TestUtils.createProject;
import static com.conveyal.datatools.manager.DataManager.GTFS_DATA_SOURCE;
import static com.conveyal.gtfs.util.Util.ensureValidNamespace;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class FeedSourceTest extends UnitTest {
    private static Project project;

    @BeforeAll
    static void setUp() throws Exception {
        DatatoolsTest.setUp();
        Auth0Connection.setAuthDisabled(true);
        project = createProject("Feed source deletion test project");
    }

    @AfterAll
    static void tearDown() {
        Auth0Connection.setAuthDisabled(Auth0Connection.getDefaultAuthDisabled());
        if (project != null) {
            project.delete();
        }
    }

    @ParameterizedTest(name = "delete feed source: missing version registry entries={0}, missing version schemas={1}")
    @CsvSource({"false, false", "true, false", "false, true", "true, true"})
    void shouldDeleteFeedSourceWithAllVersionsMissingPostgresData(
        boolean removeRegistryEntries,
        boolean dropSchemas
    ) throws SQLException, InvalidNamespaceException {
        FeedSource source = createFeedSource("Feed source with missing version data", project);
        List<FeedVersion> feedVersions = new ArrayList<>();
        try {
            feedVersions.add(createFeedVersionFromGtfsZip(source, "bart_old_lite.zip"));
            feedVersions.add(createFeedVersionFromGtfsZip(source, "bart_old_lite.zip"));
            assertEquals(feedVersions.size(), source.retrieveFeedVersions().size());

            for (FeedVersion version : feedVersions) {
                String namespace = version.namespace;
                ensureValidNamespace(namespace);
                String registryCountSql = String.format(
                    "SELECT count(*) FROM feeds WHERE namespace = '%s'",
                    namespace
                );
                String schemaCountSql = String.format(
                    "SELECT count(*) FROM information_schema.schemata WHERE schema_name = '%s'",
                    namespace
                );
                assertThatSqlCountQueryYieldsExpectedCount(registryCountSql, 1);
                assertThatSqlCountQueryYieldsExpectedCount(schemaCountSql, 1);

                // Remove only SQL data, leaving both versions for FeedSource.delete() to discover in MongoDB.
                try (
                    Connection connection = GTFS_DATA_SOURCE.getConnection();
                    Statement statement = connection.createStatement()
                ) {
                    if (removeRegistryEntries) {
                        assertEquals(1, statement.executeUpdate(
                            String.format("DELETE FROM feeds WHERE namespace = '%s'", namespace)
                        ));
                    }
                    if (dropSchemas) {
                        statement.executeUpdate(String.format("DROP SCHEMA %s CASCADE", namespace));
                    }
                    connection.commit();
                }
                assertThatSqlCountQueryYieldsExpectedCount(registryCountSql, removeRegistryEntries ? 0 : 1);
                assertThatSqlCountQueryYieldsExpectedCount(schemaCountSql, dropSchemas ? 0 : 1);
                assertNotNull(
                    Persistence.feedVersions.getById(version.id),
                    "Removing PostgreSQL data must leave the feed version in MongoDB."
                );
            }
            assertNotNull(Persistence.feedSources.getById(source.id));
            assertEquals(feedVersions.size(), source.retrieveFeedVersions().size());

            source.delete();

            assertAll(
                () -> assertNull(
                    Persistence.feedSources.getById(source.id),
                    "The feed source must be removed even when all its versions are missing PostgreSQL data."
                ),
                () -> assertEquals(
                    0, source.retrieveFeedVersions().size(),
                    "Deleting the source must not leave orphaned feed versions in MongoDB."
                )
            );
            for (FeedVersion version : feedVersions) {
                assertNull(Persistence.feedVersions.getById(version.id));
                assertThatSqlCountQueryYieldsExpectedCount(String.format(
                    "SELECT count(*) FROM information_schema.schemata WHERE schema_name = '%s'",
                    version.namespace
                ), 0);
            }
        } finally {
            if (Persistence.feedSources.getById(source.id) != null) {
                source.delete();
            }
        }
    }
}
