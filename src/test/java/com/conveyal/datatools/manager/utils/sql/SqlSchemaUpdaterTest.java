package com.conveyal.datatools.manager.utils.sql;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.storage.StorageException;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;

import static com.conveyal.datatools.TestUtils.appendDate;
import static com.conveyal.datatools.TestUtils.createFeedVersionFromGtfsZip;
import static com.conveyal.datatools.manager.models.FeedRetrievalMethod.MANUALLY_UPLOADED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class SqlSchemaUpdaterTest extends UnitTest {
    private static Project project;
    private static FeedSource feedSource;
    private static FeedVersion sourceVersion;

    /**
     * Initialize Data Tools and set up a simple feed source and project.
     */
    @BeforeAll
    public static void setUp() throws IOException {
        // start server if it isn't already running.
        DatatoolsTest.setUp();

        // Create a project and feed sources.
        project = new Project();
        project.name = appendDate("Test");
        Persistence.projects.create(project);
        feedSource = new FeedSource(appendDate("Test Feed"), project.id, MANUALLY_UPLOADED);
        Persistence.feedSources.create(feedSource);

        // Create source version (also creates the feeds table and adds an entry).
        sourceVersion = createFeedVersionFromGtfsZip(
            feedSource,
            "caltrain_gtfs_lite.zip"
        );
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
     * Test to check tables for missing tables/fields or incorrect types
     * and to upgrade the tables accordingly.
     */
    @ParameterizedTest
    @ValueSource(strings = {"namespace", "editor"})
    void canCheckAndUpgradeTables(String namespaceType) throws Exception {
        try (
            Connection connection = DataManager.GTFS_DATA_SOURCE.getConnection();
            SqlSchemaUpdater schemaUpdater = new SqlSchemaUpdater(connection)
        ) {
            String namespace = sourceVersion.namespace;
            NamespaceCheck namespaceCheck = schemaUpdater.checkTablesForNamespace(
                namespace,
                feedSource,
                namespaceType
            );

            // Some tables are missing from the feed and should be flagged.
            assertTrue(namespaceCheck.missingTables.containsAll(Lists.newArrayList(
                Table.SCHEDULE_EXCEPTIONS,
                Table.FEED_INFO,
                Table.TRANSFERS,
                Table.FREQUENCIES,
                Table.ATTRIBUTIONS,
                Table.TRANSLATIONS
            )));

            TableCheck checkToRemove = null;
            TableCheck checkToAdd = null;
            for (TableCheck tableCheck : namespaceCheck.checkedTables) {
                // The agency table is missing columns, so they should be flagged.
                if (tableCheck.table == Table.AGENCY) {
                    ArrayList<String> missingColumns = Lists.newArrayList(
                        "agency_lang",
                        "agency_phone",
                        "agency_branding_url",
                        "agency_fare_url",
                        "agency_email"
                    );

                    assertEquals(missingColumns.size(), tableCheck.missingColumns.size());
                    for (ColumnCheck c : tableCheck.missingColumns) {
                        assertTrue(missingColumns.contains(c.columnName));
                        assertEquals(
                            String.format(
                                "ADD COLUMN IF NOT EXISTS %s %s", c.columnName, c.getExpectedType()
                            ),
                            c.getAddColumnSql()
                        );
                    }
                }

                // Simulate a previously incorrect column type that needs to be updated.
                if (tableCheck.table == Table.CALENDAR) {
                    assertTrue(tableCheck.columnsWithWrongType.isEmpty());

                    try (Statement changeStatement = connection.createStatement()) {
                        String changeSql = String.format(
                            "ALTER TABLE %s.calendar ALTER COLUMN monday TYPE VARCHAR",
                            namespace
                        );
                        changeStatement.execute(changeSql);
                    }

                    // Check that the modified column is flagged.
                    TableCheck changedTableCheck = new TableCheck(tableCheck.table, namespace, namespaceCheck.type, schemaUpdater);
                    checkToRemove = tableCheck;
                    checkToAdd = changedTableCheck;

                    assertEquals(1, changedTableCheck.columnsWithWrongType.size());
                    ColumnCheck columnWithWrongType = changedTableCheck.columnsWithWrongType.get(0);
                    assertEquals("monday", columnWithWrongType.columnName);
                    assertEquals("varchar", columnWithWrongType.getDataType());
                    assertEquals("integer", columnWithWrongType.getExpectedType());
                    String alterColumnSql = "ALTER COLUMN monday TYPE integer USING monday::integer";
                    assertEquals(alterColumnSql, columnWithWrongType.getAlterColumnTypeSql());

                    if (namespaceType.equals("editor")) {
                        String addColumnSql = "ADD COLUMN IF NOT EXISTS description varchar";
                        String tableAddColumnSql = String.format("ALTER TABLE %s.calendar %s;", namespace, addColumnSql);
                        assertEquals(tableAddColumnSql, changedTableCheck.getAddColumnsSql());
                    }
                    String tableAlterColumnSql = String.format("ALTER TABLE %s.calendar %s;", namespace, alterColumnSql);
                    assertEquals(tableAlterColumnSql, changedTableCheck.getAlterColumnsSql());
                }
            }
            // Update namespaceCheck with the table check with the modified column above,
            // so that the table upgrade can pick it up.
            namespaceCheck.checkedTables.remove(checkToRemove);
            namespaceCheck.checkedTables.add(checkToAdd);

            // Go ahead and update the tables.
            schemaUpdater.upgradeNamespaceIfNotOrphanOrDeleted(namespaceCheck);

            // Perform a new check (delete previous ones).
            schemaUpdater.resetCheckedNamespaces();
            NamespaceCheck updatedNamespaceCheck = schemaUpdater.checkTablesForNamespace(
                namespace,
                feedSource,
                "namespace"
            );

            // Check that missing tables were added.
            assertTrue(updatedNamespaceCheck.missingTables.isEmpty());

            for (TableCheck tableCheck : updatedNamespaceCheck.checkedTables) {
                // The agency table is missing columns, so they should be flagged.
                if (tableCheck.table == Table.AGENCY) {
                    assertTrue(tableCheck.missingColumns.isEmpty());
                }

                // Simulate a previously incorrect column type that needs to be updated.
                if (tableCheck.table == Table.CALENDAR) {
                    assertTrue(tableCheck.columnsWithWrongType.isEmpty());
                }

                // Check for no missing fields or fields with wrong type.
                assertTrue(tableCheck.missingColumns.isEmpty());
                assertTrue(tableCheck.columnsWithWrongType.isEmpty());
            }
        }
    }

    /**
     * Orphan namespaces that are not in the database should not be upgraded.
     */
    @Test
    void shouldNotUpgradeOrphanNamespaces() throws Exception {
        try (
            Connection connection = DataManager.GTFS_DATA_SOURCE.getConnection();
            SqlSchemaUpdater schemaUpdater = new SqlSchemaUpdater(connection)
        ) {
            NamespaceCheck namespaceCheck = schemaUpdater.checkTablesForNamespace(
                "random_namespace",
                feedSource,
                "namespace"
            );

            // Go ahead and update the tables.
            try {
                schemaUpdater.upgradeNamespaceIfNotOrphanOrDeleted(namespaceCheck);
            } catch (StorageException e) {
                fail("Orphan namespaces should not be upgraded.");
            }
        }
    }
}
