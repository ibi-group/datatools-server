package com.conveyal.datatools.manager;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.controllers.api.UserController;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.GtfsUtils;
import com.conveyal.gtfs.loader.Table;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

import static com.conveyal.datatools.TestUtils.appendDate;
import static com.conveyal.datatools.TestUtils.createFeedVersionFromGtfsZip;
import static com.conveyal.datatools.manager.auth.Auth0Users.USERS_API_PATH;
import static com.conveyal.datatools.manager.controllers.api.UserController.TEST_AUTH0_DOMAIN;
import static com.conveyal.datatools.manager.models.FeedRetrievalMethod.MANUALLY_UPLOADED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TableUpdaterTestE2E extends UnitTest {
    private static Project project;
    private static FeedSource feedSource;

    /**
     * Initialize Data Tools and set up a simple feed source and project.
     */
    @BeforeAll
    public static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();
        // No idea why notifications are sent after this test, so
        // at least prevent notifications from being sent.
        UserController.setBaseUsersUrl("http://" + TEST_AUTH0_DOMAIN + USERS_API_PATH);

        // Create a project, feed sources, and feed versions to merge.
        project = new Project();
        project.name = appendDate("Test");
        Persistence.projects.create(project);

        // Bart
        feedSource = new FeedSource(appendDate("Test Feed"), project.id, MANUALLY_UPLOADED);
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
     * Test to check tables for missing tables/fields or incorrect types
     * and to upgrade the tables accordingly.
     */
    @Test
    void canCheckAndUpgradeTables() throws SQLException {
        // Create source version (folder contains stop_attributes file).
        FeedVersion sourceVersion = createFeedVersionFromGtfsZip(
            feedSource,
            "caltrain_gtfs_lite.zip"
        );

        try (Connection connection = DataManager.GTFS_DATA_SOURCE.getConnection()) {
            String namespace = sourceVersion.namespace;
            GtfsUtils.NamespaceInfo nsInfo = GtfsUtils.checkTablesForNamespace(
                namespace,
                "test version",
                "namespace",
                connection
            );

            // Some tables are missing from the feed and should be flagged.
            assertTrue(nsInfo.missingTables.containsAll(Lists.newArrayList(
                Table.SCHEDULE_EXCEPTIONS,
                Table.FEED_INFO,
                Table.TRANSFERS,
                Table.FREQUENCIES,
                Table.ATTRIBUTIONS,
                Table.TRANSLATIONS
            )));
            for (Table t : nsInfo.validTables) {
                GtfsUtils.TableInfo tableInfo = GtfsUtils.checkTableColumns(nsInfo, connection, t);
                assertNotNull(tableInfo);

                // The agency table is missing columns, so they should be flagged.
                if (t == Table.AGENCY) {
                    ArrayList<String> missingColumns = Lists.newArrayList(
                        "agency_lang",
                        "agency_phone",
                        "agency_branding_url",
                        "agency_fare_url",
                        "agency_email"
                    );

                    assertEquals(missingColumns.size(), tableInfo.missingColumns.size());
                    for (GtfsUtils.ColumnInfo c : tableInfo.missingColumns) {
                        assertTrue(missingColumns.contains(c.columnName));
                        assertEquals(
                            String.format(
                                "ADD COLUMN IF NOT EXISTS %s %s", c.columnName, c.expectedType
                            ),
                            c.getAddColumnSql()
                        );
                    }
                }

                // Simulate a previously incorrect column type that needs to be updated.
                if (t == Table.CALENDAR) {
                    assertTrue(tableInfo.columnsWithWrongType.isEmpty());

                    PreparedStatement changeStatement = connection.prepareStatement(
                        String.format(
                            "ALTER TABLE %s.calendar ALTER COLUMN monday TYPE VARCHAR",
                            namespace
                        )
                    );
                    changeStatement.execute();

                    // Check that the modified column is flagged.
                    GtfsUtils.TableInfo changedTableInfo = GtfsUtils.checkTableColumns(nsInfo, connection, t);
                    assertNotNull(changedTableInfo);

                    assertEquals(1, changedTableInfo.columnsWithWrongType.size());
                    GtfsUtils.ColumnInfo columnWithWrongType = changedTableInfo.columnsWithWrongType.get(0);
                    assertEquals("monday", columnWithWrongType.columnName);
                    assertEquals("varchar", columnWithWrongType.dataType);
                    assertEquals("integer", columnWithWrongType.expectedType);
                    assertEquals("ALTER COLUMN monday TYPE integer USING monday::integer", columnWithWrongType.getAlterColumnTypeSql());
                }
            }

            // Go ahead and update the tables.
            GtfsUtils.upgradeNamespace(nsInfo, connection);

            GtfsUtils.resetScannedNamespaces();
            GtfsUtils.NamespaceInfo updatedNsInfo = GtfsUtils.checkTablesForNamespace(
                namespace,
                "test version",
                "namespace",
                connection
            );

            // Check that missing fields were added
            for (Table t : updatedNsInfo.validTables) {
                GtfsUtils.TableInfo tableInfo = GtfsUtils.checkTableColumns(nsInfo, connection, t);
                assertNotNull(tableInfo);

                // The agency table is missing columns, so they should be flagged.
                if (t == Table.AGENCY) {
                    assertTrue(tableInfo.missingColumns.isEmpty());
                }

                // Simulate a previously incorrect column type that needs to be updated.
                if (t == Table.CALENDAR) {
                    assertTrue(tableInfo.columnsWithWrongType.isEmpty());
                }
            }


            // Check that missing tables were added.
            assertTrue(updatedNsInfo.missingTables.isEmpty());
            for (Table t : updatedNsInfo.validTables) {
                GtfsUtils.TableInfo tableInfo = GtfsUtils.checkTableColumns(updatedNsInfo, connection, t);
                assertNotNull(tableInfo);

                assertTrue(tableInfo.missingColumns.isEmpty());
                assertTrue(tableInfo.columnsWithWrongType.isEmpty());
            }
        }
    }
}
