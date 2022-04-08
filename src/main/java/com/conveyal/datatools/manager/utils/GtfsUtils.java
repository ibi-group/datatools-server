package com.conveyal.datatools.manager.utils;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.gtfsplus.tables.GtfsPlusTable;
import com.conveyal.datatools.manager.models.Snapshot;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.Table;
import com.google.common.base.Strings;
import org.bson.conversions.Bson;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.not;

/**
 * Utility class dealing with GTFS tables.
 */
public class GtfsUtils {
    /**
     * Obtains a GTFS table.
     * @param tableName The name of the table to obtain (e.g. "agency.txt").
     * @return
     */
    public static Table getGtfsTable(String tableName) {
        final List<Table> tables = Arrays.stream(Table.tablesInOrder)
            .filter(Table::isSpecTable)
            .collect(Collectors.toList());
        if (DataManager.isModuleEnabled("gtfsplus")) {
            // Add GTFS+ tables only if MTC extension is enabled.
            tables.addAll(Arrays.asList(GtfsPlusTable.tables));
        }
        // Check that table name is valid (to prevent SQL injection).
        for (Table specTable : tables) {
            if (specTable.name.equals(tableName)) {
                return specTable;
            }
        }

        return null;
    }

    public static void checkReferencedNamespaces() {
        try (Connection connection = DataManager.GTFS_DATA_SOURCE.getConnection()) {
            Persistence.projects.getAll().forEach(
                p -> {
                    System.out.println("Project " + p.name);
                    Persistence.feedSources.getFiltered(eq("projectId", p.id)).forEach(
                        fs -> {
                            System.out.println("- FeedSource " + fs.name + " " + fs.id);
                            if (!Strings.isNullOrEmpty(fs.editorNamespace)) {
                                System.out.println("  - editor: " + fs.editorNamespace);
                                checkTablesForNamespace(fs.editorNamespace, connection);
                            }

                            Bson feedSourceIdFilter = eq("feedSourceId", fs.id);
                            Bson feedSourceIdNamespaceFilter = and(
                                eq("feedSourceId", fs.id),
                                not(eq("namespace", null))
                            );
    /*
                            List<FeedVersion> allFeedVersions = Persistence.feedVersions.getFiltered(feedSourceIdFilter);
                            List<FeedVersion> feedVersions = Persistence.feedVersions.getFiltered(feedSourceIdNamespaceFilter);
                            System.out.println("  - FeedVersions (" + feedVersions.size() + "/" + allFeedVersions.size() + " with valid namespace)");
                            feedVersions.forEach(
                                fv -> {
                                    System.out.print("    - v" + fv.version + ": " + fv.namespace);
                                    checkTablesForNamespace(fv.namespace);
                                }
                            );
    */
                            List<Snapshot> allSnapshots = Persistence.snapshots.getFiltered(feedSourceIdFilter);
                            List<Snapshot> snapshots = Persistence.snapshots.getFiltered(feedSourceIdNamespaceFilter);
                            System.out.println("  - Snapshots (" + snapshots.size() + "/" + allSnapshots.size() + " with valid namespace)");
                            snapshots.forEach(
                                sn -> {
                                    System.out.println("    - " + sn.name + " " + sn.id);
                                    System.out.println("      - namespace: " + sn.namespace);
                                    checkTablesForNamespace(sn.namespace, connection);
                                    if (!Strings.isNullOrEmpty(sn.snapshotOf) && !sn.snapshotOf.equals("mapdb_editor")) {
                                        System.out.println("      - snapshotOf: " + sn.snapshotOf);
                                        checkTablesForNamespace(sn.snapshotOf, connection);
                                    }
                                }
                            );

                        }
                    );
                }
            );
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
    }

    public static void checkTablesForNamespace(String namespace, Connection connection) {
        try {
            NamespaceInfo nsInfo = new NamespaceInfo(namespace, connection);

            if (nsInfo.tableNames.isEmpty()) {
                if (Strings.isNullOrEmpty(nsInfo.loadedDate)) {
                    System.out.println(" - orphan");
                } else {
                    // Are there tables?
                    System.out.println("\n          No tables found.");
                }
            } else {
                // Are tables missing?
                nsInfo.missingTables.forEach(t -> System.out.println("          Missing table: " + t));

                for (Table table : Table.tablesInOrder) {
                    if (nsInfo.tableNames.contains(table.name)) {
                        // Are fields missing?
                        checkTableColumns(namespace, connection, table);
                    }
                }
            }

        } catch (SQLException sqle) {
            // Do nothing
        }



        // If a table is missing, create it?

        // For tables that were there already, check fields

    }

    private static void checkTableColumns(String namespace, Connection connection, Table table) throws SQLException {
        // TODO Refactor all prepared statements.
        PreparedStatement selectColumnStatement = connection.prepareStatement(
            //"select column_name, data_type from information_schema.columns where table_schema = ? and table_name = ?"
            "select column_name, data_type from metacolumns where table_schema = ? and table_name = ?"
        );
        selectColumnStatement.setString(1, namespace);
        selectColumnStatement.setString(2, table.name);

        ResultSet resultSet = selectColumnStatement.executeQuery();
        List<ColumnInfo> columns = new ArrayList<>();
        while (resultSet.next()) {
            columns.add(new ColumnInfo(
                resultSet.getString(1),
                resultSet.getString(2)
            ));
        }

        TableInfo tableInfo = new TableInfo(table, columns);
        if (tableInfo.hasColumnIssues()) {
            System.out.println("          Issues in table: " + table.name);
            tableInfo.missingColumns.forEach(c -> System.out.println("            Missing column: " + c));
            tableInfo.columnsWithWrongType.forEach(
                c -> System.out.println("            Incorrect type for column: " + c.columnName + " expected: " + c.expectedType + " actual: " + c.dataType)
            );
        }
    }

    public static class ColumnInfo {
        public String columnName;
        public String dataType;
        public String expectedType;

        public ColumnInfo(String columnName, String dataType) {
            this.columnName = columnName;
            this.dataType = dataType;

            // Normalize data types to the ones used by the GTFS-lib tables.
            if (dataType.equals("character varying")) {
                this.dataType = "varchar";
            }
            if (dataType.equals("ARRAY")) {
                this.dataType = "text[]";
            }
        }
    }

    public static class NamespaceInfo {
        public final String namespace;
        private String loadedDate = "";
        public final List<String> tableNames = new ArrayList<>();
        public final List<String> missingTables = new ArrayList<>();
        public final List<String> validTables = new ArrayList<>();

        /** Used for tests only */
        public NamespaceInfo(String namespace, List<String> excludedTables) {
            this.namespace = namespace;
            for (Table table : Table.tablesInOrder) {
                this.tableNames.add(table.name);
            }
            this.tableNames.removeAll(excludedTables);
            sortTables();
        }

        public NamespaceInfo(String namespace, Connection connection) throws SQLException {
            this.namespace = namespace;

            // Check that all tables for the namespace are present.
            PreparedStatement selectNamespaceTablesStatement = connection.prepareStatement(
                //"select table_name from information_schema.tables where table_schema = ?"
                "select table_name from metatables where table_schema = ?"
            );
            selectNamespaceTablesStatement.setString(1, namespace);
            PreparedStatement selectLoadedDateStatement = connection.prepareStatement(
                "select loaded_date from feeds where namespace = ?"
            );
            selectLoadedDateStatement.setString(1, namespace);

            ResultSet loadedDateSet = selectLoadedDateStatement.executeQuery();
            while (loadedDateSet.next()) {
                this.loadedDate = loadedDateSet.getString(1);
                break;
            }

            ResultSet resultSet = selectNamespaceTablesStatement.executeQuery();
            while (resultSet.next()) {
                tableNames.add(resultSet.getString(1));
            }

            sortTables();
        }

        private void sortTables() {
            for (Table table : Table.tablesInOrder) {
                String tableName = table.name;
                if (tableNames.contains(tableName)) {
                    validTables.add(tableName);
                } else {
                    missingTables.add(tableName);
                }
            }
        }
    }

    public static class TableInfo {
        public final List<ColumnInfo> columns;
        public final List<String> missingColumns = new ArrayList<>();
        public final List<ColumnInfo> columnsWithWrongType = new ArrayList<>();

        public TableInfo(Table table, List<ColumnInfo> columns) {
            this.columns = columns;

            for (Field field : table.fields) {
                Optional<ColumnInfo> foundColumnForField = columns
                    .stream()
                    .filter(c -> c.columnName.equals(field.name))
                    .findFirst();

                if (foundColumnForField.isPresent()) {
                    ColumnInfo columnForField = foundColumnForField.get();
                    // Are fields that are present of the correct type?
                    if (!field.getSqlTypeName().equals(columnForField.dataType)) {
                        columnForField.expectedType = field.getSqlTypeName();
                        columnsWithWrongType.add(columnForField);
                    }
                    // Only the id column seems to be marked as not nullable, so we won't check that for now.
                } else {
                    missingColumns.add(field.name);
                }
            }
        }

        public boolean hasColumnIssues() {
            return !missingColumns.isEmpty() || !columnsWithWrongType.isEmpty();
        }
    }
}
