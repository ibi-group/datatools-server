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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.not;

/**
 * Utility class dealing with GTFS tables.
 */
public class GtfsUtils {
    /** Maintain a list of scanned namespaces to avoid duplicate work. */
    private static Map<String, NamespaceInfo> scannedNamespaces = new HashMap<>();

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
        scannedNamespaces = new HashMap<>();

        try (Connection connection = DataManager.GTFS_DATA_SOURCE.getConnection()) {
            Persistence.projects.getAll().forEach(
                p -> {
                    System.out.println("Project " + p.name);
                    Persistence.feedSources.getFiltered(eq("projectId", p.id)).forEach(
                        fs -> {
                            if (scannedNamespaces.size() < 25) {
                                System.out.println("- FeedSource " + fs.name + " " + fs.id);
                                if (!Strings.isNullOrEmpty(fs.editorNamespace)) {
                                    checkTablesForNamespace(fs.editorNamespace, "editor", connection);
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
                                        checkTablesForNamespace(sn.namespace, "namespace", connection);
                                        if (!Strings.isNullOrEmpty(sn.snapshotOf) && !sn.snapshotOf.equals("mapdb_editor")) {
                                            checkTablesForNamespace(sn.snapshotOf, "snapshotOf", connection);
                                        }
                                    }
                                );
                            }

                        }
                    );
                }
            );

            // Once done, print the SQL statements to update the tables
            System.out.println("-- Overview of changes that should be made");
            scannedNamespaces.values().forEach(nsInfo -> {
                // Add missing tables
                nsInfo.missingTables.forEach(t -> {
                    System.out.println("CREATE TABLE IF NOT EXISTS " + nsInfo.namespace + "." + t.name + " ... (use table.createSqlTable(...))");
                });
                nsInfo.tableInfos.forEach(t -> {
                    // Add missing columns
                    String alterTableSql = "ALTER TABLE " + nsInfo.namespace + "." + t.table.name;
                    if (!t.missingColumns.isEmpty()) {
                        String alterSql = alterTableSql;
                        for (ColumnInfo c : t.missingColumns) {
                            alterSql += " ADD COLUMN IF NOT EXISTS " + c.columnName + " " + c.expectedType;
                        }
                        System.out.println(alterSql);
                    }

                    // Attempt to fix the columns with wrong types
                    if (!t.columnsWithWrongType.isEmpty()) {
                        String alterSql = alterTableSql;
                        for (ColumnInfo c : t.columnsWithWrongType) {
                            alterSql += " ALTER COLUMN " + c.columnName + " TYPE " + c.expectedType;
                        }
                        System.out.println(alterSql);
                    }
                });
            });
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
    }

    public static void checkTablesForNamespace(String namespace, String type, Connection connection) {
        if (scannedNamespaces.get(namespace) == null) {
            String qualifier = "";
            try {
                NamespaceInfo nsInfo = new NamespaceInfo(namespace, connection);
                if (nsInfo.isOrphan()) {
                    qualifier = "- orphan";
                } else if (nsInfo.tableNames.isEmpty()) {
                    qualifier = "- No tables";
                }
                System.out.println("      - " + type + ": " + namespace + " " + qualifier);

                if (!nsInfo.tableNames.isEmpty()) {
                    // Are tables missing?
                    nsInfo.missingTables.forEach(t -> System.out.println("          Missing table: " + t));
                    nsInfo.validTables.forEach(t -> checkTableColumns(nsInfo, connection, t));
                }
                scannedNamespaces.put(namespace, nsInfo);

            } catch (SQLException sqle) {
                // Do nothing
            }
        }
    }

    private static void checkTableColumns(NamespaceInfo nsInfo, Connection connection, Table table) {
        // TODO Refactor all prepared statements.
        PreparedStatement selectColumnStatement = null;
        try {
            selectColumnStatement = connection.prepareStatement(
                //"select column_name, data_type from information_schema.columns where table_schema = ? and table_name = ?"
                "select column_name, data_type from metacolumns where table_schema = ? and table_name = ?"
            );
            selectColumnStatement.setString(1, nsInfo.namespace);
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

            nsInfo.tableInfos.add(tableInfo);
        } catch (SQLException e) {
            e.printStackTrace();
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
        public final List<Table> missingTables = new ArrayList<>();
        public final List<Table> validTables = new ArrayList<>();
        public final List<TableInfo> tableInfos = new ArrayList<>();

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
                if (tableNames.contains(table.name)) {
                    validTables.add(table);
                } else {
                    missingTables.add(table);
                }
            }
        }

        public boolean isOrphan() {
            return tableNames.isEmpty() && Strings.isNullOrEmpty(loadedDate);
        }
    }

    public static class TableInfo {
        public final Table table;
        public final List<ColumnInfo> columns;
        public final List<ColumnInfo> missingColumns = new ArrayList<>();
        public final List<ColumnInfo> columnsWithWrongType = new ArrayList<>();

        public TableInfo(Table table, List<ColumnInfo> columns) {
            this.table = table;
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
                    ColumnInfo c = new ColumnInfo(field.name, field.getSqlTypeName());
                    c.expectedType = field.getSqlTypeName();
                    missingColumns.add(c);
                }
            }
        }

        public boolean hasColumnIssues() {
            return !missingColumns.isEmpty() || !columnsWithWrongType.isEmpty();
        }
    }
}
