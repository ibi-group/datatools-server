package com.conveyal.datatools.manager.utils;

import com.conveyal.datatools.manager.models.FeedVersion;
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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.not;

/**
 * This class handles operations to detect whether postgresql tables
 * are consistent with the gtfs-lib implementation from the project dependencies,
 * and fills out missing tables/columns and changes column types if needed.
 */
public class SqlSchemaUpdater {
    /** Maintain a list of scanned namespaces to avoid duplicate work. */
    private Map<String, NamespaceInfo> scannedNamespaces = new HashMap<>();

    private final Connection connection;
    private final PreparedStatement selectNamespaceTablesStatement;
    private final PreparedStatement selectLoadedDateStatement;
    private final PreparedStatement selectColumnStatement;
    private final PreparedStatement selectColumnStatement1;

    public SqlSchemaUpdater(Connection connection) throws SQLException {
        this.connection = connection;
        selectColumnStatement = connection.prepareStatement(
            "select column_name, data_type from information_schema.columns where table_schema = ? and table_name = ?"
        );

        // Check that all tables for the namespace are present.
        selectNamespaceTablesStatement = connection.prepareStatement(
            "select table_name from information_schema.tables where table_schema = ?"
        );

        selectLoadedDateStatement = connection.prepareStatement(
            "select loaded_date from feeds where namespace = ?"
        );

        selectColumnStatement1 = connection.prepareStatement(
            "select column_name, data_type from information_schema.columns where table_schema = ? and table_name = ?"
        );
    }


    /**
     * Checks all namespaces that are referenced from projects
     * for tables and columns that need to be added or changed.
     */
    public void checkReferencedNamespaces() {
        resetScannedNamespaces();

        Persistence.projects.getAll().forEach(p -> {
            System.out.println("Project " + p.name);
            Persistence.feedSources.getFiltered(eq("projectId", p.id)).forEach(fs -> {
                if (scannedNamespaces.size() < 25) {
                    System.out.println("- FeedSource " + fs.name + " " + fs.id);
                    if (!Strings.isNullOrEmpty(fs.editorNamespace)) {
                        checkTablesForNamespace(fs.editorNamespace, p.name + "/" + fs.name + "/editor", "editor");
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
                            checkTablesForNamespace(fv.namespace, fv.name + "/v" + fv.version, "namespace");
                        }
                    );
*/
                    List<Snapshot> allSnapshots = Persistence.snapshots.getFiltered(feedSourceIdFilter);
                    List<Snapshot> snapshots = Persistence.snapshots.getFiltered(feedSourceIdNamespaceFilter);
                    System.out.println("  - Snapshots (" + snapshots.size() + "/" + allSnapshots.size() + " with valid namespace)");
                    snapshots.forEach(
                        sn -> {
                            System.out.println("    - " + sn.name + " " + sn.id);
                            checkTablesForNamespace(sn.namespace, p.name + "/snapshot " + sn.name,  "namespace");
                            if (!Strings.isNullOrEmpty(sn.snapshotOf) && !sn.snapshotOf.equals("mapdb_editor")) {
                                checkTablesForNamespace(sn.snapshotOf, p.name + "/snapshotOf " + sn.name,  "snapshotOf");
                            }
                        }
                    );}
                }
            );}
        );

        // Once done, print the SQL statements to update the tables
        System.out.println("-- Overview of changes that should be made");
        scannedNamespaces.values().forEach(nsInfo -> {
            System.out.println("-- " + nsInfo.nickname);
            // Add missing tables
            nsInfo.missingTables.forEach(t -> {
                System.out.println("CREATE TABLE IF NOT EXISTS " + nsInfo.namespace + "." + t.name + " ... (use table.createSqlTable(...))");
            });
            nsInfo.scannedTables.forEach(t -> {
                // Add missing columns
                if (!t.missingColumns.isEmpty()) {
                    System.out.printf("ALTER TABLE %s.%s %s;\n",
                        nsInfo.namespace,
                        t.table.name,
                        t.missingColumns.stream().map(ColumnInfo::getAddColumnSql).collect(Collectors.joining(", "))
                    );
                }

                // Attempt to fix the columns with wrong types
                if (!t.columnsWithWrongType.isEmpty()) {
                    System.out.printf("ALTER TABLE %s.%s %s;\n",
                        nsInfo.namespace,
                        t.table.name,
                        t.columnsWithWrongType.stream().map(ColumnInfo::getAlterColumnTypeSql).collect(Collectors.joining(", "))
                    );
                }
            });
        });
    }

    /**
     * Clears the list of scanned namespaces (used for tests).
     */
    public void resetScannedNamespaces() {
        scannedNamespaces = new HashMap<>();
    }

    public NamespaceInfo checkTablesForNamespace(String namespace, String nickname, String type) {
        NamespaceInfo existingNsInfo = scannedNamespaces.get(namespace);
        if (existingNsInfo == null) {
            String qualifier = "";
            try {
                NamespaceInfo nsInfo = new NamespaceInfo(namespace, nickname, this);
                if (nsInfo.isOrphan()) {
                    qualifier = "- orphan";
                } else if (nsInfo.tableNames.isEmpty()) {
                    qualifier = "- No tables";
                }
                System.out.println("      - " + type + ": " + namespace + " " + qualifier);

                if (!nsInfo.tableNames.isEmpty()) {
                    // Are tables missing?
                    nsInfo.missingTables.forEach(t -> System.out.println("          Missing table: " + t.name));
                    nsInfo.validTables.forEach(t -> checkTableColumns(nsInfo, t));
                }
                scannedNamespaces.put(namespace, nsInfo);
                return nsInfo;
            } catch (SQLException sqle) {
                // Do nothing
                sqle.printStackTrace();
                System.out.println("fail");
            }
        }
        return existingNsInfo;
    }

    public TableInfo checkTableColumns(NamespaceInfo nsInfo, Table table) {
        try {
            selectColumnStatement1.setString(1, nsInfo.namespace);
            selectColumnStatement1.setString(2, table.name);

            ResultSet resultSet = selectColumnStatement1.executeQuery();
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
                tableInfo.missingColumns.forEach(c -> System.out.println("            Missing column: " + c.columnName));
                tableInfo.columnsWithWrongType.forEach(
                    c -> System.out.println("            Incorrect type for column: " + c.columnName + " expected: " + c.expectedType + " actual: " + c.dataType)
                );
            }

            nsInfo.scannedTables.add(tableInfo);
            return tableInfo;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Upgrades a namespace by adding missing tables/columns and
     * changing columns with incorrect types.
     */
    public void upgradeNamespace(NamespaceInfo nsInfo) throws SQLException {
        // Add missing tables
        for (Table t : nsInfo.missingTables) {
            t.createSqlTable(connection, nsInfo.namespace, true);
        }

        for (Table t : nsInfo.validTables) {
            TableInfo tableInfo = checkTableColumns(nsInfo, t);
            // Add missing columns
            Statement alterStatement = connection.createStatement();
            if (!tableInfo.missingColumns.isEmpty()) {
                String alterTableSql = String.format(
                    "ALTER TABLE %s.%s %s",
                    nsInfo.namespace,
                    t.name,
                    tableInfo.missingColumns
                        .stream()
                        .map(ColumnInfo::getAddColumnSql)
                        .collect(Collectors.joining(", "))
                );
                alterStatement.execute(alterTableSql);
            }

            // Attempt to migrate column types
            if (!tableInfo.columnsWithWrongType.isEmpty()) {
                String alterTableSql = String.format(
                    "ALTER TABLE %s.%s %s",
                    nsInfo.namespace,
                    t.name,
                    tableInfo.columnsWithWrongType
                        .stream()
                        .map(ColumnInfo::getAlterColumnTypeSql)
                        .collect(Collectors.joining(", "))
                );
                alterStatement.execute(alterTableSql);
            }
        }
    }

    /**
     * Obtains the table names for a given namespace.
     */
    public List<String> getTables(String namespace) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        selectNamespaceTablesStatement.setString(1, namespace);
        ResultSet resultSet = selectNamespaceTablesStatement.executeQuery();
        while (resultSet.next()) {
            tableNames.add(resultSet.getString(1));
        }

        return tableNames;
    }

    /**
     * Obtains the loaded date for a given namespace.
     */
    public String getDateLoaded(String namespace) throws SQLException {
        selectLoadedDateStatement.setString(1, namespace);
        ResultSet loadedDateSet = selectLoadedDateStatement.executeQuery();
        if (loadedDateSet.next()) {
            return loadedDateSet.getString(1);
        }
        return "";
    }

    public List<ColumnInfo> getColumns(String namespace, Table table) {
        List<ColumnInfo> columns = new ArrayList<>();
        try {
            selectColumnStatement.setString(1, namespace);
            selectColumnStatement.setString(2, table.name);

            ResultSet resultSet = selectColumnStatement.executeQuery();
            while (resultSet.next()) {
                columns.add(new ColumnInfo(
                    resultSet.getString(1),
                    resultSet.getString(2)
                ));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return columns;
    }

    public static class ColumnInfo {
        public final String columnName;
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

        public ColumnInfo(Field field) {
            this(field.name, field.getSqlTypeName());
            this.expectedType = field.getSqlTypeName();
        }

        public String getAlterColumnTypeSql() {
            return String.format("ALTER COLUMN %s TYPE %s USING %s::%s", columnName, expectedType, columnName, expectedType);
        }

        public String getAddColumnSql() {
            return String.format("ADD COLUMN IF NOT EXISTS %s %s", columnName, expectedType);
        }
    }

    public static class NamespaceInfo {
        public final String nickname;
        public final String namespace;
        private String loadedDate = "";
        public final List<String> tableNames = new ArrayList<>();
        public final List<Table> missingTables = new ArrayList<>();
        public final List<Table> validTables = new ArrayList<>();
        public final List<TableInfo> scannedTables = new ArrayList<>();

        /** Used for tests only */
        public NamespaceInfo(String namespace, List<String> excludedTables) {
            this.namespace = namespace;
            this.nickname = namespace;
            for (Table table : Table.tablesInOrder) {
                this.tableNames.add(table.name);
            }
            this.tableNames.removeAll(excludedTables);
            sortTables();
        }

        public NamespaceInfo(String namespace, String nickName, SqlSchemaUpdater schemaUpdater) throws SQLException {
            this.namespace = namespace;
            this.nickname = nickName;
            this.loadedDate = schemaUpdater.getDateLoaded(namespace);
            this.tableNames.addAll(schemaUpdater.getTables(namespace));
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
                    missingColumns.add(new ColumnInfo(field));
                }
            }
        }

        public boolean hasColumnIssues() {
            return !missingColumns.isEmpty() || !columnsWithWrongType.isEmpty();
        }
    }
}
