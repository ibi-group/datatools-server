package com.conveyal.datatools.manager.utils;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.gtfsplus.tables.GtfsPlusTable;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
        Persistence.projects.getAll().forEach(
            p -> {
                System.out.println("Project " + p.name);
                Persistence.feedSources.getFiltered(eq("projectId", p.id)).forEach(
                    fs -> {
                        System.out.println("- FeedSource " + fs.name + " " + fs.id);
                        Bson feedSourceIdFilter = eq("feedSourceId", fs.id);
                        Bson feedSourceIdNamespaceFilter = and(
                            eq("feedSourceId", fs.id),
                            not(eq("namespace", null))
                        );
                        List<FeedVersion> allFeedVersions = Persistence.feedVersions.getFiltered(feedSourceIdFilter);
                        List<FeedVersion> feedVersions = Persistence.feedVersions.getFiltered(feedSourceIdNamespaceFilter);
                        System.out.println("  - FeedVersions (" + feedVersions.size() + "/" + allFeedVersions.size() + " with valid namespace)");
                        feedVersions.forEach(
                            fv -> {
                                System.out.print("    - v" + fv.version + ": " + fv.namespace);
                                checkTablesForNamespace(fv.namespace);
                            }
                        );
                        List<Snapshot> allSnapshots = Persistence.snapshots.getFiltered(feedSourceIdFilter);
                        List<Snapshot> snapshots = Persistence.snapshots.getFiltered(feedSourceIdNamespaceFilter);
                        System.out.println("  - Snapshots (" + snapshots.size() + "/" + allSnapshots.size() + " with valid namespace)");
                        snapshots.forEach(
                            sn -> {
                                System.out.println("    - " + sn.name + " " + sn.id);
                                System.out.print("      - namespace: " + sn.namespace);
                                checkTablesForNamespace(sn.namespace);
                                if (!Strings.isNullOrEmpty(sn.snapshotOf) && !sn.snapshotOf.equals("mapdb_editor")) {
                                    System.out.print("      - snapshotOf: " + sn.snapshotOf);
                                    checkTablesForNamespace(sn.snapshotOf);
                                }
                            }
                        );
                    }
                );
            }
        );
    }

    public static void checkTablesForNamespace(String namespace) {
        try (Connection connection = DataManager.GTFS_DATA_SOURCE.getConnection()) {
            // Check that all tables for the namespace are present.
            PreparedStatement selectNamespaceTablesStatement = connection.prepareStatement(
                "select table_name from information_schema.tables where table_schema = ?"
            );
            selectNamespaceTablesStatement.setString(1, namespace);
            PreparedStatement selectLoadedDateStatement = connection.prepareStatement(
                "select loaded_date from feeds where namespace = ?"
            );
            selectLoadedDateStatement.setString(1, namespace);

            ResultSet loadedDateSet = selectLoadedDateStatement.executeQuery();
            String loadedDate = "";
            while (loadedDateSet.next()) {
                loadedDate = loadedDateSet.getString(1);
                break;
            }
            // System.out.println("          Loaded date: " + loadedDate);

            ResultSet resultSet = selectNamespaceTablesStatement.executeQuery();
            List<String> tableNames = new ArrayList<>();
            while (resultSet.next()) {
                String tableName = resultSet.getString(1);
                tableNames.add(tableName);
                //System.out.printf("          - %s\n", tableName);

/*
                String updateTableSql = updateSql.replaceAll(NAMESPACE_WILDCARD, namespace);
                Statement statement = connection.createStatement();
                try {
                    System.out.println(updateTableSql);
                    int updated = statement.executeUpdate(updateTableSql);
                    System.out.println(String.format("Updated rows: %d", updated));
                    successCount++;
                } catch (SQLException e) {
                    // The stops table likely did not exist for the schema.
                    e.printStackTrace();
                    failedNamespace.add(namespace);
                }

 */
            }

            if (tableNames.isEmpty()) {
                if (Strings.isNullOrEmpty(loadedDate)) {
                    System.out.println(" - orphan");
                } else {
                    // Are there tables?
                    System.out.println("\n          No tables found.");
                }
            } else {
                System.out.println();
                // Are tables missing?
                for (Table table : Table.tablesInOrder) {
                    if (tableNames.contains(table.name)) {
                        System.out.println("          Found table: " + table.name);
                        // Are fields missing?
                        checkTableColumns(namespace, connection, table);
                    } else {
                        System.out.println("          Missing table: " + table.name);
                    }
                }
            }






        } catch (SQLException sqle) {
            //
        }

        //
        //Table.tablesInOrder

        // If a table is missing, create it?

        // For tables that were there already, check fields

    }

    private static void checkTableColumns(String namespace, Connection connection, Table table) throws SQLException {
        // TODO Refactor all prepared statements.
        PreparedStatement selectColumnStatement = connection.prepareStatement(
            "select column_name, data_type from information_schema.columns where table_schema = ? and table_name = ?"
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

        for (Field field : table.fields) {
            ColumnInfo columnForField = null;
            for (ColumnInfo column : columns) {
                if (field.name.equals(column.columnName)) {
                    columnForField = column;
                    break;
                }
            }

            if (columnForField != null) {
                // System.out.println("            Found: " + field.name);

                // Are fields that are present of the correct type?
                if (!field.getSqlTypeName().equals(columnForField.dataType)) {
                    System.out.println("Incorrect type for " + field.name + " expected: " + field.getSqlTypeName() + " actual: " + columnForField.dataType);
                }
                // Only the id column seems to be marked as not nullable, so we won't check that for now.
            } else {
                System.out.println("            Not found: " + field.name);
            }
        }
    }

    public static class ColumnInfo {
        public String columnName;
        public String dataType;

        public ColumnInfo(String columnName, String dataType) {
            this.columnName = columnName;
            this.dataType = dataType;
            if (dataType.equals("character varying")) {
                this.dataType = "varchar";
            }
            if (dataType.equals("ARRAY")) {
                this.dataType = "text[]";
            }
        }
    }
}
