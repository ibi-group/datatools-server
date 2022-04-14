package com.conveyal.datatools.manager.utils.sql;

import com.conveyal.gtfs.loader.Table;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Contains the outcome of a namespace check (e.g. whether tables are missing).
 */
public class NamespaceCheck {
    public final String nickname;
    public final String namespace;
    public final List<String> tableNames = new ArrayList<>();
    public final List<Table> missingTables = new ArrayList<>();
    public final List<Table> validTables = new ArrayList<>();
    public final List<TableCheck> checkedTables = new ArrayList<>();

    /**
     * Used for tests only.
     */
    public NamespaceCheck(String namespace, List<String> excludedTables) {
        this.namespace = namespace;
        this.nickname = namespace;
        for (Table table : Table.tablesInOrder) {
            this.tableNames.add(table.name);
        }
        this.tableNames.removeAll(excludedTables);
        checkMissingTables();
    }

    public NamespaceCheck(String namespace, String nickName, SqlSchemaUpdater schemaUpdater) throws SQLException {
        this.namespace = namespace;
        this.nickname = nickName;
        this.tableNames.addAll(schemaUpdater.getTableNames(namespace));

        checkMissingTables();

        for (Table t : validTables) {
            TableCheck tableCheck = new TableCheck(t, namespace, schemaUpdater);
            checkedTables.add(tableCheck);
            tableCheck.printReport();
        }
    }

    /**
     * Establishes, for this namespace, which tables are valid and which are missing.
     */
    private void checkMissingTables() {
        for (Table table : Table.tablesInOrder) {
            if (tableNames.contains(table.name)) {
                validTables.add(table);
            } else {
                missingTables.add(table);
            }
        }
    }

    /**
     * @return true if a namespace does not have any reference in the PSQL database.
     */
    public boolean isOrphan() {
        return tableNames.isEmpty();
    }

    public void printReport(String type) {
        String qualifier = "";
        if (isOrphan()) {
            qualifier = "- orphan";
        } else if (tableNames.isEmpty()) {
            qualifier = "- No tables";
        }
        System.out.println("      - " + type + ": " + namespace + " " + qualifier);

        if (!tableNames.isEmpty()) {
            for (Table t : missingTables) {
                System.out.println("          Missing table: " + t.name);
            }
            for (TableCheck tableCheck : checkedTables) {
                tableCheck.printReport();
            }
        }
    }
}
