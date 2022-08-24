package com.conveyal.datatools.manager.utils.sql;

import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.gtfs.loader.Table;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Contains the outcome of a namespace check (e.g. whether tables are missing).
 */
public class NamespaceCheck {
    public final String type;
    private final FeedSource feedSource;
    public final String namespace;
    public final Boolean isDeleted;
    private final List<String> tableNames = new ArrayList<>();
    public final List<Table> missingTables = new ArrayList<>();
    public final List<Table> validTables = new ArrayList<>();
    public final List<TableCheck> checkedTables = new ArrayList<>();

    /**
     * Used for tests only.
     */
    public NamespaceCheck(String namespace, FeedSource feedSource, String type, List<String> excludedTables) {
        this.namespace = namespace;
        this.feedSource = feedSource;
        this.type = type;
        this.isDeleted = false;
        for (Table table : Table.tablesInOrder) {
            this.tableNames.add(table.name);
        }
        this.tableNames.removeAll(excludedTables);
        checkMissingTables();
    }

    public NamespaceCheck(String namespace, FeedSource feedSource, String type, SqlSchemaUpdater schemaUpdater)
        throws SQLException
    {
        this.namespace = namespace;
        this.feedSource = feedSource;
        this.type = type;
        this.tableNames.addAll(schemaUpdater.getTableNames(namespace));
        this.isDeleted = schemaUpdater.getDeletedStatus(namespace);

        checkMissingTables();

        for (Table t : validTables) {
            checkedTables.add(new TableCheck(t, namespace, type, schemaUpdater));
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
        if (isDeleted != null && isDeleted) {
            qualifier = "- deleted ";
        }
        if (isOrphan()) {
            qualifier += "- orphan";
        } else if (tableNames.isEmpty()) {
            qualifier += "- No tables";
        }
        System.out.printf("\t\t- %s: %s %s%n", type, namespace, qualifier);

        if (!tableNames.isEmpty()) {
            for (Table t : missingTables) {
                System.out.printf("\t\t\tMissing table: %s%n", t.name);
            }
            for (TableCheck tableCheck : checkedTables) {
                tableCheck.printReport();
            }
        }
    }

    public String getHeaderText() {
        return String.format("%s/%s/%s", feedSource.retrieveProject().name, feedSource.name, type);
    }
}
