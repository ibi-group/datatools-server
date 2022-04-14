package com.conveyal.datatools.manager.utils.sql;

import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Snapshot;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.storage.StorageException;
import com.google.common.base.Strings;
import org.bson.conversions.Bson;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.not;

/**
 * This class handles operations to detect whether postgresql tables
 * are consistent with the gtfs-lib implementation from the project dependencies,
 * and fills out missing tables/columns and changes column types if needed.
 */
public class SqlSchemaUpdater implements AutoCloseable {
    /** Maintain a list of scanned namespaces to avoid duplicate work. */
    private Map<String, NamespaceCheck> checkedNamespaces = new HashMap<>();

    private final Connection connection;
    private final PreparedStatement selectNamespaceTablesStatement;
    private final PreparedStatement selectColumnStatement;

    public SqlSchemaUpdater(Connection connection) throws SQLException {
        this.connection = connection;

        // Cache all prepared statements.

        selectColumnStatement = connection.prepareStatement(
            "select column_name, data_type from information_schema.columns where table_schema = ? and table_name = ?"
        );

        // Check that all tables for the namespace are present.
        selectNamespaceTablesStatement = connection.prepareStatement(
            "select table_name from information_schema.tables where table_schema = ?"
        );
    }


    /**
     * Checks all namespaces that are referenced from projects
     * for tables and columns that need to be added or changed.
     */
    public Collection<NamespaceCheck> checkReferencedNamespaces() {
        resetCheckedNamespaces();

        Persistence.projects.getAll().forEach(p -> {
            System.out.println("Project " + p.name);
            Persistence.feedSources.getFiltered(eq("projectId", p.id)).forEach(fs -> {
                System.out.println("- FeedSource " + fs.name + " " + fs.id);
                if (!Strings.isNullOrEmpty(fs.editorNamespace)) {
                    checkTablesForNamespace(fs.editorNamespace, p.name + "/" + fs.name + "/editor", "editor");
                }

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
                        checkTablesForNamespace(fv.namespace, fv.name + "/v" + fv.version, "namespace");
                    }
                );

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
            );}
        );

        // Once done, print the SQL statements to update the tables
        System.out.println("-- Overview of changes that should be made");
        checkedNamespaces.values().forEach(ns -> {
            if (!ns.isOrphan()) {
                System.out.println("-- " + ns.nickname);
                // Add missing tables
                ns.missingTables.forEach(t -> {
                    System.out.println("CREATE TABLE IF NOT EXISTS " + ns.namespace + "." + t.name + " ... (use table.createSqlTable(...))");
                });
                // Print alter table statements
                ns.checkedTables.forEach(t -> {
                    if (t.hasColumnIssues()) {
                        System.out.println(t.getAlterTableSql());
                    }
                });
            }
        });

        return checkedNamespaces.values();
    }

    /**
     * Clears the list of scanned namespaces (used for tests).
     */
    public void resetCheckedNamespaces() {
        checkedNamespaces = new HashMap<>();
    }

    public NamespaceCheck checkTablesForNamespace(String namespace, String nickname, String type) {
        NamespaceCheck existingNamespaceCheck = checkedNamespaces.get(namespace);
        if (existingNamespaceCheck == null) {
            try {
                NamespaceCheck namespaceCheck = new NamespaceCheck(
                    namespace,
                    nickname,
                    this
                );
                namespaceCheck.printReport(type);
                checkedNamespaces.put(namespace, namespaceCheck);
                return namespaceCheck;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return existingNamespaceCheck;
    }

    /**
     * Upgrades a namespace that is not orphan by adding missing tables/columns and
     * changing columns with incorrect types.
     */
    public void upgradeNamespaceIfNotOrphan(NamespaceCheck ns) throws SQLException, StorageException {
        if (!ns.isOrphan()) {
            String namespace = ns.namespace;

            // Add missing tables
            for (Table t : ns.missingTables) {
                t.createSqlTable(connection, namespace, true);
            }

            for (TableCheck tableCheck : ns.checkedTables) {
                // Fix column issues for a table, if any.
                if (tableCheck.hasColumnIssues()) {
                    try (Statement alterStatement = connection.createStatement()) {
                        alterStatement.execute(tableCheck.getAlterTableSql());
                    }
                }
            }
        }
    }

    /**
     * Obtains the table names for a given namespace.
     */
    public List<String> getTableNames(String namespace) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        selectNamespaceTablesStatement.setString(1, namespace);
        ResultSet resultSet = selectNamespaceTablesStatement.executeQuery();
        while (resultSet.next()) {
            tableNames.add(resultSet.getString(1));
        }
        return tableNames;
    }

    public List<ColumnCheck> getColumns(String namespace, Table table) {
        List<ColumnCheck> columns = new ArrayList<>();
        try {
            selectColumnStatement.setString(1, namespace);
            selectColumnStatement.setString(2, table.name);

            ResultSet resultSet = selectColumnStatement.executeQuery();
            while (resultSet.next()) {
                columns.add(new ColumnCheck(
                    resultSet.getString(1),
                    resultSet.getString(2)
                ));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return columns;
    }

    @Override
    public void close() throws Exception {
        selectColumnStatement.close();
        selectNamespaceTablesStatement.close();
    }
}
