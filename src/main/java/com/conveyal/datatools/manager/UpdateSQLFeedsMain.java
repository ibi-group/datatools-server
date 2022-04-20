package com.conveyal.datatools.manager;

import com.conveyal.datatools.manager.utils.sql.NamespaceCheck;
import com.conveyal.datatools.manager.utils.sql.SqlSchemaUpdater;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.conveyal.datatools.manager.DataManager.initializeApplication;
import static com.conveyal.datatools.manager.DataManager.registerRoutes;

/**
 * This class checks for missing SQL tables/columns and columns of incorrect type in namespaces referenced by projects,
 * and adds/modifies tables according to the referenced gtfs-lib implementation.
 *
 * Argument descriptions:
 * 1. path to env.yml
 * 2. path to server.yml
 * 3. Types of namespaces to upgrade, a combination of "EDITOR", "SNAPSHOTS", and "VERSIONS" separated by "-",
 * 4. boolean (optional) whether to run SQL as a test run (i.e., rollback changes and do not commit). If missing, this
 *    defaults to true.
 *
 * Sample arguments:
 *   "/path/to/config/env.yml" "/path/to/config/server.yml" EDITOR-SNAPSHOTS false
 */
public class UpdateSQLFeedsMain {
    public static void main(String[] args) throws IOException, SQLException {
        // First, set up application.
        initializeApplication(args);
        // Register HTTP endpoints so that the status endpoint is available during migration.
        registerRoutes();
        // Load args (first and second args are used for config files).

        // What to upgrade
        String itemsToUpgrade = args.length > 2 ? args[2] : null;
        List<String> namespaceTypesToCheck = itemsToUpgrade == null
            ? new ArrayList<>()
            : Arrays.stream(itemsToUpgrade.split("-"))
                .filter(SqlSchemaUpdater.NAMESPACE_TYPES::contains)
                .collect(Collectors.toList());

        if (namespaceTypesToCheck.isEmpty()) {
            System.out.println("No namespace types were specified. Exiting.");
        } else {
            // If test run arg is not included, default to true. Else, only set to false if value equals false.
            boolean testRun = args.length <= 3 || !"false".equals(args[3]);

            checkAndUpdateTables(testRun, namespaceTypesToCheck);
            System.out.println("Finished!");
        }
        System.exit(0);
    }

    /**
     * Check that tables from namespaces referenced from projects/feed sources
     * have all the columns, and upgrades the tables in those namespaces
     * If testRun is true, all changes applied to database will be rolled back at the end of execution.
     */
    private static void checkAndUpdateTables(boolean testRun, List<String> namespaceTypesToCheck) throws SQLException {
        try (Connection connection = DataManager.GTFS_DATA_SOURCE.getConnection()) {
            // Keep track of failed namespaces for convenient printing at end of method.
            List<String> failedNamespaces = new ArrayList<>();
            int successCount = 0;

            if (!testRun) {
                System.out.println("Auto-committing each statement");
                // Set auto-commit to true.
                connection.setAutoCommit(true);
            } else {
                System.out.println("TEST RUN. Changes will NOT be committed.");
            }

            SqlSchemaUpdater schemaUpdater = new SqlSchemaUpdater(connection);
            Collection<NamespaceCheck> checkedNamespaces = schemaUpdater.checkReferencedNamespaces(namespaceTypesToCheck);

            for (NamespaceCheck namespaceCheck : checkedNamespaces) {
                String namespace = namespaceCheck.namespace;
                try {
                    if (!namespaceCheck.isOrphan()) {
                        schemaUpdater.upgradeNamespaceIfNotOrphanOrDeleted(namespaceCheck);
                        System.out.printf("Updated namespace %s%n", namespace);
                        successCount++;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    failedNamespaces.add(namespace);
                }
            }

            System.out.printf("Updated %d namespaces.%n", successCount);
            System.out.printf("Failed namespaces (%d):%n%s%n", failedNamespaces.size(), String.join("\n", failedNamespaces));
            // No need to commit the transaction because of auto-commit above (in fact, "manually" committing is not
            // permitted with auto-commit enabled.
            if (testRun) {
                // Rollback changes if performing a test run.
                System.out.println("Rolling back changes...");
                connection.rollback();
            }
        }
    }
}
