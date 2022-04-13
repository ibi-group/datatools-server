package com.conveyal.datatools.manager;

import com.conveyal.datatools.manager.utils.sql.NamespaceCheck;
import com.conveyal.datatools.manager.utils.sql.SqlSchemaUpdater;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.conveyal.datatools.manager.DataManager.initializeApplication;
import static com.conveyal.datatools.manager.DataManager.registerRoutes;

/**
 * Main method that performs batch SQL updates on optionally filtered set of namespaces over the GTFS SQL database
 * connection specified in the configuration files (env.yml).
 *
 * For example, this script can add a column for all `routes` tables for schemas that do not have a null value for
 * the filename field in the feeds table. In effect, this would alter only those feeds that are editor snapshots.
 *
 * Argument descriptions:
 * 1. path to env.yml
 * 2. path to server.yml
 * 3. string update sql statement to apply to optionally filtered feeds (this should contain a namespace wildcard
 *    {@link UpdateSQLFeedsMain#NAMESPACE_WILDCARD} string for the namespace argument substitution).
 * 4. string field to filter feeds on
 * 5. string value (corresponding to field in arg 3) to filter feeds on (omit to use NULL as value or comma separate to
 *    include multiple values)
 * 6. boolean (optional) whether to run SQL as a test run (i.e., rollback changes and do not commit). If missing, this
 *    defaults to true.
 *
 * Sample arguments:
 *
 * "/path/to/config/env.yml" "/path/to/config/server.yml" "alter table %s.routes add column some_column_name int" filename
 *
 * "/path/to/config/env.yml" "/path/to/config/server.yml" "alter table %s.routes add column some_column_name int" filename /tmp/gtfs.zip
 */
public class UpdateSQLFeedsMain {
    public static final String NAMESPACE_WILDCARD = "#ns#";

    public static void main(String[] args) throws IOException, SQLException {
        // First, set up application.
        initializeApplication(args);
        // Register HTTP endpoints so that the status endpoint is available during migration.
        registerRoutes();
        // Load args (first and second args are used for config files).
        // Update SQL string should be contained within third argument with %s specifier where namespace should be
        // substituted.
        String updateSql = args[2];
        // The next arguments will apply a where clause to conditionally to apply the updates.
        String field = args.length > 3 ? args[3] : null;
        String valuesArg = args.length > 4 ? args[4] : null;
        String[] values;
        // Set value to null if the string value = "null".
        if ("null".equals(valuesArg) || valuesArg == null) values = null;
        else values = valuesArg.split(",");

        // If test run arg is not included, default to true. Else, only set to false if value equals false.
        boolean testRun = args.length <= 5 || !"false".equals(args[5]);

        //List<String> failedNamespace = updateFeedsWhere(updateSql, field, values, testRun);
        checkAndUpdateTables(testRun);
        System.out.println("Finished!");
        //System.out.println("Failed namespaces: " + String.join(", ", failedNamespace));
        System.exit(0);
    }

    /**
     * Check that tables from namespaces referenced from projects/feed sources
     * have all the columns, and upgrades the tables in those namespaces
     * If testRun is true, all changes applied to database will be rolled back at the end of execution.
     */
    private static void checkAndUpdateTables(boolean testRun) throws SQLException {
        try (Connection connection = DataManager.GTFS_DATA_SOURCE.getConnection()) {
            // Keep track of failed namespaces for convenient printing at end of method.
            List<String> failedNamespaces = new ArrayList<>();
            int successCount = 0;

            if (!testRun) {
                System.out.println("Auto-committing each statement");
                // Set auto-commit to true.
                connection.setAutoCommit(true);
            } else {
                System.out.println("TEST RUN. Changes will NOT be committed (a rollback occurs at the end of method).");
            }

            SqlSchemaUpdater schemaUpdater = new SqlSchemaUpdater(connection);
            Collection<NamespaceCheck> checkedNamespaces = schemaUpdater.checkReferencedNamespaces();
            // schemaUpdater.printReport();

            for (NamespaceCheck namespaceCheck : checkedNamespaces) {
                String namespace = namespaceCheck.namespace;
                try {
                    if (!namespaceCheck.isOrphan()) {
                        schemaUpdater.upgradeNamespaceIfNotOrphan(namespaceCheck);
                        System.out.println(String.format("Updated namespace %s", namespace));
                        successCount++;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    failedNamespaces.add(namespace);
                }
            }

            System.out.println(String.format("Updated %d namespaces.", successCount));
            System.out.printf("Failed namespaces (%d):\n%s\n", failedNamespaces.size(), String.join("\n", failedNamespaces));
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
