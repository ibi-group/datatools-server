package com.conveyal.datatools.manager;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
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
 * 3. update sql statement to apply to optionally filtered feeds
 * 4. field to filter feeds on
 * 5. value (corresponding to field in arg 3) to filter feeds on (omit to use NULL as value)
 *
 * Sample arguments:
 *
 * "/path/to/config/env.yml" "/path/to/config/server.yml" "alter table %s.routes add column some_column_name int" filename
 *
 * "/path/to/config/env.yml" "/path/to/config/server.yml" "alter table %s.routes add column some_column_name int" filename /tmp/gtfs.zip
 */
public class UpdateSQLFeedsMain {

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
        String value = args.length > 4 ? args[4] : null;
        List<String> failedNamespace = updateFeedsWhere(updateSql, field, value);
        System.out.println("Finished!");
        System.out.println("Failed namespaces: " + String.join(", ", failedNamespace));
        System.exit(0);
    }

    /**
     *
     * @param updateSql
     * @param field
     * @param value
     * @return
     * @throws SQLException
     */
    private static List<String> updateFeedsWhere(String updateSql, String field, String value)throws SQLException {
        if (updateSql == null) throw new RuntimeException("Update SQL must not be null!");
        // Keep track of failed namespaces for convenient printing at end of method.
        List<String> failedNamespace = new ArrayList<>();
        // Select feeds migrated from MapDB
        String selectFeedsSql = "select namespace from feeds";
        if (field != null) {
            // Add where clause if field is not null
            // NOTE: if value is null, where clause will be executed accordingly (i.e., WHERE field = null)
            String operator = value == null ? "IS NULL" : "= ?";
            selectFeedsSql = String.format("%s where %s %s", selectFeedsSql, field, operator);
        }
        Connection connection = DataManager.GTFS_DATA_SOURCE.getConnection();
        // Set auto-commit to true.
        connection.setAutoCommit(true);
        PreparedStatement selectStatement = connection.prepareStatement(selectFeedsSql);
        // Set filter value if not null (otherwise, IS NULL has already been populated).
        if (value != null) {
            selectStatement.setString(1, value);
        }
        System.out.println(selectStatement.toString());
        ResultSet resultSet = selectStatement.executeQuery();
        int successCount = 0;
        while (resultSet.next()) {
            String namespace = resultSet.getString(1);
            String updateLocationSql = String.format(updateSql, namespace);
            Statement statement = connection.createStatement();
            try {
                int updated = statement.executeUpdate(updateLocationSql);
                System.out.println(updateLocationSql);
                System.out.println(String.format("Updated rows: %d", updated));
                successCount++;
            } catch (SQLException e) {
                // The stops table likely did not exist for the schema.
                e.printStackTrace();
                failedNamespace.add(namespace);
            }
        }
        System.out.println(String.format("Updated %d tables.", successCount));
        // No need to commit the transaction because of auto-commit
        connection.close();
        return failedNamespace;
    }
}
