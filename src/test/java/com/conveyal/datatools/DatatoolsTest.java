package com.conveyal.datatools;

import com.conveyal.datatools.manager.DataManager;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by landon on 2/24/17.
 */
public abstract class DatatoolsTest {
    private static final Logger LOG = LoggerFactory.getLogger(DatatoolsTest.class);
    private static boolean setUpIsDone = false;

    @BeforeClass
    public static void setUp() {
        if (setUpIsDone) {
            return;
        }
        LOG.info("DatatoolsTest setup");
        String[] args = {"configurations/default/env.yml.tmp", "configurations/default/server.yml.tmp"};
        try {
            DataManager.main(args);
            // Attempt to create database for testing.
            String databaseUrl = DataManager.getConfigPropertyAsText("GTFS_DATABASE_URL");
            String databaseName = databaseUrl.split("/")[3];
            try {
                Connection connection = DataManager.GTFS_DATA_SOURCE.getConnection();
                // Auto-commit must be enabled for a create database command.
                connection.setAutoCommit(true);
                String createDBSql = String.format("CREATE DATABASE %s", databaseName);
                LOG.info(createDBSql);
                connection
                    .prepareStatement(createDBSql)
                    .execute();
            } catch (SQLException e) {
                // Catch already exists error.
                e.printStackTrace();
            }
            setUpIsDone = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
