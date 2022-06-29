package com.conveyal.datatools.manager.utils;

import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.gtfs.loader.Table;
import com.google.common.base.Strings;

import java.sql.SQLException;

import static com.conveyal.datatools.TestUtils.assertThatSqlCountQueryYieldsExpectedCount;

/**
 * This class contains helper methods to assert against various PSQL tables
 * of a given namespace.
 */
public class SqlAssert {
    private final FeedVersion version;
    public final SqlTableAssert agency = new SqlTableAssert(Table.AGENCY);
    public final SqlTableAssert calendar = new SqlTableAssert(Table.CALENDAR);
    public final SqlTableAssert calendarDates = new SqlTableAssert(Table.CALENDAR_DATES);
    public final SqlTableAssert errors = new SqlTableAssert("errors");
    public final SqlTableAssert routes = new SqlTableAssert(Table.ROUTES);
    public final SqlTableAssert trips = new SqlTableAssert(Table.TRIPS);
    public final SqlTableAssert stops = new SqlTableAssert(Table.STOPS);
    public final SqlTableAssert patterns = new SqlTableAssert(Table.PATTERNS);

    public SqlAssert(FeedVersion version) {
        this.version = version;
    }

    /**
     * Checks there are no unused service ids.
     */
    public void assertNoUnusedServiceIds() throws SQLException {
        errors.assertCount(0, "error_type='SERVICE_UNUSED'");
    }

    /**
     * Checks there are no referential integrity issues.
     */
    public void assertNoRefIntegrityErrors() throws SQLException {
        errors.assertCount(0, "error_type = 'REFERENTIAL_INTEGRITY'");
    }

    /**
     * Helper class to assert against a particular PSQL table.
     */
    public class SqlTableAssert {
        private final String tableName;
        private SqlTableAssert(Table table) {
            this.tableName = table.name;
        }

        private SqlTableAssert(String tableName) {
            this.tableName = tableName;
        }

        /**
         * Helper method to assert a row count on a simple WHERE clause.
         */
        public void assertCount(int count, String condition) throws SQLException {
            assertThatSqlCountQueryYieldsExpectedCount(
                String.format("SELECT count(*) FROM %s.%s %s", version.namespace, tableName,
                    Strings.isNullOrEmpty(condition) ? "" : "WHERE " + condition),
                count
            );
        }

        /**
         * Helper method to assert a row count on the entire table.
         */
        public void assertCount(int count) throws SQLException {
            assertCount(count, null);
        }
    }
}
