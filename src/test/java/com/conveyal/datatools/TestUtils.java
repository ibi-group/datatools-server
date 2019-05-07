package com.conveyal.datatools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.stream.Collectors;

import static com.conveyal.datatools.manager.DataManager.GTFS_DATA_SOURCE;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class TestUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

    /**
     * Parse a json string into an unmapped JsonNode object
     */
    public static JsonNode parseJson(String jsonString) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(jsonString);
    }

    public static void assertThatSqlQueryYieldsRowCount(String sql, int expectedRowCount) throws
        SQLException {
        LOG.info(sql);
        int recordCount = 0;
        ResultSet rs = GTFS_DATA_SOURCE.getConnection().prepareStatement(sql).executeQuery();
        while (rs.next()) recordCount++;
        assertThat("Records matching query should equal expected count.", recordCount, equalTo(expectedRowCount));
    }

    public static void assertThatFeedHasNoErrorsOfType (String namespace, String... errorTypes) throws SQLException {
        assertThatSqlQueryYieldsRowCount(
            String.format(
                "select * from %s.errors where error_type in (%s)",
                namespace,
                Arrays.stream(errorTypes)
                    .map(error -> String.format("'%s'", error))
                    .collect(Collectors.joining(","))
            ),
            0
        );
    }
}
