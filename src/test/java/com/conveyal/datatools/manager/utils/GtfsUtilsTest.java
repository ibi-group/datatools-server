package com.conveyal.datatools.manager.utils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Contains tests for GtfsUtils.
 */
public class GtfsUtilsTest {
    @ParameterizedTest
    @MethodSource("createGetTableNameCases")
    public void testGetTableNames(String tableName, boolean outcome) {
        assertEquals(outcome, GtfsUtils.getGtfsTable(tableName) != null);
    }

    private static Stream<Arguments> createGetTableNameCases() {
        return Stream.of(
            Arguments.of("agency", true),
            Arguments.of("a_nonexistent_table", false)
        );
    }
}
