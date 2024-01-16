package com.conveyal.datatools.manager.jobs.validation;

import com.conveyal.datatools.UnitTest;
import com.csvreader.CsvReader;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.zenika.snapshotmatcher.SnapshotMatcher.matchesSnapshot;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SharedStopsValidatorTest extends UnitTest {

    @ParameterizedTest
    @MethodSource("createCSVHeaders")
    void canHandleVariousCorrectCSV(String headers) {
        assertThat(SharedStopsValidator.getHeaderIndices(CsvReader.parse(headers)), matchesSnapshot());
    }

    private static Stream<String> createCSVHeaders() {
        return Stream.of(
            "is_primary,feed_id,stop_id,stop_group_id",
            "feed_id,stop_id,stop_group_id,is_primary",
            "feed_id,is_primary,stop_group_id,stop_id,feed_id"
        );
    }

    @ParameterizedTest
    @MethodSource("createInvalidCSVArguments")
    void attemptToParseInvalidCSV(InvalidCSVArgument invalidCSVArgument) {
        CsvReader configReader = CsvReader.parse(invalidCSVArgument.csv);
        Exception exception = assertThrows(RuntimeException.class, () -> SharedStopsValidator.getHeaderIndices(configReader));
        assertEquals(invalidCSVArgument.expectedError, exception.getMessage());
    }

    private static Stream<InvalidCSVArgument> createInvalidCSVArguments() {
        String invalid = "Unknown shared stops header: ";
        String missing = "shared_stops.csv is missing headers!";
        return Stream.of(
            new InvalidCSVArgument("this is a great string, but it's not a shared_stops csv!", invalid + "this is a great string"),
            new InvalidCSVArgument("", invalid),
            new InvalidCSVArgument("is_primary,stop_id", missing),
            new InvalidCSVArgument("is_primary,feed_id,,stop_group_id", invalid),
            new InvalidCSVArgument("is_primary,feed_id,stop_group_id", missing),
            new InvalidCSVArgument("feed_id,   is_primary,    stop_group_id,stop_id", invalid + "   is_primary")
        );
    }

    private static class InvalidCSVArgument {
        public String csv;
        public String expectedError;

        public InvalidCSVArgument(String csv, String expectedError) {
            this.csv = csv;
            this.expectedError = expectedError;
        }
    }
}
