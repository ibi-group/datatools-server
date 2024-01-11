package com.conveyal.datatools.manager.jobs.validation;

import com.conveyal.datatools.UnitTest;
import com.csvreader.CsvReader;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.zenika.snapshotmatcher.SnapshotMatcher.matchesSnapshot;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SharedStopsValidatorTest extends UnitTest {
    private void attemptToParseInvalidCSV(String csv, String expectedError) {
        CsvReader configReader = CsvReader.parse(csv);
        Exception exception = assertThrows(RuntimeException.class, () -> {
            SharedStopsValidator.getHeaderIncedes(configReader);
        });

        String error = exception.getMessage();
        assertEquals(expectedError, error);
    }

    private Map parseValidCSV(String csv) {
        CsvReader configReader = CsvReader.parse(csv);
        return SharedStopsValidator.getHeaderIncedes(configReader);
    }

    @Test
    void canHandleIncorrectCsv() {
        String invalid = "shared_stops.csv contained invalid headers!";
        String missing = "shared_stops.csv is missing headers!";

        attemptToParseInvalidCSV("this is a great string, but it's not a shared_stops csv!", invalid);
        attemptToParseInvalidCSV("", invalid);
        attemptToParseInvalidCSV("is_primary,stop_id", missing);
        attemptToParseInvalidCSV("is_primary,feed_id,,stop_group_id", invalid);
        attemptToParseInvalidCSV("is_primary,feed_id,stop_group_id", missing);
        attemptToParseInvalidCSV("feed_id,   is_primary,    stop_group_id,stop_id", invalid);
    }

    @Test
    void canHandleVariousCorrectCSV() {
        assertThat(parseValidCSV("is_primary,feed_id,stop_id,stop_group_id"), matchesSnapshot());
        assertThat(parseValidCSV("feed_id,stop_id,stop_group_id,is_primary"), matchesSnapshot());

        // Only last is handled
        assertThat(parseValidCSV("feed_id,is_primary,stop_group_id,stop_id,feed_id"), matchesSnapshot());
    }
}
