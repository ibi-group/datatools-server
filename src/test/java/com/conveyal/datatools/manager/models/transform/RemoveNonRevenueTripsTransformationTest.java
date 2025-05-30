package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.csvreader.CsvReader;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.conveyal.datatools.TestUtils.zipFolderFiles;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class RemoveNonRevenueTripsTransformationTest extends UnitTest {

    private static final RemoveNonRevenueTripsTransformation trans = new RemoveNonRevenueTripsTransformation();

    @Test
    void testNonRevenueTripRemoval() throws IOException {
        final List<String> stopTimesNonRevenue = List.of(
            "non-revenue-trip1,1,",
            "non-revenue-trip1,1,1",
            "non-revenue-trip1,1,1",
            "non-revenue-trip1,1,"
        );

        final List<String> stopTimesRevenue = List.of(
            "revenue-trip,0,0",
            "revenue-trip,1,1",
            "revenue-trip,1,1",
            "revenue-trip,0,0"
        );

        // Fields: trip_id, continuous_pickup, continuous_drop_off.
        final String stopTimesRows = Stream
            .concat(stopTimesNonRevenue.stream(), stopTimesRevenue.stream())
            .collect(Collectors.joining("\n"));

        Map<String, List<String[]>> stopTimes = trans.getAllRevenueStopTimes(
            new CsvReader(new StringReader(stopTimesRows)),
            0,
            1,
            2
        );
        assertEquals(1, stopTimes.size());
        assertEquals(stopTimesRevenue.size(), stopTimes.get("revenue-trip").size());
    }

    @Test
    void testEndToEndNonRevenueTripRemoval() throws Exception {
        File zip = zipFolderFiles("non-revenue-trips");
        FeedTransformZipTarget zipTarget = new FeedTransformZipTarget(zip);
        trans.transform(zipTarget, new MonitorableJob.Status());
        assertFalse(zipTarget.feedTransformResult.tableTransformResults.isEmpty());
        assertFalse(hasNonRevenueTrips(zipTarget));
    }

    private boolean hasNonRevenueTrips(FeedTransformZipTarget zipTarget) throws IOException {
        try (ZipFile gtfsZipfile = new ZipFile(zipTarget.gtfsFile.getAbsolutePath())) {
            for (String tableName : List.of("stop_times.txt", "trips.txt")) {
                ZipEntry entry = gtfsZipfile.getEntry(tableName);

                if (entry == null) {
                    throw new AssertionError(
                        String.format("Expected table %s not found in outputted zip file", tableName)
                    );
                }

                try (InputStream zipInputStream = gtfsZipfile.getInputStream(entry)) {
                    CsvReader csvReader = new CsvReader(zipInputStream, ',', StandardCharsets.UTF_8);
                    try {
                        csvReader.readHeaders();
                        while (csvReader.readRecord()) {
                            if ("non-revenue-trip1".equals(csvReader.get("tripId"))) {
                                return true;
                            }
                        }
                    } finally {
                        csvReader.close();
                    }
                }
            }
        }
        return false;
    }
}