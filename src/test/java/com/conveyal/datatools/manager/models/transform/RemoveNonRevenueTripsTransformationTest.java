package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.UnitTest;
import com.csvreader.CsvReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RemoveNonRevenueTripsTransformationTest extends UnitTest {

    // Fields: trip_id,arrival_time,departure_time,stop_id,stop_sequence,
    //         continuous_pickup,continuous_drop_off
    private static final String STOP_TIMES_NONREVENUE =
            "nonrevenue-trip1,08:00:00,08:00:00,4u6g,1,1,1\n" +
            "nonrevenue-trip2,08:01:00,08:01:00,johv,2,1,1\n" +
            "nonrevenue-trip3,08:02:00,08:02:00,4u6g,3,1,1\n" +
            "nonrevenue-trip4,08:03:00,08:03:00,johv,4,1,1\n";
    private static final String STOP_TIMES_REVENUE =
            "mixed-trip1,07:00:00,07:00:00,4u6g,1,0,0\n" +
            "mixed-trip2,07:01:00,07:01:00,johv,2,1,1\n" +
            "mixed-trip3,07:02:00,07:02:00,4u6g,3,0,0\n" +
            "mixed-trip4,07:03:00,07:03:00,johv,4,1,1\n";

    private static RemoveNonRevenueTripsTransformation trans =
            new RemoveNonRevenueTripsTransformation();

    @Test
    public void testNonRevenueStopTimes() throws IOException {
        Map<String, List<String[]>> stopTimes = trans.getAllRevenueStopTimes(
            new CsvReader(new StringReader(STOP_TIMES_NONREVENUE)),
            0, 5, 6 // trip_id, continuous_pickup, continuous_drop_off
        );
        assertEquals(0, stopTimes.size());    
    }

    @Test
    public void testRevenueStopTimes() throws IOException {
        Map<String, List<String[]>> stopTimes = trans.getAllRevenueStopTimes(
            new CsvReader(new StringReader(STOP_TIMES_REVENUE)),
            0, 5, 6 // trip_id, continuous_pickup, continuous_drop_off
        );
        assertEquals(2, stopTimes.size());    
        assertTrue(stopTimes.containsKey("mixed-trip1"));
        assertTrue(stopTimes.containsKey("mixed-trip3"));
    }
}
