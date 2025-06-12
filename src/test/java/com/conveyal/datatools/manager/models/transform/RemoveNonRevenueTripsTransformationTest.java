package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.models.TableTransformResult;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.csvreader.CsvReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.conveyal.datatools.TestUtils.appendDate;
import static com.conveyal.datatools.TestUtils.createFeedVersion;
import static com.conveyal.datatools.TestUtils.zipFolderFiles;
import static com.conveyal.datatools.manager.models.FeedRetrievalMethod.MANUALLY_UPLOADED;
import static com.conveyal.datatools.manager.models.transform.RemoveNonRevenueTripsTransformation.CONTINUOUS_DROP_OFF_FIELD_NAME;
import static com.conveyal.datatools.manager.models.transform.RemoveNonRevenueTripsTransformation.CONTINUOUS_PICKUP_FIELD_NAME;
import static com.conveyal.datatools.manager.models.transform.RemoveNonRevenueTripsTransformation.DROP_OFF_FIELD_NAME;
import static com.conveyal.datatools.manager.models.transform.RemoveNonRevenueTripsTransformation.PICKUP_FIELD_NAME;
import static com.conveyal.datatools.manager.models.transform.RemoveNonRevenueTripsTransformation.TRIP_ID_FIELD_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class RemoveNonRevenueTripsTransformationTest extends UnitTest {

    private static Project project;
    private static FeedSource feedSource;
    private static final RemoveNonRevenueTripsTransformation trans = new RemoveNonRevenueTripsTransformation();

    @BeforeAll
    static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();
        DatatoolsTest.enableMTCExtension();
        Auth0Connection.setAuthDisabled(true);
        project = new Project();
        project.name = appendDate("Test");
        Persistence.projects.create(project);
        feedSource = new FeedSource(appendDate("Test Feed"), project.id, MANUALLY_UPLOADED);
        Persistence.feedSources.create(feedSource);
    }

    @AfterAll
    static void tearDown() {
        Auth0Connection.setAuthDisabled(Auth0Connection.getDefaultAuthDisabled());
        // Project delete cascades to feed sources.
        project.delete();
        DatatoolsTest.resetMTCExtension();
    }

    @ParameterizedTest
    @MethodSource("createNonRevenueCases")
    void testNonRevenueTripRemoval(
        List<String> stopTimesNonRevenue,
        List<String> stopTimesRevenue,
        Map<String, Integer> fieldIndexes
    ) throws IOException {
        final String stopTimesRows = Stream
            .concat(stopTimesNonRevenue.stream(), stopTimesRevenue.stream())
            .collect(Collectors.joining("\n"));

        CsvReader csvReader = new CsvReader(new StringReader(stopTimesRows));
        Map<String, List<String[]>> revenueStopTimes = trans.getAllRevenueStopTimes(
            csvReader,
            fieldIndexes
        );
        csvReader.close();
        assertEquals(1, revenueStopTimes.size());
        assertEquals(stopTimesRevenue.size(), revenueStopTimes.get("revenue-trip").size());
    }

    static Stream<Arguments> createNonRevenueCases() {

        Map<String, Integer> fieldIndexes = new HashMap<>();
        fieldIndexes.put(TRIP_ID_FIELD_NAME, 0);
        fieldIndexes.put(PICKUP_FIELD_NAME, 1);
        fieldIndexes.put(DROP_OFF_FIELD_NAME, 2);

        Map<String, Integer> allFieldIndexes = new HashMap<>();
        allFieldIndexes.put(TRIP_ID_FIELD_NAME, 0);
        allFieldIndexes.put(PICKUP_FIELD_NAME, 1);
        allFieldIndexes.put(DROP_OFF_FIELD_NAME, 2);
        allFieldIndexes.put(CONTINUOUS_PICKUP_FIELD_NAME, 3);
        allFieldIndexes.put(CONTINUOUS_DROP_OFF_FIELD_NAME, 4);

        return Stream.of(
            Arguments.of(
                List.of(
                    "non-revenue-trip1,1,",
                    "non-revenue-trip1,1,1",
                    "non-revenue-trip1,1,1",
                    "non-revenue-trip1,1,"
                ),
                List.of(
                    "revenue-trip,0,0",
                    "revenue-trip,1,1",
                    "revenue-trip,1,1",
                    "revenue-trip,0,0"
                ),
                fieldIndexes
            ),
            Arguments.of(
                List.of(
                    "non-revenue-trip1,1,1,1,1",
                    "non-revenue-trip1,1,1,1,1",
                    "non-revenue-trip1,1,1,,",
                    "non-revenue-trip1,1,1,1,1"
                ),
                // Non revenue pickup and drop off, but revenue continuous pickup/drop off.
                List.of(
                    "revenue-trip,0,0,2,2",
                    "revenue-trip,0,0,1,1",
                    "revenue-trip,0,0,2,2",
                    "revenue-trip,0,0,2,2"
                ),
                allFieldIndexes
            )
        );
    }

    @Test
    void testEndToEndNonRevenueTripRemoval() throws Exception {
        FeedVersion sourceVersion = createFeedVersion(
            feedSource,
            zipFolderFiles("non-revenue-trips")
        );
        hadExpectTransformResults(sourceVersion.feedTransformResult.tableTransformResults);
        assertFalse(hasNonRevenueTrips(sourceVersion.retrieveGtfsFile().getAbsolutePath()));
    }

    @Test
    void testDirectNonRevenueTripRemovalTransformation() throws Exception {
        File zip = zipFolderFiles("non-revenue-trips");
        FeedTransformZipTarget zipTarget = new FeedTransformZipTarget(zip);
        trans.transform(zipTarget, new MonitorableJob.Status());
        hadExpectTransformResults(zipTarget.feedTransformResult.tableTransformResults);
        assertFalse(hasNonRevenueTrips(zipTarget.gtfsFile.getAbsolutePath()));
    }

    private void hadExpectTransformResults(List<TableTransformResult> tableTransformResults) {
        assertEquals(2, tableTransformResults.size());
        assertEquals(4, tableTransformResults.get(0).addedCount);
        assertEquals(1, tableTransformResults.get(1).addedCount);
    }

    private boolean hasNonRevenueTrips(String pathToZip) throws IOException {
        try (ZipFile gtfsZipfile = new ZipFile(pathToZip)) {
            for (String tableName : List.of("stop_times.txt", "trips.txt")) {
                ZipEntry entry = gtfsZipfile.getEntry(tableName);

                if (entry == null) {
                    throw new AssertionError(
                        String.format("Expected table %s not found in outputted zip file.", tableName)
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