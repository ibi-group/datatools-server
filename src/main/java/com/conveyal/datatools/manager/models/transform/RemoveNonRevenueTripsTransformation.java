package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.TableTransformResult;
import com.conveyal.datatools.manager.utils.GtfsUtils;
import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.Table;
import com.csvreader.CsvReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipFile;

import static com.conveyal.gtfs.loader.Field.getFieldIndex;

/**
 * This transformation removes non revenue trips from a feed by:
 * - Establishing trips where _all_ related stop times have continuous_pickup and continuous_drop_off types set to 1
 * (or empty) "No pickup available",
 * - Then remove these stop times and related trips the feed.
 * "Non revenue" stops are kept because the transit ops systems use these stops in the published feed for all trips.
 */
public class RemoveNonRevenueTripsTransformation extends ZipTransformation {
    private static final Logger LOG = LoggerFactory.getLogger(RemoveNonRevenueTripsTransformation.class);

    private static final String TRIP_ID_FIELD_NAME = "trip_id";
    private static final String CONTINUOUS_PICKUP_FIELD_NAME = "continuous_pickup";
    private static final String CONTINUOUS_DROP_OFF_FIELD_NAME = "continuous_drop_off";

    /** no-arg constructor for de/serialization */
    public RemoveNonRevenueTripsTransformation() {
    }

    @Override
    public void transform(FeedTransformZipTarget zipTarget, MonitorableJob.Status status) throws Exception {
        try {
            Path tempZipPath = Files.createTempFile("non-revenue-trips-transform", ".zip");
            Path originalZipPath = Paths.get(zipTarget.gtfsFile.getAbsolutePath());
            // Create a temporary working zip file from original.
            Files.copy(originalZipPath, tempZipPath, StandardCopyOption.REPLACE_EXISTING);

            Table gtfsTable = GtfsUtils.getGtfsTable("stop_times");
            CsvReader csvReaderForStopTimes = gtfsTable.getCsvReader(new ZipFile(tempZipPath.toAbsolutePath().toString()), null);
            final String[] headersForStopTime = csvReaderForStopTimes.getHeaders();
            Field[] fieldsFoundInZip = gtfsTable.getFieldsFromFieldHeaders(headersForStopTime, null);
            int tripIdFieldIndex = getFieldIndex(fieldsFoundInZip, TRIP_ID_FIELD_NAME);
            int continuousPickupFieldIndex = getFieldIndex(fieldsFoundInZip, CONTINUOUS_PICKUP_FIELD_NAME);
            int continuousDropOffFieldIndex = getFieldIndex(fieldsFoundInZip, CONTINUOUS_DROP_OFF_FIELD_NAME);
            if (tripIdFieldIndex == -1 || continuousPickupFieldIndex == -1 || continuousDropOffFieldIndex == -1) {
                status.error = true;
                status.fail("Unable to remove non revenue trips because the stop times file does not contain all required fields.");
                return;
            }

            Map<String, List<String[]>> revenueStopTimes = getAllRevenueStopTimes(
                csvReaderForStopTimes,
                tripIdFieldIndex,
                continuousPickupFieldIndex,
                continuousDropOffFieldIndex
            );

            gtfsTable = GtfsUtils.getGtfsTable("trips");
            CsvReader csvReaderForTrips = gtfsTable.getCsvReader(new ZipFile(tempZipPath.toAbsolutePath().toString()), null);
            final String[] headersForTrips = csvReaderForTrips.getHeaders();
            fieldsFoundInZip = gtfsTable.getFieldsFromFieldHeaders(headersForTrips, null);
            tripIdFieldIndex = getFieldIndex(fieldsFoundInZip, TRIP_ID_FIELD_NAME);
            if (tripIdFieldIndex == -1) {
                status.error = true;
                status.fail("Unable to remove non revenue trips because the trips file does not contain the required trip id field.");
                return;
            }
            List<String[]> revenueTrips = getAllRevenueTrips(csvReaderForTrips, revenueStopTimes.keySet(), tripIdFieldIndex);
            if (revenueTrips.isEmpty()) {
                LOG.warn("No revenue trips! All trips and related stop times will be removed.");
            }

            writeToFile(
                zipTarget,
                "stop_times.txt",
                revenueStopTimes.values().stream().flatMap(List::stream).collect(Collectors.toList()),
                headersForStopTime
            );

            writeToFile(
                zipTarget,
                "trips.txt",
                revenueTrips,
                headersForTrips
            );
        } catch (Exception e) {
            status.fail("Unknown error encountered while transforming zip file", e);
        }

    }

    /**
     * Get all trips that have revenue stop times.
     */
    public List<String[]> getAllRevenueTrips(
        CsvReader csvReader,
        Set<String> revenueTrips,
        int tripIdFieldIndex
    ) throws IOException {
        List<String[]> revenueTripRows = new ArrayList<>();
        while (csvReader.readRecord()) {
            if (revenueTrips.contains(csvReader.get(tripIdFieldIndex))) {
                revenueTripRows.add(csvReader.getValues());
            }
        }
        return revenueTripRows;
    }

    /**
     * Get only the trips (and related rows) for revenue stop times. These are stop times were at least one stop time
     * has a continuous pickup or drop off. This will have the effect off removing non revenue trips.
     */
    public Map<String, List<String[]>> getAllRevenueStopTimes(
        CsvReader csvReader,
        int tripIdFieldIndex,
        int continuousPickupFieldIndex,
        int continuousDropOffFieldIndex
    ) throws IOException {
        Map<String, List<String[]>> nonRevenueTrips = new HashMap<>();
        Map<String, List<String[]>> revenueTrips = new HashMap<>();
        while (csvReader.readRecord()) {
            String tripId = csvReader.get(tripIdFieldIndex);
            String pickup = csvReader.get(continuousPickupFieldIndex);
            String dropOff = csvReader.get(continuousDropOffFieldIndex);
            String[] currentRecord = csvReader.getValues();

            if (("1".equals(pickup) || pickup.isEmpty()) && ("1".equals(dropOff) || dropOff.isEmpty())) {
                // Candidate for non revenue trip.
                if (!revenueTrips.containsKey(tripId)) {
                    // Only add to non-revenue trips if it's not already marked as a revenue trip.
                    nonRevenueTrips.computeIfAbsent(tripId, k -> new ArrayList<>()).add(currentRecord);
                } else {
                    revenueTrips.computeIfAbsent(tripId, k -> new ArrayList<>()).add(currentRecord);
                }
            } else {
                // Candidate for revenue trip.
                List<String[]> nonRevenueRecords = nonRevenueTrips.remove(tripId);
                if (nonRevenueRecords != null) {
                    // Move all previously flagged non-revenue trips to revenue trips.
                    revenueTrips.computeIfAbsent(tripId, k -> new ArrayList<>()).addAll(nonRevenueRecords);
                }
                // Add the current record as a revenue trip.
                revenueTrips.computeIfAbsent(tripId, k -> new ArrayList<>()).add(currentRecord);
            }
        }
        return revenueTrips;
    }

    /**
     * Write the updated content to file.
     */
    private void writeToFile(
        FeedTransformZipTarget zipTarget,
        String tableName,
        List<String[]> rows,
        String[] headers
    ) throws IOException {
        try(
            StringWriter stringWriter = new StringWriter();
            CsvListWriter writer = new CsvListWriter(stringWriter, CsvPreference.STANDARD_PREFERENCE)
        ) {
            writer.write(headers);
            if (!rows.isEmpty()) {
                for (String[] row : rows) {
                    writer.write(row);
                }
            }
            writer.flush();

            // Copy csv input stream into the zip file, replacing the existing file.
            try (
                // Modify target zip file that we just read.
                FileSystem targetZipFs = FileSystems.newFileSystem(Paths.get(zipTarget.gtfsFile.getAbsolutePath()), (ClassLoader) null);
                // Stream for file copy operation.
                InputStream inputStream = new ByteArrayInputStream(stringWriter.toString().getBytes(StandardCharsets.UTF_8))
            ) {
                Path targetTxtFilePath = getTablePathInZip(tableName, targetZipFs);
                Files.copy(inputStream, targetTxtFilePath, StandardCopyOption.REPLACE_EXISTING);
                zipTarget.feedTransformResult.tableTransformResults.add(
                    new TableTransformResult(tableName, 0, 0, 0)
                );
            }
        }
    }

    @Override
    public void validateParameters(MonitorableJob.Status status) {

    }
}