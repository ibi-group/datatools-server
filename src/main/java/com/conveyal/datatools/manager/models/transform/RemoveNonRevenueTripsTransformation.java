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
 * This transformation removes non revenue trips from a feed. Establishing trips where _all_ related stop times have
 * "no pickup available" or "no continuous stopping pickup". Then remove these stop times and related trips from the
 * feed. "Non revenue" stops are kept because the transit ops systems use these in the published feed for all trips.
 */
public class RemoveNonRevenueTripsTransformation extends ZipTransformation {
    private static final Logger LOG = LoggerFactory.getLogger(RemoveNonRevenueTripsTransformation.class);

    public static final String TRIP_ID_FIELD_NAME = "trip_id";
    public static final String PICKUP_FIELD_NAME = "pickup_type";
    public static final String DROP_OFF_FIELD_NAME = "drop_off_type";
    public static final String CONTINUOUS_PICKUP_FIELD_NAME = "continuous_pickup";
    public static final String CONTINUOUS_DROP_OFF_FIELD_NAME = "continuous_drop_off";

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
            Field[] fieldsFoundInStopTimes = gtfsTable.getFieldsFromFieldHeaders(headersForStopTime, null);
            Map<String, Integer> fieldIndexes = getFieldIndexes(fieldsFoundInStopTimes);
            if (!hasRequiredFields(fieldIndexes)) {
                status.error = true;
                status.fail("Unable to remove non revenue trips because the stop times file does not contain all required fields.");
                return;
            }

            Map<String, List<String[]>> revenueStopTimes = getAllRevenueStopTimes(
                csvReaderForStopTimes,
                fieldIndexes
            );

            gtfsTable = GtfsUtils.getGtfsTable("trips");
            CsvReader csvReaderForTrips = gtfsTable.getCsvReader(new ZipFile(tempZipPath.toAbsolutePath().toString()), null);
            final String[] headersForTrips = csvReaderForTrips.getHeaders();
            Field[] fieldsFoundInStopTrips = gtfsTable.getFieldsFromFieldHeaders(headersForTrips, null);
            int tripIdFieldIndex = getFieldIndex(fieldsFoundInStopTrips, TRIP_ID_FIELD_NAME);
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

            writeToFile(zipTarget, "trips.txt", revenueTrips, headersForTrips);
        } catch (Exception e) {
            status.fail("Unknown error encountered while attempting to remove non revenue trip from zip file.", e);
        }

    }

    /**
     * Get the index of required fields from available fields in stop times.
     */
    private static Map<String, Integer> getFieldIndexes(Field[] fieldsFoundInStopTimes) {
        Map<String, Integer> fieldIndexes = new HashMap<>();
        fieldIndexes.put(TRIP_ID_FIELD_NAME, getFieldIndex(fieldsFoundInStopTimes, TRIP_ID_FIELD_NAME));
        fieldIndexes.put(PICKUP_FIELD_NAME, getFieldIndex(fieldsFoundInStopTimes, PICKUP_FIELD_NAME));
        fieldIndexes.put(DROP_OFF_FIELD_NAME, getFieldIndex(fieldsFoundInStopTimes, DROP_OFF_FIELD_NAME));
        fieldIndexes.put(CONTINUOUS_PICKUP_FIELD_NAME, getFieldIndex(fieldsFoundInStopTimes, CONTINUOUS_PICKUP_FIELD_NAME));
        fieldIndexes.put(CONTINUOUS_DROP_OFF_FIELD_NAME, getFieldIndex(fieldsFoundInStopTimes, CONTINUOUS_DROP_OFF_FIELD_NAME));
        return fieldIndexes;
    }

    /**
     * The minimum required fields to determine revenue stop times.
     */
    private boolean hasRequiredFields(Map<String, Integer> fieldIndexes) {
        return
            fieldIndexes.get(TRIP_ID_FIELD_NAME) != -1 &&
            fieldIndexes.get(PICKUP_FIELD_NAME) != -1 &&
            fieldIndexes.get(DROP_OFF_FIELD_NAME) != -1;
    }

    /**
     * Check if continuous fields are available. These can be checked if pickup and drop off are non revenue.
     */
    private boolean hasContinuousFields(Map<String, Integer> fieldIndexes) {
        return
            fieldIndexes.get(CONTINUOUS_PICKUP_FIELD_NAME) != null &&
            fieldIndexes.get(CONTINUOUS_DROP_OFF_FIELD_NAME) != null &&
            fieldIndexes.get(CONTINUOUS_PICKUP_FIELD_NAME) != -1 &&
            fieldIndexes.get(CONTINUOUS_DROP_OFF_FIELD_NAME) != -1;
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
     * has a pickup, drop off, continuous pickup or continuous drop off. This will have the effect of removing non
     * revenue trips.
     */
    public Map<String, List<String[]>> getAllRevenueStopTimes(
        CsvReader csvReader,
        Map<String, Integer> fieldIndexes
    ) throws IOException {
        Map<String, List<String[]>> nonRevenueStopTimes = new HashMap<>();
        Map<String, List<String[]>> revenueStopTimes = new HashMap<>();
        while (csvReader.readRecord()) {
            String tripId = csvReader.get(fieldIndexes.get(TRIP_ID_FIELD_NAME));
            String[] currentRecord = csvReader.getValues();

            if (isRevenueStopTime(csvReader, fieldIndexes, hasContinuousFields(fieldIndexes))) {
                // Candidate for revenue trip.
                List<String[]> nonRevenueRecords = nonRevenueStopTimes.remove(tripId);
                if (nonRevenueRecords != null) {
                    // Move all previously flagged non-revenue trips to revenue trips.
                    revenueStopTimes.computeIfAbsent(tripId, k -> new ArrayList<>()).addAll(nonRevenueRecords);
                }
                // Add the current record as a revenue trip.
                revenueStopTimes.computeIfAbsent(tripId, k -> new ArrayList<>()).add(currentRecord);
            } else {
                // Candidate for non revenue trip.
                if (!revenueStopTimes.containsKey(tripId)) {
                    // Only add to non-revenue trips if it's not already marked as a revenue trip.
                    nonRevenueStopTimes.computeIfAbsent(tripId, k -> new ArrayList<>()).add(currentRecord);
                } else {
                    revenueStopTimes.computeIfAbsent(tripId, k -> new ArrayList<>()).add(currentRecord);
                }
            }
        }
        return revenueStopTimes;
    }

    /**
     * Determine if a stop time meets the criteria of a revenue stop time.
     */
    private boolean isRevenueStopTime(
        CsvReader csvReader,
        Map<String, Integer> fieldIndexes,
        boolean hasContinuousFields
    ) throws IOException {
        boolean isRevenue = isRevenuePickupDropOff(csvReader, fieldIndexes);
        if (!isRevenue && hasContinuousFields) {
            isRevenue = isRevenueContinuousPickupDropOff(csvReader, fieldIndexes);
        }
        return isRevenue;
    }

    /**
     * If pickup or drop off is not 1 (No pickup available) it is considered a revenue stop time.
     */
    private boolean isRevenuePickupDropOff(CsvReader csvReader, Map<String, Integer> fieldIndexes) throws IOException {
        String pickup = csvReader.get(fieldIndexes.get(PICKUP_FIELD_NAME));
        String dropOff = csvReader.get(fieldIndexes.get(DROP_OFF_FIELD_NAME));
        return !"1".equals(pickup) && !"1".equals(dropOff);
    }

    /**
     * If continuous pickup or drop off is not empty or 1 (No continuous stopping pickup) it is considered a revenue
     * stop time.
     */
    private boolean isRevenueContinuousPickupDropOff(CsvReader csvReader, Map<String, Integer> fieldIndexes) throws IOException {
        String continuousPickup = csvReader.get(fieldIndexes.get(CONTINUOUS_PICKUP_FIELD_NAME));
        String continuousDropOff = csvReader.get(fieldIndexes.get(CONTINUOUS_DROP_OFF_FIELD_NAME));
        return
            (!continuousPickup.isEmpty() && !"1".equals(continuousPickup)) ||
            (!continuousDropOff.isEmpty() && !"1".equals(continuousDropOff));
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
                FileSystem targetZipFile = FileSystems.newFileSystem(
                    Paths.get(zipTarget.gtfsFile.getAbsolutePath()),
                    (ClassLoader) null
                );
                // Stream for file copy operation.
                InputStream inputStream = new ByteArrayInputStream(stringWriter.toString().getBytes(StandardCharsets.UTF_8))
            ) {
                Files.copy(inputStream, getTablePathInZip(tableName, targetZipFile), StandardCopyOption.REPLACE_EXISTING);
                zipTarget.feedTransformResult.tableTransformResults.add(
                    new TableTransformResult(tableName, rows.size(), 0, 0)
                );
            }
        }
    }

    @Override
    public void validateParameters(MonitorableJob.Status status) {
        // No required.
    }
}