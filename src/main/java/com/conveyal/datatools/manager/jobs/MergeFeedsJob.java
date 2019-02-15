package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.gtfsplus.GtfsPlusTable;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.JdbcGtfsLoader;
import com.conveyal.gtfs.loader.ReferenceTracker;
import com.conveyal.gtfs.loader.Table;
import com.csvreader.CsvReader;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import static com.conveyal.datatools.manager.jobs.MergeFeedsType.MTC;
import static com.conveyal.datatools.manager.jobs.MergeFeedsType.REGIONAL;
import static com.conveyal.datatools.manager.utils.StringUtils.getCleanName;
import static com.conveyal.gtfs.loader.DateField.GTFS_DATE_FORMATTER;
import static com.conveyal.gtfs.loader.JdbcGtfsLoader.getFieldIndex;
import static com.conveyal.gtfs.loader.JdbcGtfsLoader.getKeyFieldIndex;

/**
 * This job handles merging two or more feed versions according to logic specific to the specified merge type.
 * The current merge types handled here are:
 * - {@link MergeFeedsType#REGIONAL}: this is essentially a "dumb" merge. For each feed version, each primary key is
 *                                    scoped so that there is no possibility that it will conflict with other IDs
 *                                    found in any other feed version. Note: There is absoluately no attempt to merge
 *                                    entities based on either expected shared IDs or entity location (e.g., stop
 *                                    coordinates).
 * - {@link MergeFeedsType#MTC}:      this strategy is defined in detail at https://github.com/conveyal/datatools-server/issues/185,
 *                                    but in essence, this strategy attempts to merge a current and future feed into
 *                                    a combined file. For certain entities (specifically stops and routes) it uses
 *                                    alternate fields as primary keys (stop_code and route_short_name) if they are
 *                                    available. There is some complexity related to this in {@link #constructMergedTable(Table, List)}.
 *                                    Another defining characteristic is to prefer entities defined in the "future"
 *                                    file if there are matching entities in the current file.
 * Future merge strategies could be added here. For example, some potential customers have mentioned a desire to
 * prefer entities from the "current" version, so that entities edited in Data Tools would override the values found
 * in the "future" file, which may have limited data attributes due to being exported from scheduling software with
 * limited GTFS support.
 */
public class MergeFeedsJob extends MonitorableJob {

    private static final Logger LOG = LoggerFactory.getLogger(MergeFeedsJob.class);
    public static final ObjectMapper mapper = new ObjectMapper();
    private final Set<FeedVersion> feedVersions;
    private final FeedSource feedSource;
    private final ReferenceTracker referenceTracker = new ReferenceTracker();
    public MergeFeedsResult mergeFeedsResult;
    private final String filename;
    public final String projectId;
    public final MergeFeedsType mergeType;
    private File mergedTempFile = null;
    private final FeedVersion mergedVersion;

    public MergeFeedsJob(String owner, Set<FeedVersion> feedVersions, String file, MergeFeedsType mergeType) {
        super(owner, mergeType.equals(REGIONAL) ? "Merging project feeds" : "Merging feed versions", JobType.MERGE_FEED_VERSIONS);
        this.feedVersions = feedVersions;
        // Grab parent feed source if performing non-regional merge (each version should share the same feed source).
        this.feedSource = mergeType.equals(REGIONAL) ? null : feedVersions.iterator().next().parentFeedSource();
        // Construct full filename with extension
        this.filename = String.format("%s.zip", file);
        // If the merge type is regional, the file string should be equivalent to projectId, which is used by the client
        // to download the merged feed upon job completion.
        this.projectId = mergeType.equals(REGIONAL) ? file : null;
        this.mergeType = mergeType;
        // Assuming job is successful, mergedVersion will contain the resulting feed version.
        this.mergedVersion = mergeType.equals(REGIONAL) ? null : new FeedVersion(this.feedSource);
        this.mergeFeedsResult = new MergeFeedsResult(mergeType);
    }

    /**
     * The final stage handles clean up (deleting temp file) and adding the next job to process the new merged version
     * (assuming the merge did not fail).
     */
    public void jobFinished() {
        // Delete temp file to ensure it does not cause storage bloat. Note: merged file has already been stored
        // permanently.
        if (!mergedTempFile.delete()) {
            // FIXME: send to bugsnag?
            LOG.error("Merged feed file {} not deleted. This may contribute to storage space shortages.", mergedTempFile.getAbsolutePath());
        }
        if (!mergeType.equals(REGIONAL) && !status.error && !mergeFeedsResult.failed) {
            // Handle the processing of the new version for non-regional merges (note: s3 upload is handled within this job).
            ProcessSingleFeedJob processSingleFeedJob = new ProcessSingleFeedJob(mergedVersion, owner, true);
            addNextJob(processSingleFeedJob);
        }
    }

    /**
     * Primary job logic handles collecting and sorting versions, creating a merged table for all versions, and writing
     * the resulting zip file to storage.
     */
    @Override
    public void jobLogic() throws IOException {
        // Create temp zip file to add merged feed content to.
        mergedTempFile = File.createTempFile(filename, null);
        mergedTempFile.deleteOnExit();
        // Create the zipfile.
        ZipOutputStream out = new ZipOutputStream(new FileOutputStream(mergedTempFile));
        LOG.info("Created project merge file: " + mergedTempFile.getAbsolutePath());
        List<FeedToMerge> feedsToMerge = collectAndSortFeeds(feedVersions);

        // Determine which tables to merge (only merge GTFS+ tables for MTC extension).
        final List<Table> tablesToMerge = Arrays.stream(Table.tablesInOrder)
                                                .filter(Table::isSpecTable)
                                                .collect(Collectors.toList());
        if (DataManager.isExtensionEnabled("mtc")) {
            // Merge GTFS+ tables only if MTC extension is enabled. We should do this for both regional and MTC merge
            // strategies.
            tablesToMerge.addAll(Arrays.asList(GtfsPlusTable.tables));
        }
        int numberOfTables = tablesToMerge.size();
        // Loop over GTFS tables and merge each feed one table at a time.
        for (int i = 0; i < numberOfTables; i++) {
            Table table = tablesToMerge.get(i);
            double percentComplete = Math.round((double) i / numberOfTables * 10000d) / 100d;
            status.update( "Merging " + table.name, percentComplete);
            // Perform the merge.
            byte[] tableOut = constructMergedTable(table, feedsToMerge);
            // If at least one feed has the table (i.e., tableOut is not null), include it in the merged feed.
            if (tableOut != null) {
                // Create entry for zip file.
                ZipEntry tableEntry = new ZipEntry(table.name + ".txt");
                LOG.info("Writing {} to merged feed", table.name);
                try {
                    out.putNextEntry(tableEntry);
                    out.write(tableOut);
                    out.closeEntry();
                } catch (IOException e) {
                    String message = String.format("Error writing to table %s", table.name);
                    LOG.error(message, e);
                    status.fail(message, e);
                }
            } else {
                LOG.info("Skipping {} table. No entries found in zip files.", table.name);
            }
        }
        // Close output stream for zip file.
        out.close();
        // Handle writing file to storage (local or s3).
        if (mergeFeedsResult.failed) status.fail("Merging feed versions failed.");
        else {
            storeMergedFeed();
            status.update(false, "Merged feed created successfully.", 100, true);
        }
    }

    /**
     * Collect zipFiles for each feed version before merging tables.
     * Note: feed versions are sorted by first calendar date so that future dataset is iterated over first. This is
     * required for the MTC merge strategy which prefers entities from the future dataset over past entities.
     */
    private List<FeedToMerge> collectAndSortFeeds(Set<FeedVersion> feedVersions) {
        return feedVersions
            .stream()
            .map(version -> {
                try {
                    return new FeedToMerge(version);
                } catch (Exception e) {
                    LOG.error("Could not create zip file for version {}:", version.parentFeedSource(), version.version);
                    return null;
                }
            })
            .filter(Objects::nonNull)
            // MTC-specific sort mentioned in above comment.
            // TODO: If another merge strategy requires a different sort order, a merge type check should be added.
            .sorted(Comparator.comparing(entry -> entry.version.validationResult.firstCalendarDate, Comparator.reverseOrder()))
            .collect(Collectors.toList());
    }

    /**
     * Handles writing the GTFS zip file to disk. For REGIONAL merges, this will end up in a project subdirectory on s3.
     * Otherwise, it will write to a new version.
     */
    private void storeMergedFeed() throws IOException {
        if (mergeType.equals(REGIONAL)) {
            status.update(false, "Saving merged feed.", 95);
            // Store the project merged zip locally or on s3
            if (DataManager.useS3) {
                String s3Key = String.join("/", "project", filename);
                FeedStore.s3Client.putObject(DataManager.feedBucket, s3Key, mergedTempFile);
                LOG.info("Storing merged project feed at s3://{}/{}", DataManager.feedBucket, s3Key);
            } else {
                try {
                    FeedVersion.feedStore.newFeed(filename, new FileInputStream(mergedTempFile), null);
                } catch (IOException e) {
                    e.printStackTrace();
                    LOG.error("Could not store feed for project {}", filename);
                    throw e;
                }
            }
        } else {
            // Store the zip file for the merged feed version.
            try {
                FeedVersion.feedStore.newFeed(mergedVersion.id, new FileInputStream(mergedTempFile), feedSource);
            } catch (IOException e) {
                LOG.error("Could not store merged feed for new version");
                throw e;
            }
        }
    }

    /**
     * Merge the specified table for multiple GTFS feeds.
     * @param table table to merge
     * @param feedsToMerge map of feedSources to zipFiles from which to extract the .txt tables
     * @return single merged table for feeds or null if the table did not exist for any feed
     */
    private byte[] constructMergedTable(Table table, List<FeedToMerge> feedsToMerge) throws IOException {
        String keyField = table.getKeyFieldName();
        String orderField = table.getOrderFieldName();
        if (mergeType.equals(MTC)) {
            // MTC requires that the stop and route records be merged based on different key fields.
            switch (table.name) {
                case "stops":
                    keyField = "stop_code";
                    break;
                case "routes":
                    keyField = "route_short_name";
                    break;
                default:
                    // Otherwise, use the standard key field (see keyField declaration.
                    break;
            }
        }
        // Set up objects for outputting the data and tracking the rows encountered
        ByteArrayOutputStream tableOut = new ByteArrayOutputStream();
        Map<String, String[]> rowValuesForKeys = new HashMap<>();
        Set<String> rowStrings = new HashSet<>();
        int mergedLineNumber = 0;
        // Get the spec fields to export
        List<Field> specFields = table.specFields();
        boolean stopCodeMissingFromFirstTable = false;
        try {
            // Iterate over each zip file.
            for (int f = 0; f < feedsToMerge.size(); f++) {
                mergeFeedsResult.feedCount++;
                // FIXME add check for merge type.
                if (f > 0 && (table.name.equals("agency") || table.name.equals("feed_info")) && mergeType.equals(MTC)) {
                    // Always prefer future file for agency and feed_info tables, which means that we can skip
                    // iterations following the first one.
                    // FIXME: This could cause issues with routes or fares that reference an agency_id that no longer
                    //  exists.
                    LOG.warn("Skipping {} file for feed {}/{} (future file preferred)", table.name, f, feedsToMerge.size());
                    continue;
                }
                FeedToMerge feed = feedsToMerge.get(f);
                FeedVersion version = feed.version;
                FeedSource feedSource = version.parentFeedSource();
                // Generate ID prefix to scope GTFS identifiers to avoid conflicts.
                String idScope = getCleanName(feedSource.name) + version.version;
                CsvReader csvReader = JdbcGtfsLoader.getCsvReader(feed.zipFile, table, null);
                // If csv reader is null, the table was not found in the zip file. There is no need to handle merging
                // this table for the current zip file.
                if (csvReader == null) continue;
                LOG.info("Adding {} table for {}{}", table.name, feedSource.name, version.version);

                Field[] fieldsFoundInZip = JdbcGtfsLoader.getFieldsFromFieldHeaders(csvReader.getHeaders(), table, null);
                List<Field> fieldsFoundList = Arrays.asList(fieldsFoundInZip);
                // Determine the index of the key field for this version's table.
                int keyFieldIndex = getFieldIndex(fieldsFoundInZip, keyField);
                int lineNumber = 0;
                // Iterate over rows in table, writing them to the out file.
                while (csvReader.readRecord()) {
                    String keyValue = csvReader.get(keyFieldIndex);
                    // FIXME add check for merge type?
                    if (mergeType.equals(MTC) && lineNumber == 0 && table.name.equals("stops")) {
                        // For the first line of the stops table, check that the alt. key
                        // field (stop_code) is present. If it is not, revert to the original key field.
                        if (f == 0) {
                            // Check that the first file contains stop_code values.
                            if ("".equals(keyValue)) {
                                LOG.warn("stop_code is not present in file {}/{}. Reverting to stop_id", f, feedsToMerge.size());
                                // If the key value for stop_code is not present, revert to stop_id.
                                keyField = table.getKeyFieldName();
                                keyFieldIndex = JdbcGtfsLoader.getKeyFieldIndex(table, fieldsFoundInZip);
                                keyValue = csvReader.get(keyFieldIndex);
                                stopCodeMissingFromFirstTable = true;
                            }
                        } else {
                            // Check whether stop_code exists for the subsequent files.
                            String firstStopCodeValue = csvReader.get(getFieldIndex(fieldsFoundInZip, "stop_code"));
                            if (stopCodeMissingFromFirstTable && !"".equals(firstStopCodeValue)) {
                                // If stop_code was missing from the first file and exists for the second, we consider
                                // that a failing error.
                                mergeFeedsResult.failed = true;
                                mergeFeedsResult.errorCount++;
                                mergeFeedsResult.failureReasons.add(
                                    "If one stops.txt file contains stop_codes, both feed versions must stop_codes."
                                );
                            }
                        }
                    }
                    boolean skipRecord = false;
                    String[] rowValues = new String[specFields.size()];
                    String[] values = csvReader.getValues();
                    if (values.length == 1) {
                        LOG.warn("Found blank line. Skipping...");
                        continue;
                    }
                    // Piece together the row to write, which should look practically identical to the original
                    // row except for the identifiers receiving a prefix to avoid ID conflicts.
                    for (int v = 0; v < specFields.size(); v++) {
                        Field field = specFields.get(v);
                        // Get index of field from GTFS spec as it appears in feed
                        int index = fieldsFoundList.indexOf(field);
                        String val = csvReader.get(index);
                        // Determine if field is a GTFS identifier.
                        boolean isKeyField = field.isForeignReference() || keyField.equals(field.name);
                        Set<NewGTFSError> idErrors = table.checkReferencesAndUniqueness(keyValue, lineNumber, field, val, referenceTracker, keyField, orderField);
                        // Store values for key fields that have been encountered.
                        switch (table.name) {
                            // case "calendar_dates":
                            case "calendar":
                                // If any service_id in the active feed matches with the future feed, it should be
                                // modified and all associated trip records must also be changed with the modified
                                // service_id.
                                // TODO How can we check that calendar_dates entries are duplicates? I think we would
                                //  need to consider the service_id:exception_type:date as the unique key and include any
                                //  all entries as long as they are unique on this key.
                                for (NewGTFSError error : idErrors) {
                                    if (error.errorType.equals(NewGTFSErrorType.DUPLICATE_ID)) {
                                        String key = String.join(":", table.name, idScope, val);
                                        // Modify service_id and ensure that referencing trips have service_id updated.
                                        val = String.join(":", idScope, val);
                                        mergeFeedsResult.remappedIds.put(key, val);
                                    }
                                }
                                // If a service_id from the active calendar has both the start_date and end_date in the
                                // future, the service will be excluded from the merged file. Records in trips,
                                // calendar_dates, and calendar_attributes referencing this service_id shall also be
                                // removed/ignored. Stop_time records for the ignored trips shall also be removed.
                                if (f > 0) {
                                    int startDateIndex = getFieldIndex(fieldsFoundInZip, "start_date");
                                    LocalDate startDate = LocalDate.parse(csvReader.get(startDateIndex), GTFS_DATE_FORMATTER);
                                    if (startDate.isAfter(LocalDate.now())) {
                                        LOG.warn("Skipping calendar entry {} because it operates in the future.", keyValue);
                                        String key = String.join(":", table.name, idScope, keyValue);
                                        mergeFeedsResult.skippedIds.add(key);
                                        skipRecord = true;
                                        continue;
                                    }
                                    // If a service_id from the active calendar has only the end_date in the future, the
                                    // end_date shall be set to one day prior to the earliest start_date in future
                                    // dataset before appending the calendar record to the merged file.
                                    int endDateIndex = getFieldIndex(fieldsFoundInZip, "end_date");
                                    if (index == endDateIndex) {
                                        LocalDate endDate = LocalDate.parse(csvReader.get(endDateIndex), GTFS_DATE_FORMATTER);
                                        if (endDate.isAfter(LocalDate.now())) {
                                            val = feedsToMerge.get(0).version.validationResult.firstCalendarDate
                                                .minus(1, ChronoUnit.DAYS)
                                                .format(GTFS_DATE_FORMATTER);
                                        }
                                    }
                                }
                                break;
                            case "trips":
                                // trip_ids between active and future datasets must not match. If any trip_id is found
                                // to be matching, the merge should fail with appropriate notification to user with the
                                // cause of the failure. Merge result should include all conflicting trip_ids.
                                for (NewGTFSError error : idErrors) {
                                    if (error.errorType.equals(NewGTFSErrorType.DUPLICATE_ID)) {
                                        mergeFeedsResult.failureReasons.add("Trip ID conflict caused merge failure.");
                                        mergeFeedsResult.addIdConflict(error.badValue);
                                        skipRecord = true;
                                    }
                                }
                                break;
                            case "stops":
                                // When stop_code is included, stop merging will be based on that. If stop_code is not
                                // included, it will be based on stop_id. All stops in future data will be carried
                                // forward and any stops found in active data that are not in the future data shall be
                                // appended. If one of the feed is missing stop_code, merge fails with a notification to
                                // the user with suggestion that the feed with missing stop_code must be fixed with
                                // stop_code.
                            case "routes":
                                String primaryKeyValue = csvReader.get(getKeyFieldIndex(table, fieldsFoundInZip));
                                Set<NewGTFSError> primaryKeyErrors = table.checkReferencesAndUniqueness(primaryKeyValue, lineNumber, field, val, referenceTracker);
                                // Merging will be based on route_short_name in the current and future datasets. All
                                // matching route_short_names between the datasets shall be considered same route. Any
                                // route_short_name in active data not present in the future will be appended to the
                                // future routes file.
                                if (keyField.equals("stop_code") || keyField.equals("route_short_name")) {
                                    for (NewGTFSError error : idErrors) {
                                        if (error.errorType.equals(NewGTFSErrorType.DUPLICATE_ID)) {
                                            // If we encounter a route that shares its short name with a previous route,
                                            // we need to remap its route_id field so that references point to the previous
                                            // route_id.
                                            String currentId = index == 0 ? val : rowValues[0];
                                            String key = String.join(":", table.name, idScope, currentId);
                                            // Extract the route_id value used for the route with matching short name.
                                            String[] strings = rowValuesForKeys.get(val);
                                            String id = strings[0];
                                            if (!id.equals(currentId)) {
                                                // Remap this row's route_id to ensure that referencing trips have their
                                                // route_id updated.
                                                mergeFeedsResult.remappedIds.put(key, id);
                                            }
                                            skipRecord = true;
                                        }
                                    }
                                    // Next check for regular ID conflicts (e.g., on route_id or stop_id) because any
                                    // conflicts here will actually break the feed. This essentially handles the case
                                    // where two routes have different short_names, but share the same route_id. We want
                                    // both of these routes two end up in the merged feed in this case because we're
                                    // matching on short name, so we must modify the route_id.
                                    if (!skipRecord && !referenceTracker.transitIds.contains(String.join(":", keyField, keyValue))) {
                                        for (NewGTFSError error : primaryKeyErrors) {
                                            if (error.errorType.equals(NewGTFSErrorType.DUPLICATE_ID)) {
                                                String key = String.join(":", table.name, idScope, val);
                                                // Modify route_id and ensure that referencing trips have route_id updated.
                                                val = String.join(":", idScope, val);
                                                mergeFeedsResult.remappedIds.put(key, val);
                                            }
                                        }
                                    }
                                } else {
                                    // Key field has defaulted to the standard primary key field (stop_id or route_id),
                                    // which makes the check much simpler (just skip the duplicate record).
                                    for (NewGTFSError error : idErrors) {
                                        if (error.errorType.equals(NewGTFSErrorType.DUPLICATE_ID)) {
                                            skipRecord = true;
                                        }
                                    }
                                }
                                break;
                            case "transfers":
                                // For any other table, skip any duplicate record.
                                for (NewGTFSError error : idErrors) {
                                    if (error.errorType.equals(NewGTFSErrorType.DUPLICATE_ID)) {
                                        skipRecord = true;
                                    }
                                }
                                break;
                            default:
                                // For any other table, skip any duplicate record.
                                for (NewGTFSError error : idErrors) {
                                    if (error.errorType.equals(NewGTFSErrorType.DUPLICATE_ID)) {
                                        skipRecord = true;
                                    }
                                }
                                break;
                        }
                        String valueToWrite = val;
                        if (field.isForeignReference()) {
                            // If the field is a foreign reference, check to see whether the reference has been
                            // remapped due to a conflicting ID from another feed (e.g., calendar#service_id).
                            String key = String.join(":", field.referenceTable.name, idScope, val);
                            if (mergeFeedsResult.remappedIds.containsKey(key)) {
                                mergeFeedsResult.remappedReferences++;
                                // If the value has been remapped update the value to write.
                                valueToWrite = mergeFeedsResult.remappedIds.get(key);
                            }
                            // If the current foreign ref points to another record that has been skipped, skip this
                            // record and add its primary key to the list of skipped IDs (so that other references can
                            // be properly omitted).
                            if (mergeFeedsResult.skippedIds.contains(key)) {
                                String skippedKey = String.join(":", table.name, idScope, keyValue);
                                if (orderField != null) {
                                    skippedKey = String.join(":",
                                                             skippedKey,
                                                             csvReader.get(getFieldIndex(fieldsFoundInZip, orderField))
                                    );
                                }
                                mergeFeedsResult.skippedIds.add(skippedKey);
                                skipRecord = true;
                                continue;
                            }
                        }
                        if (this.mergeType.equals(REGIONAL) && isKeyField && !val.isEmpty()) {
                            // For regional merge, if field is a GTFS identifier (e.g., route_id, stop_id, etc.),
                            // add scoped prefix.
                            valueToWrite = String.join(":", idScope, val);
                        }

                        rowValues[v] = valueToWrite;
                    }
                    if (table.name.equals("routes") && rowValues[0].contains("BART")) {
                        LOG.info("Route record: {}", rowValues[0]);
                    }
                    // Do not write rows that are designated to be skipped.
                    if (skipRecord && this.mergeType.equals(MTC)) {
                        mergeFeedsResult.recordsSkipCount++;
                        continue;
                    }
                    String newLine = String.join(",", rowValues);
                    if (!rowStrings.add(newLine)) {
                        // The line already exists in the output file, do not append it again. This prevents duplicate
                        // entries for certain files that do not contain primary keys (e.g., fare_rules and transfers) and
                        // do not otherwise have convenient ways to track uniqueness (like an order field).
                        // FIXME: add ordinal field/compound keys for transfers (from/to_stop_id) and fare_rules (?).
                        //  Perhaps it makes sense to include all unique fare rules rows, but transfers that share the
                        //  same from/to stop IDs but different transfer times or other values should not both be
                        //  included in the merged feed (yet this strategy would fail to filter those out).
                        mergeFeedsResult.recordsSkipCount++;
                        continue;
                    }
                    // FIXME: This should be revised for tables with order fields, but it should work fine for its
                    //  primary purposes: to detect exact copy rows and to temporarily hold the data in case a reference
                    //  needs to be looked up in order to remap an entity to that key.
                    rowValuesForKeys.put(rowValues[keyFieldIndex], rowValues);
                    if (mergedLineNumber == 0) {
                        // Write headers to table.
                        String headers = specFields.stream().map(field -> field.name).collect(Collectors.joining(","));
                        tableOut.write(headers.getBytes());
                        tableOut.write("\n".getBytes());
                    }
                    // Write line to table (plus new line char).
                    tableOut.write(newLine.getBytes());
                    tableOut.write("\n".getBytes());
                    lineNumber++;
                    mergedLineNumber++;
                }
            }
        } catch (Exception e) {
            LOG.error(
                "Error merging feed sources: {}",
                feedVersions.stream()
                            .map(version -> version.parentFeedSource().name)
                            .collect(Collectors.toList())
                            .toString()
            );
            throw e;
        }
        // If no rows were ever written, return null (there is no need to generate the empty file).
        if (tableOut.size() == 0) return null;
        // Track the number of lines in the merged table.
        mergeFeedsResult.linesPerTable.put(table.name, mergedLineNumber);
        // Otherwise, return the output stream as a byte array.
        return tableOut.toByteArray();
    }

    /**
     * Helper class that collects the feed version and its zip file. Note: this class helps with sorting versions to
     * merge in a list collection.
     */
    private class FeedToMerge {
        public FeedVersion version;
        public ZipFile zipFile;
        FeedToMerge (FeedVersion version) throws IOException {
            this.version = version;
            this.zipFile = new ZipFile(version.retrieveGtfsFile());
        }
    }
}
