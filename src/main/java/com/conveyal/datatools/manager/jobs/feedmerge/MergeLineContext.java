package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.datatools.manager.jobs.MergeFeedsJob;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.ReferenceTracker;
import com.conveyal.gtfs.loader.Table;
import com.csvreader.CsvReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static com.conveyal.datatools.manager.jobs.feedmerge.MergeFeedsType.REGIONAL;
import static com.conveyal.datatools.manager.jobs.feedmerge.MergeFeedsType.SERVICE_PERIOD;
import static com.conveyal.datatools.manager.jobs.feedmerge.MergeStrategy.EXTEND_FUTURE;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.containsField;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.getAllFields;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.getMergeKeyField;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.getTableScopedValue;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.hasDuplicateError;
import static com.conveyal.datatools.manager.utils.StringUtils.getCleanName;
import static com.conveyal.gtfs.loader.DateField.GTFS_DATE_FORMATTER;

public class MergeLineContext {
    protected static final String AGENCY_ID = "agency_id";
    protected static final String SERVICE_ID = "service_id";
    private static final Logger LOG = LoggerFactory.getLogger(MergeLineContext.class);
    protected final MergeFeedsJob job;
    private final ZipOutputStream out;
    private final Set<Field> allFields;
    private boolean handlingActiveFeed;
    private boolean handlingFutureFeed;
    private String idScope;
    // CSV writer used to write to zip file.
    private final CsvListWriter writer;
    private CsvReader csvReader;
    private boolean skipRecord;
    protected boolean keyFieldMissing;
    private String[] rowValues;
    private int lineNumber = 0;
    protected final Table table;
    protected FeedToMerge feed;
    protected String keyValue;
    protected final ReferenceTracker referenceTracker = new ReferenceTracker();
    protected String keyField;
    private String orderField;
    protected final MergeFeedsResult mergeFeedsResult;
    protected final FeedMergeContext feedMergeContext;
    protected int keyFieldIndex;
    private Field[] fieldsFoundInZip;
    private List<Field> fieldsFoundList;
    // Set up objects for tracking the rows encountered
    private final Map<String, String[]> rowValuesForStopOrRouteId = new HashMap<>();
    private final Set<String> rowStrings = new HashSet<>();
    private List<Field> sharedSpecFields;
    private FieldContext fieldContext;
    private int feedIndex;

    public FeedVersion version;
    public FeedSource feedSource;
    public boolean skipFile;
    public int mergedLineNumber = 0;

    public static MergeLineContext create(MergeFeedsJob job, Table table, ZipOutputStream out) throws IOException {
        switch (table.name) {
            case "agency":
                return new AgencyMergeLineContext(job, table, out);
            case "calendar":
                return new CalendarMergeLineContext(job, table, out);
            case "calendar_dates":
                return new CalendarDatesMergeLineContext(job, table, out);
            case "routes":
                return new RoutesMergeLineContext(job, table, out);
            case "shapes":
                return new ShapesMergeLineContext(job, table, out);
            case "stops":
                return new StopsMergeLineContext(job, table, out);
            case "trips":
                return new TripsMergeLineContext(job, table, out);
            default:
                return new MergeLineContext(job, table, out);
        }
    }

    protected MergeLineContext(MergeFeedsJob job, Table table, ZipOutputStream out) throws IOException {
        this.job = job;
        this.table = table;
        this.feedMergeContext = job.getFeedMergeContext();
        // Get shared fields between all feeds being merged. This is used to filter the spec fields so that only
        // fields found in the collection of feeds are included in the merged table.
        allFields = getAllFields(feedMergeContext.feedsToMerge, table);
        this.mergeFeedsResult = job.mergeFeedsResult;
        this.writer = new CsvListWriter(new OutputStreamWriter(out), CsvPreference.STANDARD_PREFERENCE);
        this.out = out;
    }

    public void startNewFeed(int feedIndex) throws IOException {
        lineNumber = 0;
        handlingActiveFeed = feedIndex > 0;
        handlingFutureFeed = feedIndex == 0;
        this.feedIndex = feedIndex;
        this.feed = feedMergeContext.feedsToMerge.get(feedIndex);
        this.version = feed.version;
        this.feedSource = version.parentFeedSource();
        keyField = getMergeKeyField(table, job.mergeType);
        orderField = table.getOrderFieldName();
        keyFieldMissing = false;

        // Generate ID prefix to scope GTFS identifiers to avoid conflicts.
        idScope = getCleanName(feedSource.name) + version.version;
        csvReader = table.getCsvReader(feed.zipFile, null);
        // If csv reader is null, the table was not found in the zip file. There is no need
        // to handle merging this table for this zip file.
        // No need to iterate over second (active) file if strategy is to simply extend the future GTFS
        // service to start earlier.
        skipFile = shouldSkipFile();
        if (csvReader == null) {
            skipFile = true;
            LOG.warn("Table {} not found in the zip file for {}{}", table.name, feedSource.name, version.version);
            return;
        }
        fieldsFoundInZip = table.getFieldsFromFieldHeaders(csvReader.getHeaders(), null);
        fieldsFoundList = Arrays.asList(fieldsFoundInZip);
        // Determine the index of the key field for this version's table.
        keyFieldIndex = getFieldIndex(keyField);
        if (keyFieldIndex == -1) {
            LOG.error("No {} field exists for {} table (feed={})", keyField, table.name, version.id);
            keyFieldMissing = true;
            // If there is no agency_id for agency table, create one and ensure that
            // route#agency_id gets set.
        }
    }

    public boolean shouldSkipFile() {
        if (handlingActiveFeed && job.mergeType.equals(SERVICE_PERIOD)) {
            // Always prefer the "future" file for the feed_info table, which means
            // we can skip any iterations following the first one.
            return EXTEND_FUTURE.equals(mergeFeedsResult.mergeStrategy) || table.name.equals("feed_info");
        }
        return false;
    }

    /**
     * Overridable method that determines whether to process rows of the current feed table.
     * @return true by default.
     */
    public boolean shouldProcessRows() {
        return true;
    }

    /**
     * Iterate over all rows in table and write them to the output zip.
     *
     * @return false, if a failing condition was encountered. true, if everything was ok.
     */
    public boolean iterateOverRows() throws IOException {
        // Iterate over rows in table, writing them to the out file.
        while (csvReader.readRecord()) {
            startNewRow();

            if (!shouldProcessRows()) {
                // e.g. If there is a mismatched agency, return immediately.
                return false;
            }

            // If checkMismatchedAgency flagged skipFile, loop back to the while loop. (Note: this is
            // intentional because we want to check all agency ids in the file).
            if (skipFile || lineIsBlank()) continue;
            // Check certain initial conditions on the first line of the file.
            if (lineNumber == 0) {
                checkFirstLineConditions();
            }
            initializeRowValues();
            // Construct row values. If a failure condition was encountered, return.
            if (!constructRowValues()) {
                return false;
            }
            finishRowAndWriteToZip();
        }
        return true;
    }

    public void startNewRow() throws IOException {
        keyValue = csvReader.get(keyFieldIndex);
        // Get the spec fields to export
        List<Field> specFields = table.specFields();
        // Filter the spec fields on the set of fields found in all feeds to be merged.
        sharedSpecFields = specFields.stream()
            .filter(f -> containsField(allFields, f.name))
            .collect(Collectors.toList());
    }

    public boolean checkForeignReferences() throws IOException {
        Field field = fieldContext.getField();
        if (field.isForeignReference()) {
            String key = getTableScopedValue(field.referenceTable, idScope, fieldContext.getValue());
            // Check if we're performing a service period merge, this ref field is a service_id, and it
            // is not found in the list of service_ids (e.g., it was removed).
            boolean isValidServiceId = mergeFeedsResult.serviceIds.contains(fieldContext.getValueToWrite());

            // If the current foreign ref points to another record that has
            // been skipped or is a ref to a non-existent service_id during a service period merge, skip
            // this record and add its primary key to the list of skipped IDs (so that other references
            // can be properly omitted).
            if (serviceIdHasOrShouldBeSkipped(key, isValidServiceId)) {
                // If a calendar#service_id has been skipped (it's listed in skippedIds), but there were
                // valid service_ids found in calendar_dates, do not skip that record for both the
                // calendar_date and any related trips.
                if (fieldNameEquals(SERVICE_ID) && isValidServiceId) {
                    LOG.warn("Not skipping valid service_id {} for {} {}", fieldContext.getValueToWrite(), table.name, keyValue);
                } else {
                    String skippedKey = getTableScopedValue(table, idScope, keyValue);
                    if (orderField != null) {
                        skippedKey = String.join(":", skippedKey,
                            getCsvValue(orderField));
                    }
                    mergeFeedsResult.skippedIds.add(skippedKey);
                    return false;
                }
            }
            // If the field is a foreign reference, check to see whether the reference has been
            // remapped due to a conflicting ID from another feed (e.g., calendar#service_id).
            if (mergeFeedsResult.remappedIds.containsKey(key)) {
                mergeFeedsResult.remappedReferences++;
                // If the value has been remapped update the value to write.
                fieldContext.setValueToWrite(mergeFeedsResult.remappedIds.get(key));
            }
        }
        return true;
    }

    private boolean serviceIdHasOrShouldBeSkipped(String key, boolean isValidServiceId) {
        boolean serviceIdShouldBeSkipped = job.mergeType.equals(SERVICE_PERIOD) &&
            fieldNameEquals(SERVICE_ID) &&
            !isValidServiceId;
        return mergeFeedsResult.skippedIds.contains(key) || serviceIdShouldBeSkipped;
    }


    /**
     * Overridable method whose default behavior below is to skip a record if it creates a duplicate id.
     * @return false, if a failing condition was encountered. true, if everything was ok.
     * @throws IOException Some overrides throw IOException.
     */
    public boolean checkFieldsForMergeConflicts(Set<NewGTFSError> idErrors) throws IOException {
        return !hasDuplicateError(idErrors);
    }

    private Set<NewGTFSError> getIdErrors() {
        // If analyzing the second feed (active feed), the service_id always gets feed scoped.
        // See https://github.com/ibi-group/datatools-server/issues/244
        String fieldValue = handlingActiveFeed && fieldNameEquals(SERVICE_ID)
            ? fieldContext.getValueToWrite()
            : fieldContext.getValue();

        return referenceTracker.checkReferencesAndUniqueness(keyValue, lineNumber, fieldContext.getField(),
            fieldValue, table, keyField, orderField);
    }

    protected boolean checkRoutesAndStopsIds(Set<NewGTFSError> idErrors) throws IOException {
        boolean shouldSkipRecord = false;
        // First, check uniqueness of primary key value (i.e., stop or route ID)
        // in case the stop_code or route_short_name are being used. This
        // must occur unconditionally because each record must be tracked
        // by the reference tracker.
        String primaryKeyValue = csvReader.get(getKeyFieldIndex());
        Set<NewGTFSError> primaryKeyErrors = referenceTracker
            .checkReferencesAndUniqueness(primaryKeyValue, lineNumber, fieldContext.getField(), fieldContext.getValue(), table);
        // Merging will be based on route_short_name/stop_code in the active and future datasets. All
        // matching route_short_names/stop_codes between the datasets shall be considered same route/stop. Any
        // route_short_name/stop_code in active data not present in the future will be appended to the
        // future routes/stops file.
        if (useAltKey()) {
            if (hasBlankPrimaryKey()) {
                // If alt key is empty (which is permitted) and primary key is duplicate, skip
                // checking of alt key dupe errors/re-mapping values and
                // simply use the primary key (route_id/stop_id).
                //
                // Otherwise, allow the record to be written in output.
                if (hasDuplicateError(primaryKeyErrors)) {
                    shouldSkipRecord = true;
                }
            } else if (hasDuplicateError(idErrors)) {
                // If we encounter a route/stop that shares its alt.
                // ID with a previous route/stop, we need to
                // remap its route_id/stop_id field so that
                // references point to the previous
                // route_id/stop_id. For example,
                // route_short_name in both feeds is "ABC" but
                // each route has a different route_id (123 and
                // 456). This block will map references to 456 to
                // 123 so that ABC/123 is the route of record.
                ////////////////////////////////////////////////////////
                // Get current route/stop ID. (Note: primary
                // ID index is always zero because we're
                // iterating over the spec fields).
                String currentPrimaryKey = rowValues[0];
                // Get unique key to check for remapped ID when
                // writing values to file.
                String key = getTableScopedValue(table, idScope, currentPrimaryKey);
                // Extract the route/stop ID value used for the
                // route/stop with already encountered matching
                // short name/stop code.
                String[] strings = rowValuesForStopOrRouteId.get(
                    String.join(":", keyField, fieldContext.getValue())
                );
                String keyForMatchingAltId = strings[0];
                if (!keyForMatchingAltId.equals(currentPrimaryKey)) {
                    // Remap this row's route_id/stop_id to ensure
                    // that referencing entities (trips, stop_times)
                    // have their references updated.
                    mergeFeedsResult.remappedIds.put(key, keyForMatchingAltId);
                }
                shouldSkipRecord = true;
            }
            // Next check for regular ID conflicts (e.g., on route_id or stop_id) because any
            // conflicts here will actually break the feed. This essentially handles the case
            // where two routes have different short_names, but share the same route_id. We want
            // both of these routes to end up in the merged feed in this case because we're
            // matching on short name, so we must modify the route_id.
            if (
                !shouldSkipRecord &&
                !referenceTracker.transitIds.contains(String.join(":", keyField, keyValue)) &&
                hasDuplicateError(primaryKeyErrors)
            ) {
                // Modify route_id and ensure that referencing trips
                // have route_id updated.
                updateAndRemapOutput();
            }
        } else {
            // Key field has defaulted to the standard primary key field
            // (stop_id or route_id), which makes the check much
            // simpler (just skip the duplicate record).
            if (hasDuplicateError(idErrors)) {
                shouldSkipRecord = true;
            }
        }

        String newAgencyId = getNewAgencyIdForFeed();

        if (newAgencyId != null && fieldNameEquals(AGENCY_ID)) {
            LOG.info(
                "Updating route#agency_id to (auto-generated) {} for route={}",
                newAgencyId, keyValue);
            fieldContext.setValue(newAgencyId);
        }

        return !shouldSkipRecord;
    }

    private String getNewAgencyIdForFeed() {
        return (handlingActiveFeed
            ? feedMergeContext.active
            : feedMergeContext.future
        ).getNewAgencyId();
    }

    private boolean hasBlankPrimaryKey() {
        return "".equals(keyValue) && fieldNameEquals(table.getKeyFieldName());
    }

    private boolean useAltKey() {
        return keyField.equals("stop_code") || keyField.equals("route_short_name");
    }

    public boolean updateAgencyIdIfNeeded() {
        String newAgencyId = getNewAgencyIdForFeed();
        if (newAgencyId != null && fieldNameEquals(AGENCY_ID) && job.mergeType.equals(REGIONAL)) {
            if (fieldContext.getValue().equals("") && table.name.equals("agency") && lineNumber > 0) {
                // If there is no agency_id value for a second (or greater) agency
                // record, return null which will trigger a failed merge feed job.
                job.failMergeJob(String.format(
                    "Feed %s has multiple agency records but no agency_id values.",
                    feed.version.id
                ));
                return false;
            }
            LOG.info("Updating {}#agency_id to (auto-generated) {} for ID {}", table.name, newAgencyId, keyValue);
            fieldContext.setValue(newAgencyId);
        }
        return true;
    }

    public boolean storeRowAndStopValues() {
        String newLine = String.join(",", rowValues);
        switch (table.name) {
            // Store row values for route or stop ID (or alternative ID field) in order
            // to check for ID conflicts. NOTE: This is only intended to be used for
            // routes and stops. Otherwise, this might (will) consume too much memory.
            case "stops":
            case "routes":
                // FIXME: This should be revised for tables with order fields, but it should work fine for its
                //  primary purposes: to detect exact copy rows and to temporarily hold the data in case a reference
                //  needs to be looked up in order to remap an entity to that key.
                // Here we need to get the key field index according to the spec
                // table definition. Otherwise, if we use the keyFieldIndex variable
                // defined above, we will be using the found fields index, which will
                // cause major issues when trying to put and get values into the
                // below map.
                int fieldIndex = Field.getFieldIndex(sharedSpecFields.toArray(new Field[0]), keyField);
                String key = String.join(":", keyField, rowValues[fieldIndex]);
                rowValuesForStopOrRouteId.put(key, rowValues);
                break;
            case "transfers":
            case "fare_rules":
            case "directions": // GTFS+ table
                if (!rowStrings.add(newLine)) {
                    // The line already exists in the output file, do not append it again. This prevents duplicate
                    // entries for certain files that do not contain primary keys (e.g., fare_rules and transfers) and
                    // do not otherwise have convenient ways to track uniqueness (like an order field).
                    // FIXME: add ordinal field/compound keys for transfers (from/to_stop_id) and fare_rules (?).
                    //  Perhaps it makes sense to include all unique fare rules rows, but transfers that share the
                    //  same from/to stop IDs but different transfer times or other values should not both be
                    //  included in the merged feed (yet this strategy would fail to filter those out).
                    mergeFeedsResult.recordsSkipCount++;
                    return true;
                }
                break;
            default:
                // Do nothing.
                break;
        }
        return false;
    }

    /**
     * Overridable placeholder for checking the first line of a file.
     */
    public void checkFirstLineConditions() throws IOException {
        // Default is to do nothing.
    }

    public void scopeValueIfNeeded() {
        boolean isKeyField = fieldContext.getField().isForeignReference() || fieldNameEquals(keyField);
        if (job.mergeType.equals(REGIONAL) && isKeyField && !fieldContext.getValue().isEmpty()) {
            // For regional merge, if field is a GTFS identifier (e.g., route_id,
            // stop_id, etc.), add scoped prefix.
            fieldContext.setValueToWrite(String.join(":", idScope, fieldContext.getValue()));
        }
    }

    public void initializeRowValues() {
        // Re-initialize skipRecord to false for next row.
        skipRecord = false;
        // Reset the row values (this must happen after the first line is checked).
        rowValues = new String[sharedSpecFields.size()];
    }

    public void addClonedServiceId() throws IOException {
        if ((table.name.equals("calendar")) && job.serviceIdsToCloneAndRename.contains(rowValues[keyFieldIndex])) {
            // FIXME: Do we need to worry about calendar_dates?
            String[] clonedValues = rowValues.clone();
            String newServiceId = clonedValues[keyFieldIndex] = String.join(":", idScope, rowValues[keyFieldIndex]);
            // Modify start/end date.
            int startDateIndex = Table.CALENDAR.getFieldIndex("start_date");
            int endDateIndex = Table.CALENDAR.getFieldIndex("end_date");
            clonedValues[startDateIndex] = feed.version.validationResult.firstCalendarDate.format(GTFS_DATE_FORMATTER);
            clonedValues[endDateIndex] = feed.version.validationResult.lastCalendarDate.format(GTFS_DATE_FORMATTER);
            referenceTracker.checkReferencesAndUniqueness(
                keyValue,
                lineNumber,
                table.fields[0],
                newServiceId,
                table,
                keyField,
                orderField
            );
            writeValuesToTable(clonedValues, true);
        }
    }

    public void writeValuesToTable(String[] values, boolean incrementLineNumbers) throws IOException {
        writer.write(values);
        if (incrementLineNumbers) {
            lineNumber++;
            mergedLineNumber++;
        }
    }

    public void flushAndClose() throws IOException {
        writer.flush();
        out.closeEntry();
    }

    public void writeHeaders() throws IOException {
        // Create entry for zip file.
        ZipEntry tableEntry = new ZipEntry(table.name + ".txt");
        out.putNextEntry(tableEntry);
        // Write headers to table.
        String[] headers = sharedSpecFields.stream()
            .map(f -> f.name)
            .toArray(String[]::new);
        writeValuesToTable(headers, false);
    }

    /**
     * Constructs a new row value.
     * @return false, if a failing condition was encountered. true, if everything was ok.
     */
    public boolean constructRowValues() throws IOException {
        // Piece together the row to write, which should look practically identical to the original
        // row except for the identifiers receiving a prefix to avoid ID conflicts.
        for (int specFieldIndex = 0; specFieldIndex < sharedSpecFields.size(); specFieldIndex++) {
            Field field = sharedSpecFields.get(specFieldIndex);
            // Default value to write is unchanged from value found in csv (i.e. val). Note: if looking to
            // modify the value that is written in the merged file, you must update valueToWrite (e.g.,
            // updating this feed's end_date or accounting for cases where IDs conflict).
            fieldContext = new FieldContext(
                field,
                csvReader.get(fieldsFoundList.indexOf(field))
            );
            // Handle filling in agency_id if missing when merging regional feeds. If false is returned,
            // the job has encountered a failing condition (the method handles failing the job itself).
            if (!updateAgencyIdIfNeeded()) {
                return false;
            }
            // Determine if field is a GTFS identifier (and scope if needed).
            scopeValueIfNeeded();
            // Only need to check for merge conflicts if using MTC merge type because
            // the regional merge type scopes all identifiers by default. Also, the
            // reference tracker will get far too large if we attempt to use it to
            // track references for a large number of feeds (e.g., every feed in New
            // York State).
            if (job.mergeType.equals(SERVICE_PERIOD)) {
                // Remap service id from active feed to distinguish them
                // from entries with the same id in the future feed.
                // See https://github.com/ibi-group/datatools-server/issues/244
                if (handlingActiveFeed && fieldNameEquals(SERVICE_ID)) {
                    updateAndRemapOutput();
                }

                // Store values for key fields that have been encountered and update any key values that need modification due
                // to conflicts.
                // This method can change skipRecord.
                if (!checkFieldsForMergeConflicts(getIdErrors())) {
                    skipRecord = true;
                    break;
                }
            }
            // If the current field is a foreign reference, check if the reference has been removed in the
            // merged result. If this is the case (or other conditions are met), we will need to skip this
            // record. Likewise, if the reference has been modified, ensure that the value written to the
            // merged result is correctly updated.
            if (!checkForeignReferences()) {
                skipRecord = true;
                break;
            }
            rowValues[specFieldIndex] = fieldContext.getValueToWrite();
        }
        return true;
    }

    public void finishRowAndWriteToZip() throws IOException {
        // Do not write rows that are designated to be skipped.
        if (skipRecord && job.mergeType.equals(SERVICE_PERIOD)) {
            mergeFeedsResult.recordsSkipCount++;
            return;
        }
        // Store row and stop values. If the return value is true, the record has been skipped and we
        // should skip writing the row to the merged table.
        if (storeRowAndStopValues()) {
            return;
        }
        // Finally, handle writing lines to zip entry.
        if (mergedLineNumber == 0) {
            writeHeaders();
        }
        // Write line to table.
        writeValuesToTable(rowValues, true);
        // If the current row is for a calendar service_id that is marked for cloning/renaming, clone the
        // values, change the ID, extend the start/end dates to the feed's full range, and write the
        // additional line to the file.
        addClonedServiceId();
    }

    public boolean lineIsBlank() throws IOException {
        if (csvReader.getValues().length == 1) {
            LOG.warn("Found blank line. Skipping...");
            return true;
        }
        return false;
    }

    public boolean isHandlingActiveFeed() {
        return handlingActiveFeed;
    }

    public boolean isHandlingFutureFeed() {
        return handlingFutureFeed;
    }

    protected CsvReader getCsvReader() {
        return csvReader;
    }

    protected int getFieldIndex(String fieldName) {
        return Field.getFieldIndex(fieldsFoundInZip, fieldName);
    }

    protected String getIdScope() {
        return idScope;
    }

    protected FieldContext getFieldContext() {
        return fieldContext;
    }

    protected int getFeedIndex() { return feedIndex; }

    protected boolean fieldNameEquals(String value) {
        return fieldContext.getField().name.equals(value);
    }

    protected int getLineNumber() {
        return lineNumber;
    }

    /**
     * Retrieves the value for the specified CSV field.
     */
    protected String getCsvValue(String fieldName) throws IOException {
        int fieldIndex = getFieldIndex(fieldName);
        return csvReader.get(fieldIndex);
    }

    /**
     * Retrieves the value for the specified CSV field as {@link LocalDate}.
     */
    protected LocalDate getCsvDate(String fieldName) throws IOException {
        return LocalDate.parse(getCsvValue(fieldName), GTFS_DATE_FORMATTER);
    }

    /**
     * Updates output for the current field and remaps the record id.
     */
    protected void updateAndRemapOutput(boolean updateKeyValue) {
        String value = fieldContext.getValue();
        String valueToWrite = String.join(":", idScope, value);
        fieldContext.setValueToWrite(valueToWrite);
        if (updateKeyValue) {
            keyValue = valueToWrite;
        }
        mergeFeedsResult.remappedIds.put(
            getTableScopedValue(table, idScope, value),
            valueToWrite
        );
    }

    /**
     * Shorthand for the above method.
     */
    protected void updateAndRemapOutput() {
        updateAndRemapOutput(false);
    }

    /**
     * Add the specified field once record reading has started.
     */
    protected void addField(Field field) {
        List<Field> fieldsList = new ArrayList<>(Arrays.asList(fieldsFoundInZip));
        fieldsList.add(field);
        fieldsFoundInZip = fieldsList.toArray(fieldsFoundInZip);
        allFields.add(field);
        fieldsFoundList = Arrays.asList(fieldsFoundInZip);
    }

    /**
     * Helper method to get the key field position.
     */
    protected int getKeyFieldIndex() {
        return table.getKeyFieldIndex(fieldsFoundInZip);
    }
}