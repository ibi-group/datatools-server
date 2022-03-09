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
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.containsField;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.getAllFields;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.getMergeKeyField;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.hasDuplicateError;
import static com.conveyal.datatools.manager.utils.StringUtils.getCleanName;
import static com.conveyal.gtfs.loader.DateField.GTFS_DATE_FORMATTER;

public class MergeLineContext {
    protected static final String AGENCY_ID = "agency_id";
    protected static final String SERVICE_ID = "service_id";
    protected static final String ROUTE_ID = "route_id";
    protected static final String ROUTE_SHORT_NAME = "route_short_name";
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
    private String[] originalRowValues;
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
    private int feedIndex;

    public FeedVersion version;
    public FeedSource feedSource;
    public boolean skipFile;
    public int mergedLineNumber = 0;
    private boolean headersWritten = false;

    public static MergeLineContext create(MergeFeedsJob job, Table table, ZipOutputStream out) throws IOException {
        switch (table.name) {
            case "agency":
                return new AgencyMergeLineContext(job, table, out);
            case "calendar":
                return new CalendarMergeLineContext(job, table, out);
            case "calendar_attributes":
                return new CalendarAttributesMergeLineContext(job, table, out);
            case "calendar_dates":
                return new CalendarDatesMergeLineContext(job, table, out);
            case "routes":
                return new RoutesMergeLineContext(job, table, out);
            case "route_attributes":
                return new RouteAttributesMergeLineContext(job, table, out);
            case "shapes":
                return new ShapesMergeLineContext(job, table, out);
            case "stops":
                return new StopsMergeLineContext(job, table, out);
            case "trips":
            case "timepoints":
                // Use same merge logic to filter out trips in both tables.
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

        idScope = makeIdScope(version);
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

        if (handlingFutureFeed) {
            mergeFeedsResult.serviceIds.addAll(
                job.serviceIdsToCloneRenameAndExtend.stream().map(
                    this::getIdWithScope
                ).collect(Collectors.toSet())
            );
        }
    }

    /**
     * Returns a scoped identifier of the form e.g. FeedName3:some_id
     * (to distinguish an id when used in multiple tables).
     */
    protected String getIdWithScope(String id, String scope) {
        return String.join(":", scope, id);
    }

    /**
     * Shorthand for above using current idScope.
     */
    protected String getIdWithScope(String id) {
        return getIdWithScope(id, idScope);
    }

    public boolean shouldSkipFile() {
        if (handlingActiveFeed && job.mergeType.equals(SERVICE_PERIOD)) {
            // Always prefer the "future" file for the feed_info table, which means
            // we can skip any iterations following the first one.
            return table.name.equals("feed_info");
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

    public boolean checkForeignReferences(FieldContext fieldContext) throws IOException {
        Field field = fieldContext.getField();
        if (field.isForeignReference()) {
            String key = getTableScopedValue(field.referenceTable, fieldContext.getValue());
            // Check if we're performing a service period merge, this ref field is a service_id, and it
            // is not found in the list of service_ids (e.g., it was removed).
            boolean isValidServiceId = mergeFeedsResult.serviceIds.contains(fieldContext.getValueToWrite());

            // If the current foreign ref points to another record that has
            // been skipped or is a ref to a non-existent service_id during a service period merge, skip
            // this record and add its primary key to the list of skipped IDs (so that other references
            // can be properly omitted).
            if (serviceIdHasKeyOrShouldBeSkipped(fieldContext, key, isValidServiceId)) {
                // If a calendar#service_id has been skipped (it's listed in skippedIds), but there were
                // valid service_ids found in calendar_dates, do not skip that record for both the
                // calendar_date and any related trips.
                if (fieldContext.nameEquals(SERVICE_ID) && isValidServiceId) {
                    LOG.warn("Not skipping valid service_id {} for {} {}", fieldContext.getValueToWrite(), table.name, keyValue);
                } else {
                    String skippedKey = getTableScopedValue(keyValue);
                    if (orderField != null) {
                        skippedKey = String.join(":", skippedKey, getCsvValue(orderField));
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

    private boolean serviceIdHasKeyOrShouldBeSkipped(FieldContext fieldContext, String key, boolean isValidServiceId) {
        boolean serviceIdShouldBeSkipped = job.mergeType.equals(SERVICE_PERIOD) &&
            fieldContext.nameEquals(SERVICE_ID) &&
            !isValidServiceId;
        return mergeFeedsResult.skippedIds.contains(key) || serviceIdShouldBeSkipped;
    }


    /**
     * Overridable method whose default behavior below is to skip a record if it creates a duplicate id.
     * @return false, if a failing condition was encountered. true, if everything was ok.
     * @throws IOException Some overrides throw IOException.
     */
    public boolean checkFieldsForMergeConflicts(Set<NewGTFSError> idErrors, FieldContext fieldContext) throws IOException {
        return !hasDuplicateError(idErrors);
    }

    private Set<NewGTFSError> getIdErrors(FieldContext fieldContext) {
        // If analyzing the second feed (active feed), the service_id always gets feed scoped.
        // See https://github.com/ibi-group/datatools-server/issues/244
        String fieldValue = handlingActiveFeed && fieldContext.nameEquals(SERVICE_ID)
            ? fieldContext.getValueToWrite()
            : fieldContext.getValue();

        return referenceTracker.checkReferencesAndUniqueness(keyValue, lineNumber, fieldContext.getField(),
            fieldValue, table, keyField, orderField);
    }

    protected boolean checkRoutesAndStopsIds(Set<NewGTFSError> idErrors, FieldContext fieldContext) throws IOException {
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
            if (hasBlankPrimaryKey(fieldContext)) {
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
                String key = getTableScopedValue(currentPrimaryKey);
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
                updateAndRemapOutput(fieldContext);
            }
        } else {
            // Key field has defaulted to the standard primary key field
            // (stop_id or route_id), which makes the check much
            // simpler (just skip the duplicate record).
            if (hasDuplicateError(idErrors)) {
                shouldSkipRecord = true;
            }
        }

        // Track route ids, so we can remove unused ones later.
        if (!shouldSkipRecord && job.mergeType.equals(SERVICE_PERIOD) && fieldContext.nameEquals(ROUTE_SHORT_NAME)) {
            if (handlingFutureFeed) {
                mergeFeedsResult.routeIds.add(primaryKeyValue);
            } else {
                String scopedKey = getTableScopedValue(primaryKeyValue);
                String mergedRouteId = mergeFeedsResult.remappedIds.get(scopedKey);
                mergeFeedsResult.routeIds.add(mergedRouteId);
            }
        }

        String newAgencyId = getNewAgencyIdForFeed();
        if (newAgencyId != null && fieldContext.nameEquals(AGENCY_ID)) {
            LOG.info(
                "Updating route#agency_id to (auto-generated) {} for route={}",
                newAgencyId, keyValue);
            fieldContext.setValue(newAgencyId);
        }

        return !shouldSkipRecord;
    }

    private boolean hasBlankPrimaryKey(FieldContext fieldContext) {
        return "".equals(keyValue) && fieldContext.nameEquals(table.getKeyFieldName());
    }

    private String getNewAgencyIdForFeed() {
        return (handlingActiveFeed
            ? feedMergeContext.active
            : feedMergeContext.future
        ).getNewAgencyId();
    }

    private boolean useAltKey() {
        return keyField.equals("stop_code") || keyField.equals("route_short_name");
    }

    public boolean updateAgencyIdIfNeeded(FieldContext fieldContext) {
        String newAgencyId = getNewAgencyIdForFeed();
        if (newAgencyId != null && fieldContext.nameEquals(AGENCY_ID) && job.mergeType.equals(REGIONAL)) {
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

    private void updateServiceIdsIfNeeded(FieldContext fieldContext) {
        String fieldValue = fieldContext.getValue();
        if (table.name.equals(Table.TRIPS.name) &&
            fieldContext.nameEquals(SERVICE_ID) &&
            job.serviceIdsToCloneRenameAndExtend.contains(fieldValue) &&
            job.mergeType.equals(SERVICE_PERIOD)
        ) {
            // Future trip ids not in the active feed will not get the service id remapped,
            // they will use the service id as defined in the future feed instead.
            if (!(handlingFutureFeed && feedMergeContext.getFutureTripIdsNotInActiveFeed().contains(keyValue))) {
                String newServiceId = getIdWithScope(fieldValue);
                LOG.info("Updating {}#service_id to (auto-generated) {} for ID {}", table.name, newServiceId, keyValue);
                fieldContext.setValueToWrite(newServiceId);
            }
        }
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

    /**
     * Overridable placeholder for additional processing after writing the current row.
     */
    public void afterRowWrite() throws IOException {
        // Default is to do nothing.
    }

    /**
     * Overridable placeholder for additional processing after processing the table
     * (whether any rows are available or not).
     */
    public void afterTableRecords() throws IOException {
        // Default is to do nothing.
    }

    /**
     * Overridable placeholder for checking internal table references. E.g. parent_station references stop_id. It is
     * illegal to have a self reference within a {@link Table} configuration.
     */
    public void checkFieldsForReferences(FieldContext fieldContext) {
        // Default is to do nothing.
    }

    public void scopeValueIfNeeded(FieldContext fieldContext) {
        boolean isKeyField = fieldContext.getField().isForeignReference() || fieldContext.nameEquals(keyField);
        if (job.mergeType.equals(REGIONAL) && isKeyField && !fieldContext.getValue().isEmpty()) {
            // For regional merge, if field is a GTFS identifier (e.g., route_id,
            // stop_id, etc.), add scoped prefix.
            fieldContext.setValueToWrite(getIdWithScope(fieldContext.getValue()));
        }
    }

    public void initializeRowValues() {
        // Re-initialize skipRecord to false for next row.
        skipRecord = false;
        // Reset the row values (this must happen after the first line is checked).
        rowValues = new String[sharedSpecFields.size()];
        originalRowValues = new String[sharedSpecFields.size()];
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

    private void writeHeaders() throws IOException {
        // Create entry for zip file.
        ZipEntry tableEntry = new ZipEntry(table.name + ".txt");
        out.putNextEntry(tableEntry);
        // Write headers to table.
        String[] headers = sharedSpecFields.stream()
            .map(f -> f.name)
            .toArray(String[]::new);
        writeValuesToTable(headers, false);

        headersWritten = true;
    }

    /**
     * Constructs a new row value.
     * @return false, if a failing condition was encountered. true, if everything was ok.
     */
    public boolean constructRowValues() throws IOException {
        boolean result = true;
        // Piece together the row to write, which should look practically identical to the original
        // row except for the identifiers receiving a prefix to avoid ID conflicts.
        for (int specFieldIndex = 0; specFieldIndex < sharedSpecFields.size(); specFieldIndex++) {
            Field field = sharedSpecFields.get(specFieldIndex);
            // Default value to write is unchanged from value found in csv (i.e. val). Note: if looking to
            // modify the value that is written in the merged file, you must update valueToWrite (e.g.,
            // updating this feed's end_date or accounting for cases where IDs conflict).
            FieldContext fieldContext = new FieldContext(
                field,
                csvReader.get(fieldsFoundList.indexOf(field))
            );
            originalRowValues[specFieldIndex] = fieldContext.getValueToWrite();
            if (!skipRecord) {
                // Handle filling in agency_id if missing when merging regional feeds. If false is returned,
                // the job has encountered a failing condition (the method handles failing the job itself).
                if (!updateAgencyIdIfNeeded(fieldContext)) {
                    result = false;
                }
                // Determine if field is a GTFS identifier (and scope if needed).
                scopeValueIfNeeded(fieldContext);
                // Only need to check for merge conflicts if using MTC merge type because
                // the regional merge type scopes all identifiers by default. Also, the
                // reference tracker will get far too large if we attempt to use it to
                // track references for a large number of feeds (e.g., every feed in New
                // York State).
                if (job.mergeType.equals(SERVICE_PERIOD)) {
                    // Remap service id from active feed to distinguish them
                    // from entries with the same id in the future feed.
                    // See https://github.com/ibi-group/datatools-server/issues/244
                    if (handlingActiveFeed && fieldContext.nameEquals(SERVICE_ID)) {
                        updateAndRemapOutput(fieldContext);
                    }

                    updateServiceIdsIfNeeded(fieldContext);

                    // Store values for key fields that have been encountered and update any key values that need modification due
                    // to conflicts.
                    if (!checkFieldsForMergeConflicts(getIdErrors(fieldContext), fieldContext)) {
                        skipRecord = true;
                        continue;
                    }
                } else if (job.mergeType.equals(REGIONAL)) {
                    // If merging feed versions from different agencies, the reference id is updated to avoid conflicts.
                    // e.g. stop_id becomes Fake_Agency2:123 instead of 123. This method allows referencing fields to be
                    // updated to the newer id.
                    checkFieldsForReferences(fieldContext);
                }

                // If the current field is a foreign reference, check if the reference has been removed in the
                // merged result. If this is the case (or other conditions are met), we will need to skip this
                // record. Likewise, if the reference has been modified, ensure that the value written to the
                // merged result is correctly updated.
                if (!checkForeignReferences(fieldContext)) {
                    skipRecord = true;
                    continue;
                }
                rowValues[specFieldIndex] = fieldContext.getValueToWrite();
            }
        }
        return result;
    }

    private void finishRowAndWriteToZip() throws IOException {
        boolean shouldWriteCurrentRow = true;
        // Do not write rows that are designated to be skipped.
        if (skipRecord && job.mergeType.equals(SERVICE_PERIOD)) {
            mergeFeedsResult.recordsSkipCount++;
            shouldWriteCurrentRow = false;
        }
        // Store row and stop values. If the return value is true, the record has been skipped and we
        // should skip writing the row to the merged table.
        if (storeRowAndStopValues()) {
            shouldWriteCurrentRow = false;
        }

        // Finally, handle writing lines to zip entry.
        if (mergedLineNumber == 0 && !headersWritten) {
            writeHeaders();
        }

        if (shouldWriteCurrentRow) {
            // Write line to table.
            writeValuesToTable(rowValues, true);
        }

        // Optional table-specific additional processing.
        afterRowWrite();
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

    protected int getFieldIndexFromSharedSpecs(String fieldName) {
        return Field.getFieldIndex(sharedSpecFields.toArray(new Field[0]), fieldName);
    }

    /**
     * Generate ID prefix to scope GTFS identifiers to avoid conflicts.
     */
    private String makeIdScope(FeedVersion version) {
        return getCleanName(feedSource.name) + version.version;
    }

    /** Get table-scoped value used for key when remapping references for a particular feed. */
    protected String getTableScopedValue(Table table, String id) {
        return String.join(
            ":",
            table.name,
            idScope,
            id
        );
    }

    /** Shorthand for above using ambient table. */
    protected String getTableScopedValue(String id) {
        return getTableScopedValue(table, id);
    }

    /**
     * Obtains the id scope to use for cloned items.
     * It is set to the id scope corresponding to the future feed.
     */
    protected String getClonedIdScope() {
        return makeIdScope(feedMergeContext.future.feedToMerge.version);
    }

    protected int getFeedIndex() { return feedIndex; }

    protected int getLineNumber() {
        return lineNumber;
    }

    protected String[] getOriginalRowValues() { return originalRowValues; }

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
    protected void updateAndRemapOutput(FieldContext fieldContext, boolean updateKeyValue) {
        String value = fieldContext.getValue();
        String valueToWrite = getIdWithScope(value);
        fieldContext.setValueToWrite(valueToWrite);
        if (updateKeyValue) {
            keyValue = valueToWrite;
        }
        mergeFeedsResult.remappedIds.put(
            getTableScopedValue(value),
            valueToWrite
        );
    }

    /**
     * Shorthand for the above method.
     */
    protected void updateAndRemapOutput(FieldContext fieldContext) {
        updateAndRemapOutput(fieldContext,false);
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

    /**
     * Helper method that determines whether a service id for the
     * current calendar-related table is unused or not.
     */
    protected boolean isServiceIdUnused() {
        boolean isUnused = false;
        FeedContext feedContext = handlingActiveFeed ? feedMergeContext.active : feedMergeContext.future;

        if (feedContext.getServiceIdsToRemove().contains(keyValue)) {
            String activeOrFuture = handlingActiveFeed ? "active" : "future";
            LOG.warn(
                "Skipping {} {} entry {} because it will become unused in the merged feed.",
                activeOrFuture,
                table.name,
                keyValue
            );

            mergeFeedsResult.skippedIds.add(getTableScopedValue(keyValue));

            isUnused = true;
        }

        return isUnused;
    }

    /**
     * Adds a cloned service id for trips with the same signature in both the active & future feeds.
     * The cloned service id spans from the start date in the active feed until the end date in the future feed.
     * If dealing with the calendar table, this will update the start_date field accordingly.
     */
    public void addClonedServiceId() throws IOException {
        if (isHandlingFutureFeed() && job.mergeType.equals(SERVICE_PERIOD)) {
            String originalServiceId = keyValue;
            if (job.serviceIdsToCloneRenameAndExtend.contains(originalServiceId)) {
                String[] clonedValues = getOriginalRowValues().clone();
                String newServiceId = clonedValues[keyFieldIndex] = getIdWithScope(originalServiceId);

                if (table.name.equals(Table.CALENDAR.name)) {
                    // Modify start date only (preserve the end date from the future calendar entry).
                    int startDateIndex = Table.CALENDAR.getFieldIndex("start_date");
                    clonedValues[startDateIndex] = feedMergeContext.active.feed.calendars.get(originalServiceId)
                        .start_date.format(GTFS_DATE_FORMATTER);
                }

                referenceTracker.checkReferencesAndUniqueness(
                    keyValue,
                    getLineNumber(),
                    table.fields[0],
                    newServiceId,
                    table,
                    keyField,
                    table.getOrderFieldName()
                );
                writeValuesToTable(clonedValues, true);
            }
        }
    }
}