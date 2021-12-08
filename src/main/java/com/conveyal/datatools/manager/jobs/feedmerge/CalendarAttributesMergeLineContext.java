package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.datatools.manager.jobs.MergeFeedsJob;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.loader.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.zip.ZipOutputStream;

import static com.conveyal.datatools.manager.jobs.feedmerge.MergeFeedsType.SERVICE_PERIOD;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.getTableScopedValue;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.hasDuplicateError;

public class CalendarAttributesMergeLineContext extends MergeLineContext {
    private static final Logger LOG = LoggerFactory.getLogger(CalendarAttributesMergeLineContext.class);

    public CalendarAttributesMergeLineContext(MergeFeedsJob job, Table table, ZipOutputStream out) throws IOException {
        super(job, table, out);
    }

    @Override
    public boolean checkFieldsForMergeConflicts(Set<NewGTFSError> idErrors, FieldContext fieldContext) {
        return checkCalendarIds(idErrors, fieldContext);
    }

    @Override
    public void afterRowWrite() throws IOException {
        // If the current row is for a calendar service_id that is marked for cloning/renaming, clone the
        // values, change the ID, extend the start/end dates to the feed's full range, and write the
        // additional line to the file.
        addClonedServiceId();
    }

    private boolean checkCalendarIds(Set<NewGTFSError> idErrors, FieldContext fieldContext) {
        boolean shouldSkipRecord = false;

        // If any service_id in the active feed matches with the future
        // feed, it should be modified and all associated trip records
        // must also be changed with the modified service_id.
        // TODO How can we check that calendar_dates entries are
        //  duplicates? I think we would need to consider the
        //  service_id:exception_type:date as the unique key and include any
        //  all entries as long as they are unique on this key.
        if (isHandlingActiveFeed() && hasDuplicateError(idErrors)) {
            // Modify service_id and ensure that referencing trips
            // have service_id updated.
            updateAndRemapOutput(fieldContext);
        }

        // Skip record (based on remapped id if necessary) if it was skipped in the calendar table.
        String keyInCalendarTable = getTableScopedValue(Table.CALENDAR, getIdScope(), keyValue);
        if (mergeFeedsResult.skippedIds.contains(keyInCalendarTable)) {
            LOG.warn(
                "Skipping calendar entry {} because it was skipped in the merged calendar table.",
                keyValue);
            shouldSkipRecord = true;
        }

        return !shouldSkipRecord;
    }

    /**
     * Adds a cloned service id for trips with the same signature in both the active & future feeds.
     * The cloned service id spans from the start date in the active feed until the end date in the future feed.
     * @throws IOException
     */
    public void addClonedServiceId() throws IOException {
        if (isHandlingFutureFeed() && job.mergeType.equals(SERVICE_PERIOD)) {
            String originalServiceId = keyValue;
            if (job.serviceIdsToCloneRenameAndExtend.contains(originalServiceId)) {
                String[] clonedValues = getOriginalRowValues().clone();
                String newServiceId = clonedValues[keyFieldIndex] = String.join(":", getIdScope(), originalServiceId);

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