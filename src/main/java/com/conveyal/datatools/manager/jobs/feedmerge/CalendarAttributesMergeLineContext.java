package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.datatools.manager.jobs.MergeFeedsJob;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.loader.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.zip.ZipOutputStream;

import static com.conveyal.datatools.manager.utils.MergeFeedUtils.hasDuplicateError;

/**
 * Holds the logic for merging entries from the GTFS+ calendar_attributes table.
 */
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
        String keyInCalendarTable = getTableScopedValue(Table.CALENDAR, keyValue);
        if (mergeFeedsResult.skippedIds.contains(keyInCalendarTable)) {
            LOG.warn(
                "Skipping calendar entry {} because it was skipped in the merged calendar table.",
                keyValue);
            shouldSkipRecord = true;
        }

        return !shouldSkipRecord;
    }
}