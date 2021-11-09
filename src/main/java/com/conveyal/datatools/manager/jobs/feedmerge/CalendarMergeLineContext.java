package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.datatools.manager.jobs.MergeFeedsJob;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.loader.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.zip.ZipOutputStream;

import static com.conveyal.datatools.manager.jobs.feedmerge.MergeStrategy.CHECK_STOP_TIMES;
import static com.conveyal.datatools.manager.jobs.feedmerge.MergeStrategy.EXTEND_FUTURE;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.getTableScopedValue;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.hasDuplicateError;
import static com.conveyal.gtfs.loader.DateField.GTFS_DATE_FORMATTER;

public class CalendarMergeLineContext extends MergeLineContext {
    private static final Logger LOG = LoggerFactory.getLogger(CalendarMergeLineContext.class);

    public CalendarMergeLineContext(MergeFeedsJob job, Table table, ZipOutputStream out) throws IOException {
        super(job, table, out);
    }

    @Override
    public void checkFieldsForMergeConflicts(Set<NewGTFSError> idErrors) throws IOException {
        checkCalendarIds(idErrors);
    }

    private void checkCalendarIds(Set<NewGTFSError> idErrors) throws IOException {
        // If any service_id in the active feed matches with the future
        // feed, it should be modified and all associated trip records
        // must also be changed with the modified service_id.
        // TODO How can we check that calendar_dates entries are
        //  duplicates? I think we would need to consider the
        //  service_id:exception_type:date as the unique key and include any
        //  all entries as long as they are unique on this key.

        if (hasDuplicateError(idErrors)) {
            // Modify service_id and ensure that referencing trips
            // have service_id updated.
            updateAndRemapOutput();
        }

        LocalDate startDate = getCsvDate("start_date");
        if (isHandlingFutureFeed()) {
            // For the future feed, check if the calendar's start date is earlier than the
            // previous earliest value and update if so.
            if (feedMergeContext.getFutureFirstCalendarStartDate().isAfter(startDate)) {
                feedMergeContext.setFutureFirstCalendarStartDate(startDate);
            }
            // FIXME: Move this below so that a cloned service doesn't get prematurely
            //  modified? (do we want the cloned record to have the original values?)
            if (shouldUpdateFutureFeedStartDate()) {
                // Update start_date to extend service through the active feed's
                // start date if the merge strategy dictates. The justification for this logic is that the active feed's
                // service_id will be modified to a different unique value and the trips shared between the future/active
                // service are exactly matching.
                getFieldContext().resetValue(feedMergeContext.activeFeedFirstDate.format(GTFS_DATE_FORMATTER));
            }
        } else {
            // If a service_id from the active calendar has both the
            // start_date and end_date in the future, the service will be
            // excluded from the merged file. Records in trips,
            // calendar_dates, and calendar_attributes referencing this
            // service_id shall also be removed/ignored. Stop_time records
            // for the ignored trips shall also be removed.
            LocalDate futureFeedFirstDate = feedMergeContext.getFutureFeedFirstDate();
            if (!startDate.isBefore(futureFeedFirstDate)) {
                LOG.warn(
                    "Skipping calendar entry {} because it operates fully within the time span of future feed.",
                    keyValue);
                String key = getTableScopedValue(table, getIdScope(), keyValue);
                mergeFeedsResult.skippedIds.add(key);
                skipRecord = true;
            } else {
                // If a service_id from the active calendar has only the
                // end_date in the future, the end_date shall be set to one
                // day prior to the earliest start_date in future dataset
                // before appending the calendar record to the merged file.
                if (fieldNameEquals("end_date")) {
                    LocalDate endDate = getCsvDate("end_date");
                    if (!endDate.isBefore(futureFeedFirstDate)) {
                        getFieldContext().resetValue(futureFeedFirstDate
                            .minus(1, ChronoUnit.DAYS)
                            .format(GTFS_DATE_FORMATTER));
                    }
                }
            }
        }
        // Track service ID because we want to avoid removing trips that may reference this
        // service_id when the service_id is used by calendar_dates that operate in the valid
        // date range, i.e., before the future feed's first date.
        if (!skipRecord && fieldNameEquals(SERVICE_ID)) mergeFeedsResult.serviceIds.add(getFieldContext().getValueToWrite());
    }

    private boolean shouldUpdateFutureFeedStartDate() {
        return fieldNameEquals("start_date") &&
            (
                EXTEND_FUTURE == mergeFeedsResult.mergeStrategy ||
                    (
                        CHECK_STOP_TIMES == mergeFeedsResult.mergeStrategy &&
                            job.serviceIdsToExtend.contains(keyValue)
                    )
            );
    }
}