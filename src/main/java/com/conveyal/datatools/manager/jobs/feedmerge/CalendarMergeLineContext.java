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

import static com.conveyal.datatools.manager.utils.MergeFeedUtils.hasDuplicateError;
import static com.conveyal.gtfs.loader.DateField.GTFS_DATE_FORMATTER;

/**
 * Contains logic for merging records in the GTFS calendar table.
 */
public class CalendarMergeLineContext extends MergeLineContext {
    private static final Logger LOG = LoggerFactory.getLogger(CalendarMergeLineContext.class);

    public CalendarMergeLineContext(MergeFeedsJob job, Table table, ZipOutputStream out) throws IOException {
        super(job, table, out);
    }

    @Override
    public boolean checkFieldsForMergeConflicts(Set<NewGTFSError> idErrors, FieldContext fieldContext) throws IOException {
        return checkCalendarIds(idErrors, fieldContext);
    }

    @Override
    public void afterRowWrite() throws IOException {
        addClonedServiceId();
    }

    private boolean checkCalendarIds(Set<NewGTFSError> idErrors, FieldContext fieldContext) throws IOException {
        boolean shouldSkipRecord = false;
        String key = getTableScopedValue(keyValue);

        if (isHandlingActiveFeed()) {
            LocalDate startDate = getCsvDate("start_date");
            if (!startDate.isBefore(feedMergeContext.future.getFeedFirstDate())) {
                // If a service_id from the active calendar has both the
                // start_date and end_date in the future, the service will be
                // excluded from the merged file. Records in trips,
                // calendar_dates, and calendar_attributes referencing this
                // service_id shall also be removed/ignored. Stop_time records
                // for the ignored trips shall also be removed.
                LOG.warn(
                    "Skipping active calendar entry {} because it operates fully within the time span of future feed.",
                    keyValue);
                mergeFeedsResult.skippedIds.add(key);
                shouldSkipRecord = true;
            } else {
                // In the MTC revised feed merge logic:
                // - If trip ids in active and future feed are disjoint,
                //   - calendar entries from the active feed will be inserted,
                //     but the ending date will be set to the day before the earliest **calendar start date** from the new feed.
                // - If some trip ids are found in both active/future feed,
                //   - new calendar entries are created for those trips
                //     that span from active feed’s start date to the future feed’s end date.
                //   - calendar entries for other trip ids in the active feed are inserted in the merged feed,
                //     but the ending date will be set to the day before the **start date of the new feed**.
                LocalDate endDate = getCsvDate("end_date");
                LocalDate futureStartDate = null;
                boolean activeAndFutureTripIdsAreDisjoint = job.sharedTripIdsWithConsistentSignature.isEmpty();
                if (activeAndFutureTripIdsAreDisjoint) {
                    futureStartDate = feedMergeContext.futureFirstCalendarStartDate;
                } else if (job.serviceIdsFromActiveFeedToTerminateEarly.contains(keyValue)) {
                    futureStartDate = feedMergeContext.future.getFeedFirstDate();
                }
                // In other cases not covered above, new calendar entry is already flagged for insertion
                // from getMergeStrategy, so that trip ids may reference it.


                if (
                    fieldContext.nameEquals("end_date") &&
                    futureStartDate != null &&
                    !endDate.isBefore(futureStartDate)
                ) {
                    fieldContext.resetValue(futureStartDate
                        .minus(1, ChronoUnit.DAYS)
                        .format(GTFS_DATE_FORMATTER));
                }
            }
        }

        if (isServiceIdUnused()) {
            shouldSkipRecord = true;
        }

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

        // Track service ID because we want to avoid removing trips that may reference this
        // service_id when the service_id is used by calendar_dates that operate in the valid
        // date range, i.e., before the future feed's first date.
        //
        // If service is going to be cloned, add to the output service ids.
        if (!shouldSkipRecord && fieldContext.nameEquals(SERVICE_ID)) {
            mergeFeedsResult.serviceIds.add(fieldContext.getValueToWrite());
        }

        return !shouldSkipRecord;
    }
}