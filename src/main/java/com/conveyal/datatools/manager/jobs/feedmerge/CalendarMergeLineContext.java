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
    public void checkFieldsForMergeConflicts(Set<NewGTFSError> idErrors, FieldContext fieldContext) throws IOException {
        checkCalendarIds(idErrors, fieldContext);
    }

    private void checkCalendarIds(Set<NewGTFSError> idErrors, FieldContext fieldContext) throws IOException {
        if (isHandlingActiveFeed()) {
            LocalDate startDate = getCsvDate("start_date");
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
                    futureStartDate = feedMergeContext.getFutureFirstCalendarStartDate();
                } else if (job.serviceIdsToTerminateEarly.contains(keyValue)) {
                    futureStartDate = futureFeedFirstDate;
                } else {
                    // New calendar entry is already flagged for insertion from getMergeStrategy.
                    // Insert this calendar record for other trip ids that may reference it.
                }

                if (fieldContext.nameEquals("end_date")) {
                    if (futureStartDate != null && !endDate.isBefore(futureStartDate)) {
                        fieldContext.resetValue(futureStartDate
                            .minus(1, ChronoUnit.DAYS)
                            .format(GTFS_DATE_FORMATTER));
                    }
                }
            }
        } else if (isHandlingFutureFeed()) {
            // In the MTC revised feed merge logic:
            // - Calendar entries from the future feed will be inserted as is in the merged feed.
            // so no additional processing needed here.
        }

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
            updateAndRemapOutput(fieldContext);
        }

        if (isHandlingFutureFeed()) {
            // FIXME: Move this below so that a cloned service doesn't get prematurely
            //  modified? (do we want the cloned record to have the original values?)
            if (shouldUpdateFutureFeedStartDate(fieldContext)) {
                // Update start_date to extend service through the active feed's
                // start date if the merge strategy dictates. The justification for this logic is that the active feed's
                // service_id will be modified to a different unique value and the trips shared between the future/active
                // service are exactly matching.
                fieldContext.resetValue(feedMergeContext.activeFeedFirstDate.format(GTFS_DATE_FORMATTER));
            }
        }
        // Track service ID because we want to avoid removing trips that may reference this
        // service_id when the service_id is used by calendar_dates that operate in the valid
        // date range, i.e., before the future feed's first date.
        if (!skipRecord && fieldContext.nameEquals(SERVICE_ID)) mergeFeedsResult.serviceIds.add(fieldContext.getValueToWrite());
    }

    private boolean shouldUpdateFutureFeedStartDate(FieldContext fieldContext) {
        return fieldContext.nameEquals("start_date") &&
            (
                EXTEND_FUTURE == mergeFeedsResult.mergeStrategy ||
                    (
                        CHECK_STOP_TIMES == mergeFeedsResult.mergeStrategy &&
                            job.serviceIdsToExtend.contains(keyValue)
                    )
            );
    }
}