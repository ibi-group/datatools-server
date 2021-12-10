package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.datatools.manager.jobs.MergeFeedsJob;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.CalendarDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Set;
import java.util.zip.ZipOutputStream;

import static com.conveyal.datatools.manager.jobs.feedmerge.MergeFeedsType.SERVICE_PERIOD;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.getTableScopedValue;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.hasDuplicateError;
import static com.conveyal.gtfs.loader.DateField.GTFS_DATE_FORMATTER;

/**
 * Contains logic for merging records in the GTFS calendar_dates table.
 */
public class CalendarDatesMergeLineContext extends MergeLineContext {
    private static final Logger LOG = LoggerFactory.getLogger(CalendarDatesMergeLineContext.class);

    /** Holds the date used to check calendar validity */
    private LocalDate futureFeedFirstDateForCalendarValidity;

    public CalendarDatesMergeLineContext(MergeFeedsJob job, Table table, ZipOutputStream out) throws IOException {
        super(job, table, out);
    }

    @Override
    public boolean checkFieldsForMergeConflicts(Set<NewGTFSError> idErrors, FieldContext fieldContext) throws IOException {
        return checkCalendarDatesIds(idErrors, fieldContext);
    }

    @Override
    public void afterRowWrite() throws IOException {
        // If the current row is for a calendar service_id that is marked for cloning/renaming, clone the
        // values, change the ID, extend the start/end dates to the feed's full range, and write the
        // additional line to the file.
        addClonedServiceId();
    }

    @Override
    public void startNewFeed(int feedIndex) throws IOException {
        super.startNewFeed(feedIndex);
        futureFeedFirstDateForCalendarValidity = getFutureFeedFirstDateForCheckingCalendarValidity();
    }

    private boolean checkCalendarDatesIds(Set<NewGTFSError> idErrors, FieldContext fieldContext) throws IOException {
        boolean shouldSkipRecord = false;
        String key = getTableScopedValue(table, getIdScope(), keyValue);
        // TODO: REfactor
        String scopedId = String.join(":", getIdScope(), keyValue);
        // Drop any calendar_dates.txt records from the existing feed for dates that are
        // not before the first date of the future feed
        // and also for service ids not in the merged calendar table
        // (we can determine that because the calendar table has already been processed).
        LocalDate date = getCsvDate("date");
        if (
            isHandlingActiveFeed() &&
                (
                    //!job.mergeFeedsResult.serviceIds.contains(scopedId) ||
                    !date.isBefore(futureFeedFirstDateForCalendarValidity)
                )
        ) {
            LOG.warn(
                "Skipping calendar_dates entry {} because it operates in the time span of future feed (i.e., after or on {}).",
                keyValue,
                futureFeedFirstDateForCalendarValidity);
            mergeFeedsResult.skippedIds.add(key);
            shouldSkipRecord = true;
        }

        // TODO: refactor below.
        if (job.mergeType.equals(SERVICE_PERIOD)) {
            if (isHandlingActiveFeed()) {
                // Remove calendar entries that are no longer used.
                if (feedMergeContext.active.getServiceIdsToRemove().contains(keyValue)) {
                    LOG.warn(
                        "Skipping active calendar_dates entry {} because it will become unused in the merged feed.",
                        keyValue);
                    mergeFeedsResult.skippedIds.add(key);
                    shouldSkipRecord = true;
                }
            } else {
                // If handling the future feed, the MTC revised feed merge logic is as follows:
                // - Calendar entries from the future feed will be inserted as is in the merged feed.
                // so no additional processing needed here, unless the calendar entry is no longer used,
                // in that case we drop the calendar entry.
                if (feedMergeContext.future.getServiceIdsToRemove().contains(keyValue)) {
                    LOG.warn(
                        "Skipping future calendar_dates entry {} because it will become unused in the merged feed.",
                        keyValue);
                    mergeFeedsResult.skippedIds.add(key);
                    shouldSkipRecord = true;
                }
            }
        }


        // Track service ID because we want to avoid removing trips that may reference this
        // service_id when the service_id is used by calendar.txt records that operate in
        // the valid date range, i.e., before the future feed's first date.
        if (!shouldSkipRecord && fieldContext.nameEquals(SERVICE_ID)) {
            mergeFeedsResult.serviceIds.add(fieldContext.getValueToWrite());
        }

        return !shouldSkipRecord;
    }

    /**
     * Obtains the future feed start date to use
     * if the future feed's first date is before its first calendar start date,
     * when checking MTC calendar_dates and calendar records for modification/exclusion.
     */
    private LocalDate getFutureFeedFirstDateForCheckingCalendarValidity() {
        LocalDate futureFirstCalendarStartDate = feedMergeContext.futureFirstCalendarStartDate;
        LocalDate futureFeedFirstDate = feedMergeContext.future.getFeedFirstDate();
        if (
            isHandlingActiveFeed() &&
                job.mergeType.equals(SERVICE_PERIOD) &&
                futureFirstCalendarStartDate.isBefore(LocalDate.MAX) &&
                futureFeedFirstDate.isBefore(futureFirstCalendarStartDate)
        ) {
            return futureFirstCalendarStartDate;
        }
        return futureFeedFirstDate;
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

                // Add entries in the future feed.
                writeValuesToTable(clonedValues, true);

                // Because this service has been extended from the future feed into the active feed,
                // we need to add all entries for the original service id under the active feed
                // (and of course rename service id).
                for (CalendarDate calDate : feedMergeContext.active.feed.calendarDates.getAll()) {
                    if (calDate.service_id.equals(originalServiceId)) {
                        writeValuesToTable(getCalendarRowValues(calDate, newServiceId), true);
                    }
                }
            }
        }
    }

    /**
     * Helper method that builds a string array from a CalendarDates object
     * with a new service_id.
     */
    private String[] getCalendarRowValues(CalendarDate calDate, String newServiceId) {
        String[] rowValues = new String[getOriginalRowValues().length];
        rowValues[getFieldIndex(SERVICE_ID)] = newServiceId;
        rowValues[getFieldIndex("date")]
            = calDate.date.format(GTFS_DATE_FORMATTER);
        rowValues[getFieldIndex("exception_type")]
            = String.valueOf(calDate.exception_type);
        return rowValues;
    }
}