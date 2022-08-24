package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.datatools.manager.jobs.MergeFeedsJob;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.CalendarDate;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.zip.ZipOutputStream;

import static com.conveyal.datatools.manager.jobs.feedmerge.MergeFeedsType.SERVICE_PERIOD;
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
        return checkCalendarDatesIds(fieldContext);
    }

    @Override
    public void afterTableRecords() throws IOException {
        // If the current row is for a calendar service_id that is marked for cloning/renaming, clone the
        // values, change the ID, extend the start/end dates to the feed's full range, and write the
        // additional line to the file.
        addClonedServiceIds();
    }

    @Override
    public void startNewFeed(int feedIndex) throws IOException {
        super.startNewFeed(feedIndex);
        futureFeedFirstDateForCalendarValidity = getFutureFeedFirstDateForCheckingCalendarValidity();
    }

    private boolean checkCalendarDatesIds(FieldContext fieldContext) throws IOException {
        boolean shouldSkipRecord = false;
        if (job.mergeType.equals(SERVICE_PERIOD)) {
            // Drop any calendar_dates.txt records from the existing feed for dates that are
            // not before the first date of the future feed,
            // or for corresponding calendar entries that have been dropped.
            LocalDate date = getCsvDate("date");
            String calendarKey = getTableScopedValue(Table.CALENDAR, keyValue);
            if (
                isHandlingActiveFeed() &&
                    (
                        job.mergeFeedsResult.skippedIds.contains(calendarKey) ||
                        !isBeforeFutureFeedStartDate(date)
                    )
            ) {
                String key = getTableScopedValue(keyValue);
                LOG.warn(
                    "Skipping calendar_dates entry {} because it operates in the time span of future feed (i.e., after or on {}).",
                    keyValue,
                    futureFeedFirstDateForCalendarValidity
                );
                mergeFeedsResult.skippedIds.add(key);
                shouldSkipRecord = true;
            }

            if (isServiceIdUnused()) {
                shouldSkipRecord = true;
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

    private boolean isBeforeFutureFeedStartDate(LocalDate date) {
        return date.isBefore(futureFeedFirstDateForCalendarValidity);
    }

    /**
     * Adds a cloned service id for trips with the same signature in both the active & future feeds.
     * The cloned service id spans from the start date in the active feed until the end date in the future feed.
     */
    public void addClonedServiceIds() throws IOException {
        if (job.mergeType.equals(SERVICE_PERIOD)) {
            String clonedIdScope = getClonedIdScope();

            // Retrieve all active and future calendar dates ahead
            // to avoid repeat database get-all queries,
            // and exclude active entries with a date after the future feed start date.
            List<CalendarDate> allCalendarDates = new ArrayList<>();
            allCalendarDates.addAll(Lists.newArrayList(
                StreamSupport.stream(feedMergeContext.active.feed.calendarDates.spliterator(), false)
                    .filter(calDate -> isBeforeFutureFeedStartDate(calDate.date))
                    .collect(Collectors.toList())
            ));
            allCalendarDates.addAll(Lists.newArrayList(
                feedMergeContext.future.feed.calendarDates.getAll()
            ));

            for (String id : job.serviceIdsToCloneRenameAndExtend) {
                String newServiceId = getIdWithScope(id, clonedIdScope);

                // Because this service has been extended to span both active and future feed,
                // we need to add all calendar_dates entries for the original service id
                // under the active AND future feed (and of course rename service id).
                for (CalendarDate calDate : allCalendarDates) {
                    if (calDate.service_id.equals(id)) {
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
        rowValues[getFieldIndexFromSharedSpecs(SERVICE_ID)] = newServiceId;
        rowValues[getFieldIndexFromSharedSpecs("date")]
            = calDate.date.format(GTFS_DATE_FORMATTER);
        rowValues[getFieldIndexFromSharedSpecs("exception_type")]
            = String.valueOf(calDate.exception_type);
        return rowValues;
    }
}