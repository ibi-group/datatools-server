package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.datatools.manager.jobs.MergeFeedsJob;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.loader.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Set;
import java.util.zip.ZipOutputStream;

import static com.conveyal.datatools.manager.jobs.feedmerge.MergeFeedsType.SERVICE_PERIOD;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.getTableScopedValue;

public class CalendarDatesMergeLineContext extends MergeLineContext {
    private static final Logger LOG = LoggerFactory.getLogger(CalendarDatesMergeLineContext.class);

    public CalendarDatesMergeLineContext(MergeFeedsJob job, Table table, ZipOutputStream out) throws IOException {
        super(job, table, out);
    }

    @Override
    public void checkFieldsForMergeConflicts(Set<NewGTFSError> idErrors) throws IOException {
        checkCalendarDatesIds();
    }

    @Override
    public void startNewRow() throws IOException {
        super.startNewRow();
        updateFutureFeedFirstDate();
    }

    private void checkCalendarDatesIds() throws IOException {
        // Drop any calendar_dates.txt records from the existing feed for dates that are
        // not before the first date of the future feed.
        LocalDate date = getCsvDate("date");
        if (isHandlingActiveFeed() && !date.isBefore(futureFeedFirstDate)) {
            LOG.warn(
                "Skipping calendar_dates entry {} because it operates in the time span of future feed (i.e., after or on {}).",
                keyValue,
                futureFeedFirstDate);
            String key = getTableScopedValue(table, getIdScope(), keyValue);
            mergeFeedsResult.skippedIds.add(key);
            skipRecord = true;
        }
        // Track service ID because we want to avoid removing trips that may reference this
        // service_id when the service_id is used by calendar.txt records that operate in
        // the valid date range, i.e., before the future feed's first date.
        if (!skipRecord && fieldNameEquals(SERVICE_ID)) mergeFeedsResult.serviceIds.add(getFieldContext().getValueToWrite());
    }

    private void updateFutureFeedFirstDate() {
        if (
            isHandlingActiveFeed() &&
            job.mergeType.equals(SERVICE_PERIOD) &&
            futureFirstCalendarStartDate.isBefore(LocalDate.MAX) &&
            futureFeedFirstDate.isBefore(futureFirstCalendarStartDate)
        ) {
            // If the future feed's first date is before its first calendar start date,
            // override the future feed first date with the calendar start date for use when checking
            // MTC calendar_dates and calendar records for modification/exclusion.
            futureFeedFirstDate = futureFirstCalendarStartDate;
        }
    }
}