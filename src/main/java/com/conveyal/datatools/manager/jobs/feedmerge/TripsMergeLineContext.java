package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.datatools.manager.jobs.MergeFeedsJob;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.loader.Table;

import java.io.IOException;
import java.util.Set;
import java.util.zip.ZipOutputStream;

import static com.conveyal.datatools.manager.jobs.feedmerge.MergeFeedsType.SERVICE_PERIOD;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.hasDuplicateError;

public class TripsMergeLineContext extends MergeLineContext {
    public TripsMergeLineContext(MergeFeedsJob job, Table table, ZipOutputStream out) throws IOException {
        super(job, table, out);
    }

    @Override
    public boolean checkFieldsForMergeConflicts(Set<NewGTFSError> idErrors, FieldContext fieldContext) {
        return checkTripIds(idErrors, fieldContext);
    }

    private boolean checkTripIds(Set<NewGTFSError> idErrors, FieldContext fieldContext) {
        // For the MTC revised feed merge process,
        // the updated logic requires to insert all trips from both the active and future feed,
        // except if they are present in both, in which case
        // we only insert the trip entry from the future feed and skip the one in the active feed.
        boolean shouldSkipRecord =
            job.mergeType.equals(SERVICE_PERIOD) &&
            isHandlingActiveFeed() &&
            job.sharedTripIdsWithConsistentSignature.contains(keyValue);

        // Remap duplicate trip ids for records that are not skipped.
        if (!shouldSkipRecord && hasDuplicateError(idErrors)) {
            updateAndRemapOutput(fieldContext, true);
        }

        // Remove remapped service_ids associated to the trips table from the merge summary
        // (the remapped id is already listed under the calendar/calendar_dates tables,
        // so there is no need to add that foreign key again).
        if (fieldContext.nameEquals(SERVICE_ID)) {
            mergeFeedsResult.remappedIds.remove(
                getTableScopedValue(fieldContext.getValue())
            );
        }

        return !shouldSkipRecord;
    }
}