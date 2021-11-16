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
        boolean shouldSkipRecord = false;
        // trip_ids between active and future datasets must not match. The tripIdsToSkip and
        // tripIdsToModify sets below are determined based on the MergeStrategy used for MTC
        // service period merges.
        if (isHandlingActiveFeed()) {
            // Handling active feed. Skip or modify trip id if found in one of the
            // respective sets.
            if (job.tripIdsToSkipForActiveFeed.contains(keyValue)) {
                shouldSkipRecord = true;
            } else if (job.tripIdsToModifyForActiveFeed.contains(keyValue)) {
                updateAndRemapOutput(true);
            }
        }

        // Remap duplicate trip ids.
        if (hasDuplicateError(idErrors)) {
            updateAndRemapOutput(fieldContext, true);
        }

        return !shouldSkipRecord;
    }
}