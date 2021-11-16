package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.datatools.manager.jobs.MergeFeedsJob;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.loader.Table;

import java.io.IOException;
import java.util.Set;
import java.util.zip.ZipOutputStream;

public class TripsMergeLineContext extends MergeLineContext {
    public TripsMergeLineContext(MergeFeedsJob job, Table table, ZipOutputStream out) throws IOException {
        super(job, table, out);
    }

    @Override
    public boolean checkFieldsForMergeConflicts(Set<NewGTFSError> idErrors) {
        return checkTripIds(idErrors);
    }

    private boolean checkTripIds(Set<NewGTFSError> idErrors) {
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
        for (NewGTFSError error : idErrors) {
            if (error.errorType.equals(NewGTFSErrorType.DUPLICATE_ID)) {
                updateAndRemapOutput(true);
            }
        }

        return !shouldSkipRecord;
    }

}