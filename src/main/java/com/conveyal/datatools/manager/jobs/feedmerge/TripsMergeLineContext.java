package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.datatools.manager.jobs.MergeFeedsJob;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.loader.Table;

import java.io.IOException;
import java.util.Set;
import java.util.zip.ZipOutputStream;

import static com.conveyal.datatools.manager.utils.MergeFeedUtils.getTableScopedValue;

public class TripsMergeLineContext extends MergeLineContext {
    public TripsMergeLineContext(MergeFeedsJob job, Table table, ZipOutputStream out) throws IOException {
        super(job, table, out);
    }

    @Override
    public void checkFieldsForMergeConflicts(Set<NewGTFSError> idErrors) throws IOException {
        checkTripIds(idErrors);
    }

    private void checkTripIds(Set<NewGTFSError> idErrors) {
        // trip_ids between active and future datasets must not match. The tripIdsToSkip and
        // tripIdsToModify sets below are determined based on the MergeStrategy used for MTC
        // service period merges.
        String idScope = getIdScope();
        if (isHandlingActiveFeed()) {
            // Handling active feed. Skip or modify trip id if found in one of the
            // respective sets.
            if (job.tripIdsToSkipForActiveFeed.contains(keyValue)) {
                skipRecord = true;
            } else if (job.tripIdsToModifyForActiveFeed.contains(keyValue)) {
                valueToWrite = String.join(":", idScope, val);
                // Update key value for subsequent ID conflict checks for this row.
                keyValue = valueToWrite;
                mergeFeedsResult.remappedIds.put( // TODO: refactor
                    getTableScopedValue(table, idScope, val),
                    valueToWrite
                );
            }
        }
        for (NewGTFSError error : idErrors) {
            if (error.errorType.equals(NewGTFSErrorType.DUPLICATE_ID)) {
                valueToWrite = String.join(":", idScope, val);
                // Update key value for subsequent ID conflict checks for this row.
                keyValue = valueToWrite;
                mergeFeedsResult.remappedIds.put( // TODO: refactor
                    getTableScopedValue(table, idScope, val),
                    valueToWrite
                );
            }
        }
    }
}