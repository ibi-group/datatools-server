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
        // For the MTC revised feed merge process,
        // the updated logic requires to insert all trips from both the active and future feed,
        // except if they are present in both, in which case we only insert the trip entry from the future feed.
        if (
            job.mergeType.equals(SERVICE_PERIOD) &&
            isHandlingActiveFeed() &&
            job.sharedTripIdsWithConsistentSignature.contains(keyValue)
        ) {
            // Skip this record, we will use the one from the future feed.
            shouldSkipRecord = true;
        }

        // Remap duplicate trip ids.
        if (hasDuplicateError(idErrors)) {
            updateAndRemapOutput(fieldContext, true);
        }

        return !shouldSkipRecord;
    }
}