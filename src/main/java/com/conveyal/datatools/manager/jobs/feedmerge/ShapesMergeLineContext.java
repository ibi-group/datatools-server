package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.datatools.manager.jobs.MergeFeedsJob;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.loader.Table;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipOutputStream;

import static com.conveyal.datatools.manager.utils.MergeFeedUtils.hasDuplicateError;

public class ShapesMergeLineContext extends MergeLineContext {
    // Track shape_ids found in future feed in order to check for conflicts with active feed (MTC only).
    private final Set<String> shapeIdsInFutureFeed = new HashSet<>();

    public ShapesMergeLineContext(MergeFeedsJob job, Table table, ZipOutputStream out) throws IOException {
        super(job, table, out);
    }

    @Override
    public void checkFieldsForMergeConflicts(Set<NewGTFSError> idErrors) {
        checkShapeIds(idErrors);
    }

    private void checkShapeIds(Set<NewGTFSError> idErrors) {
        // If a shape_id is found in both future and active datasets, all shape points from
        // the active dataset must be feed-scoped. Otherwise, the merged dataset may contain
        // shape_id:shape_pt_sequence values from both datasets (e.g., if future dataset contains
        // sequences 1,2,3,10 and active contains 1,2,7,9,10; the merged set will contain
        // 1,2,3,7,9,10).
        if (getField().name.equals("shape_id")) {
            if (isHandlingFutureFeed()) {
                // Track shape_id if working on future feed.
                shapeIdsInFutureFeed.add(val);
            } else if (shapeIdsInFutureFeed.contains(val)) {
                // For the active feed, if the shape_id was already processed from the
                // future feed, we need to add the feed-scope to avoid weird, hybrid shapes
                // with points from both feeds.
                updateAndRemapValue(true);
                // Re-check refs and uniqueness after changing shape_id value. (Note: this
                // probably won't have any impact, but there's not much harm in including it.)
                idErrors = referenceTracker
                    .checkReferencesAndUniqueness(keyValue, getLineNumber(), getField(), valueToWrite,
                        table, keyField, table.getOrderFieldName());
            }
        }
        // Skip record if normal duplicate errors are found.
        if (hasDuplicateError(idErrors)) skipRecord = true;
    }
}