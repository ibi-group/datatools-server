package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;

/**
 * Target for a DB transformation (e.g., {@link DeleteRecordsTransformation}).
 */
public class FeedTransformDbTarget extends FeedTransformTarget {
    public String snapshotId;

    public FeedTransformDbTarget(String targetSnapshotId) {
        this.snapshotId = targetSnapshotId;
    }

    public String toString() {
        return String.format("Db namespace: %s", snapshotId);
    }

    @Override
    public void validate(MonitorableJob.Status status) {
        if (snapshotId == null) {
            status.fail("Snapshot ID must not be null");
        }
    }
}
