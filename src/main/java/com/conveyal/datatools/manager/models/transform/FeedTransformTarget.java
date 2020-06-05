package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;

/**
 * Interface for the different targets that a {@link FeedTransformation} can apply to:
 * - zip file {@link FeedTransformZipTarget} and
 * - db snapshot {@link FeedTransformDbTarget} (contains snapshotId which is a proxy for the database namespace)
 */
public interface FeedTransformTarget {
    /**
     * Validate the target fields and update the status if there is an error.
     */
    void validate(MonitorableJob.Status status);
}
