package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.FeedTransformResult;

/**
 * Interface for the different targets that a {@link FeedTransformation} can apply to:
 * - zip file {@link FeedTransformZipTarget} and
 * - db snapshot {@link FeedTransformDbTarget} (contains snapshotId which is a proxy for the database namespace)
 */
public interface FeedTransformTarget {
    FeedTransformResult feedTransformResult = new FeedTransformResult();
    /**
     * Validate the target fields and update the status if there is an error.
     */
    void validate(MonitorableJob.Status status);
}
