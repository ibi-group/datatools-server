package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.FeedTransformResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface for the different targets that a {@link FeedTransformation} can apply to:
 * - zip file {@link FeedTransformZipTarget} and
 * - db snapshot {@link FeedTransformDbTarget} (contains snapshotId which is a proxy for the database namespace)
 */
public abstract class FeedTransformTarget {
    private static final Logger LOG = LoggerFactory.getLogger(FeedTransformTarget.class);

    /** Contains the result of various transformations applied to the target feed. */
    public FeedTransformResult feedTransformResult = new FeedTransformResult();

    /**
     * Override this method to validate the target fields and update the status if there is an error.
     */
    public void validate(MonitorableJob.Status status) {
        LOG.warn("Override validate method to ensure feed transform target is defined correctly.");
    }
}
