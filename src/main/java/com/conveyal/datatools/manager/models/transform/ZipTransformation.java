package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;

/**
 * This is an abstract class that represents a transformation that should apply to a GTFS in zip form. In other
 * words, subclasses will provide a transform override method that acts directly on the zip file. Sample fields
 * csvData and sourceVersionId can be used to reference replacement string or file that should be passed to the target
 * zip file of the transformation.
 */
public abstract class ZipTransformation extends FeedTransformation {
    public String csvData;
    public String sourceVersionId;

    public abstract void transform(FeedTransformZipTarget target, MonitorableJob.Status status);

    @Override
    public void doTransform(FeedTransformTarget target, MonitorableJob.Status status) {
        if (!(target instanceof FeedTransformZipTarget)) {
            status.fail("Target must be FeedTransformZipTarget.");
            return;
        }

        // Cast transform target to zip flavor and pass it to subclasses to transform.
        transform((FeedTransformZipTarget)target, status);
    }
}
