package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;

import java.util.List;

/**
 * This is an abstract class that represents a transformation that should apply to a GTFS in database form. In other
 * words, subclasses will provide a transform override method that acts on a database namespace. Sample fields
 * matchField and matchValues can be used to construct a WHERE clause for applying updates to a filtered set of records.
 */
public abstract class DbTransformation extends FeedTransformation {
    public String matchField;
    public List<String> matchValues;

    public abstract void transform(FeedTransformDbTarget target, MonitorableJob.Status status);

    @Override
    public void doTransform(FeedTransformTarget target, MonitorableJob.Status status) {
        if (!(target instanceof FeedTransformDbTarget)) {
            status.fail("Target must be FeedTransformDbTarget.");
            return;
        }
        // Cast transform target to DB flavor and pass it to subclasses to transform.
        transform((FeedTransformDbTarget)target, status);
    }
}
