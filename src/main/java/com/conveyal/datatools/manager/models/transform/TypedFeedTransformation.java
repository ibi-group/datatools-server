package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;

/**
 * This abstract class is the base for arbitrary feed transformations.
 */
public abstract class TypedFeedTransformation<Target extends FeedTransformTarget> extends FeedTransformation {
    private static final long serialVersionUID = 1L;

    @Override
    public void doTransform(FeedTransformTarget target, MonitorableJob.Status status) {
        try {
            // Attempt to cast transform target to correct flavor
            // (fails the job if types mismatch.)
            Target typedTarget = (Target)target;

            // Validate parameters before running transform.
            validateTableName(status);
            validateFieldNames(status);
            // Let subclasses check parameters.
            validateParameters(status);
            if (status.error) {
                return;
            }

            // Pass the typed transform target to subclasses to transform.
            transform(typedTarget, status);
        } catch (ClassCastException classCastException) {
            status.fail(
                String.format("Transformation must be of type '%s'.", getTransformationTypeName())
            );
        }
    }

    protected abstract String getTransformationTypeName();

    /**
     * Contains the logic for this database-bound transformation.
     * @param target The database-bound or ZIP-file-bound target the transformation will operate on.
     * @param status Used to report success or failure status and details.
     */
    public abstract void transform(Target target, MonitorableJob.Status status);
}
