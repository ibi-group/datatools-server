package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.gtfs.loader.JdbcGtfsLoader;

import java.util.List;

/**
 * This is an abstract class that represents a transformation that should apply to a GTFS in database form. In other
 * words, subclasses will provide a transform override method that acts on a database namespace. Sample fields
 * matchField and matchValues can be used to construct a WHERE clause for applying updates to a filtered set of records.
 */
public abstract class DbTransformation extends FeedTransformation<FeedTransformDbTarget> {
    public String matchField;
    public List<String> matchValues;

    protected String getTransformationTypeName() {
        return FeedTransformDbTarget.class.getSimpleName();
    }

    protected void validateFieldNames(MonitorableJob.Status status) {
        // Validate fields before running transform.

        if (matchField == null) {
            status.fail("Must provide valid match field");
            return;
        }
        String cleanField = JdbcGtfsLoader.sanitize(matchField, null);
        if (!matchField.equals(cleanField)) {
            status.fail("Input match field contained disallowed special characters (only alphanumeric and underscores are permitted).");
        }
    }
}
