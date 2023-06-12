package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;

/**
 * This feed transformation will replace a file in the target zip (table) with the provided csv data.
 */
public class ReplaceFileFromStringTransformation extends StringTransformation {

    @Override
    public void validateParameters(MonitorableJob.Status status) {
        if (csvData == null) {
            status.fail("CSV data must not be null (delete table not yet supported)");
        }
    }
}
