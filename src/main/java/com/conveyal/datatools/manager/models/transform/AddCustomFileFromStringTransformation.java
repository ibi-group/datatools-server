package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;

public class AddCustomFileFromStringTransformation extends StringTransformation {

    @Override
    public void validateTableName(MonitorableJob.Status status) {
        if (table.contains(".txt")) {
            status.fail("CSV Table name should not contain .txt");
        }
    }

}
