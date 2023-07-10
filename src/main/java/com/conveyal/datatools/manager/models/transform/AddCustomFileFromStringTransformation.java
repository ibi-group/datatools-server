package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;

public class AddCustomFileFromStringTransformation extends StringTransformation {

    // Additional create method required to ensure transformation type is AddCustomFile in tests.
    // Otherwise, we'd use the StringTransformation#create which doesn't differentiate types and hence
    // would fail table name tests.
    public static AddCustomFileFromStringTransformation create(String csvData, String table) {
        AddCustomFileFromStringTransformation transformation = new AddCustomFileFromStringTransformation();
        transformation.csvData = csvData;
        transformation.table = table;
        return transformation;
    }

    @Override
    public void validateTableName(MonitorableJob.Status status) {
        if (table.contains(".txt")) {
            status.fail("CSV Table name should not contain .txt");
        }
    }

}
