package com.conveyal.datatools.manager.models;

import java.io.Serializable;

/**
 * Describes changes made to a table by a single {@link com.conveyal.datatools.manager.models.transform.FeedTransformation}.
 */
public class TableTransformResult implements Serializable {
    private static final long serialVersionUID = 1L;

    public int deletedCount;
    public int updatedCount;
    public int addedCount;
    public TransformType transformType;
    public String tableName;

    public TableTransformResult() {}

    public TableTransformResult(String tableName, TransformType transformType) {
        this.tableName = tableName;
        this.transformType = transformType;
    }

    public TableTransformResult(String tableName, int deletedCount, int updatedCount, int addedCount) {
        this.tableName = tableName;
        this.transformType = TransformType.TABLE_MODIFIED;
        this.deletedCount = deletedCount;
        this.updatedCount = updatedCount;
        this.addedCount = addedCount;
    }
}
