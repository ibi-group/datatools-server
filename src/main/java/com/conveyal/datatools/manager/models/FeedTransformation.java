package com.conveyal.datatools.manager.models;

public class FeedTransformation extends Model {
    private static final long serialVersionUID = 1L;

    public String sourceVersionId;
    public TransformType transformType;
    public String csvData;
    public String table;
    // FIXME
    public Class jobClazz;

    public static FeedTransformation defaultReplaceFileTransform(String sourceVersionId, String table) {
        FeedTransformation transformation = new FeedTransformation();
        transformation.transformType = TransformType.REPLACE_FILE_FROM_VERSION;
        transformation.sourceVersionId = sourceVersionId;
        transformation.table = table;
        return transformation;
    }

    public boolean isAppliedBeforeLoad() {
        switch (this.transformType) {
            case REPLACE_FILE_FROM_VERSION:
            case REPLACE_FILE_FROM_STRING:
                return true;
            default:
                return false;
        }
    }
}

