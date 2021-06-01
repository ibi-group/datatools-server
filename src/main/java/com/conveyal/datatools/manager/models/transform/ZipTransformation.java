package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;

import java.nio.file.FileSystem;
import java.nio.file.Path;

/**
 * This is an abstract class that represents a transformation that should apply to a GTFS in zip form. In other
 * words, subclasses will provide a transform override method that acts directly on the zip file. Sample fields
 * csvData and sourceVersionId can be used to reference replacement string or file that should be passed to the target
 * zip file of the transformation.
 */
public abstract class ZipTransformation extends FeedTransformation<FeedTransformZipTarget> {
    public String csvData;
    public String sourceVersionId;

    protected void validateFieldNames(MonitorableJob.Status status) {
        // Do nothing.
    }

    protected String getTransformationTypeName() {
        return FeedTransformZipTarget.class.getSimpleName();
    }

    /**
     * Obtains a {@link Path} object for the specified GTFS table file in a ZIP archive.
     */
    protected Path getTablePathInZip(String tableName, FileSystem targetZipFs) {
        return targetZipFs.getPath("/" + tableName);
    }
}
