package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.gtfsplus.tables.GtfsPlusTable;
import com.conveyal.gtfs.loader.Table;

import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.Arrays;

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

        // Validate parameters before running transform.
        // Check that the table name is valid per GTFS or GTFS-plus.
        if (table == null) {
            status.fail("Must specify transformation table name.");
            return;
        }
        if (Arrays.stream(Table.tablesInOrder).noneMatch(t -> t.name.equals(table)) &&
            Arrays.stream(GtfsPlusTable.tables).noneMatch(t -> t.name.equals(table))
        ) {
            status.fail("The transformation table name is not valid.");
            return;
        }
        // Let subclasses check parameters.
        validateParameters(status);
        if (status.error) {
            return;
        }

        // Cast transform target to zip flavor and pass it to subclasses to transform.
        transform((FeedTransformZipTarget)target, status);
    }

    /**
     * Obtains a {@link Path} object for the specified GTFS table file in a ZIP archive.
     */
    protected Path getTablePathInZip(String tableName, FileSystem targetZipFs) {
        return targetZipFs.getPath("/" + tableName);
    }
}
