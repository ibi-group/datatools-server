package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.controllers.api.FeedSourceController;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * This feed transformation will replace a file in the target zip (table) with a file from the source version.
 */
public class ReplaceFileTransformation extends ZipTransformation {
    private static final Logger LOG = LoggerFactory.getLogger(ReplaceFileTransformation.class);

    /** no-arg constructor for de/serialization */
    public ReplaceFileTransformation() {}

    public static ReplaceFileTransformation create(String sourceVersionId, String table) {
        ReplaceFileTransformation transformation = new ReplaceFileTransformation();
        transformation.sourceVersionId = sourceVersionId;
        transformation.table = table;
        return transformation;
    }

    @Override
    public void transform(FeedTransformTarget target, MonitorableJob.Status status) {
        // TODO: Refactor into validation code?
        if (target.gtfsFile == null || !target.gtfsFile.exists()) {
            status.fail("Target file must exist.");
            return;
        }
        FeedVersion sourceVersion = Persistence.feedVersions.getById(sourceVersionId);
        if (sourceVersion == null) {
            status.fail("Source version ID must reference valid version.");
            return;
        }
        if (table == null) {
            status.fail("Must specify transformation table name.");
            return;
        }
        String tableName = table + ".txt";
        String tableNamePath = "/" + tableName;

        // Run the replace transformation
        Path sourceZipPath = Paths.get(sourceVersion.retrieveGtfsFile().getAbsolutePath());
        try (FileSystem sourceZipFs = FileSystems.newFileSystem(sourceZipPath, null)) {
            // If the source txt file does not exist, NoSuchFileException will be thrown and caught below.
            Path sourceTxtFilePath = sourceZipFs.getPath(tableNamePath);
            Path targetZipPath = Paths.get(target.gtfsFile.getAbsolutePath());
            LOG.info("Replacing file {} in zip file {} with source {}", tableNamePath, targetZipPath.getFileName(), sourceVersion.id);
            try( FileSystem targetZipFs = FileSystems.newFileSystem(targetZipPath, null) ){
                Path targetTxtFilePath = targetZipFs.getPath(tableNamePath);
                // Copy a file into the zip file, replacing it if it already exists.
                Files.copy(sourceTxtFilePath, targetTxtFilePath, StandardCopyOption.REPLACE_EXISTING);
            }
            LOG.info("File replacement zip transformation successful!");
        } catch (NoSuchFileException e) {
            status.fail("Source version does not contain table: " + tableName, e);
        } catch (Exception e) {
            status.fail("Unknown error encountered while transforming zip file", e);
        }
    }
}
