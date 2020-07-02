package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.TableTransformResult;
import com.conveyal.datatools.manager.models.TransformType;
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
public class ReplaceFileFromVersionTransformation extends ZipTransformation {
    private static final Logger LOG = LoggerFactory.getLogger(ReplaceFileFromVersionTransformation.class);

    /** no-arg constructor for de/serialization */
    public ReplaceFileFromVersionTransformation() {}

    public static ReplaceFileFromVersionTransformation create(String sourceVersionId, String table) {
        ReplaceFileFromVersionTransformation transformation = new ReplaceFileFromVersionTransformation();
        transformation.sourceVersionId = sourceVersionId;
        transformation.table = table;
        return transformation;
    }

    @Override
    public void transform(FeedTransformTarget target, MonitorableJob.Status status) {
        if (!(target instanceof FeedTransformZipTarget)) {
            status.fail("Target must be FeedTransformZipTarget.");
            return;
        }
        // Cast transform target to zip flavor.
        FeedTransformZipTarget zipTarget = (FeedTransformZipTarget)target;
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
            Path targetZipPath = Paths.get(zipTarget.gtfsFile.getAbsolutePath());
            LOG.info("Replacing file {} in zip file {} with source {}", tableNamePath, targetZipPath.getFileName(), sourceVersion.id);
            try( FileSystem targetZipFs = FileSystems.newFileSystem(targetZipPath, null) ){
                Path targetTxtFilePath = targetZipFs.getPath(tableNamePath);
                // Set transform type according to whether target file exists.
                TransformType type = Files.exists(targetTxtFilePath)
                    ? TransformType.TABLE_REPLACED
                    : TransformType.TABLE_ADDED;
                // Copy a file into the zip file, replacing it if it already exists.
                Files.copy(sourceTxtFilePath, targetTxtFilePath, StandardCopyOption.REPLACE_EXISTING);
                target.feedTransformResult.addResultForTable(new TableTransformResult(tableName, type));
            }
            LOG.info("File replacement zip transformation successful!");
        } catch (NoSuchFileException e) {
            status.fail("Source version does not contain table: " + tableName, e);
        } catch (Exception e) {
            status.fail("Unknown error encountered while transforming zip file", e);
        }
    }
}
