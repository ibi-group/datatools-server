package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.TableTransformResult;
import com.conveyal.datatools.manager.models.TransformType;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class StringTransformation extends ZipTransformation {

    public static StringTransformation create(String csvData, String table) {
        StringTransformation transformation = new StringTransformation();
        transformation.csvData = csvData;
        transformation.table = table;
        return transformation;
    }

    @Override
    public void validateParameters(MonitorableJob.Status status) {
        if (csvData == null) {
            status.fail("CSV data must not be null");
        }
    }

    @Override
    public void transform(FeedTransformZipTarget zipTarget, MonitorableJob.Status status) {
        String tableName = table + ".txt";
        Path targetZipPath = Paths.get(zipTarget.gtfsFile.getAbsolutePath());
        try (
            FileSystem targetZipFs = FileSystems.newFileSystem(targetZipPath, (ClassLoader) null);
            InputStream inputStream = new ByteArrayInputStream(csvData.getBytes(StandardCharsets.UTF_8));
        ) {
            Path targetTxtFilePath = getTablePathInZip(tableName, targetZipFs);
            TransformType type = Files.exists(targetTxtFilePath)
                ? TransformType.TABLE_REPLACED
                : TransformType.TABLE_ADDED;
            // Copy csv input stream into the zip file, replacing it if it already exists.
            Files.copy(inputStream, targetTxtFilePath, StandardCopyOption.REPLACE_EXISTING);
            final int NEW_LINE_CHARACTER_CODE = 10;
            int lineCount = (int) csvData.chars().filter(c -> c == NEW_LINE_CHARACTER_CODE).count();
            int addedCount = type == TransformType.TABLE_ADDED ? lineCount : 0;
            int updatedCount = type == TransformType.TABLE_MODIFIED ? lineCount : 0;
            zipTarget.feedTransformResult.tableTransformResults.add(new TableTransformResult(
                tableName,
                type,
                0,
                updatedCount,
                addedCount,
                0
            ));
        } catch (Exception e) {
            status.fail("Unknown error encountered while transforming zip file", e);
        }
    }
}
