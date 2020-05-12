package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * This feed transformation will replace a file in the target zip (table) with the provided csv data.
 */
public class ReplaceFileFromStringTransformation extends ZipTransformation {

    public static ReplaceFileFromStringTransformation create(String csvData, String table) {
        ReplaceFileFromStringTransformation transformation = new ReplaceFileFromStringTransformation();
        transformation.csvData = csvData;
        transformation.table = table;
        return transformation;
    }

    @Override
    public void transform(FeedTransformTarget target, MonitorableJob.Status status) {
        // TODO: Refactor into validation code?
        if (target.gtfsFile == null || !target.gtfsFile.exists()) {
            status.fail("Target version must not be null.");
            return;
        }
        if (csvData == null) {
            // TODO: Should we permit a null value (perhaps to result in removing the file)?
            status.fail("CSV data must not be null.");
            return;
        }
        if (table == null) {
            status.fail("Must specify transformation table name.");
            return;
        }
        String tableName = table + ".txt";
        String tableNamePath = "/" + tableName;
        // Run the replace transformation
        Path targetZipPath = Paths.get(target.gtfsFile.getAbsolutePath());
        try( FileSystem targetZipFs = FileSystems.newFileSystem(targetZipPath, null) ){
            // Convert csv data to input stream.
            InputStream inputStream = new ByteArrayInputStream(csvData.getBytes(StandardCharsets.UTF_8));
            Path targetTxtFilePath = targetZipFs.getPath(tableNamePath);
            // Copy csv input stream into the zip file, replacing it if it already exists.
            Files.copy(inputStream, targetTxtFilePath, StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
            status.fail("Unknown error encountered while transforming zip file", e);
        }
    }
}
