
package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.TableTransformResult;
import com.conveyal.datatools.manager.models.TransformType;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class AppendToFileTransformation extends ZipTransformation {

    public static AppendToFileTransformation create(String csvData, String table) {
        AppendToFileTransformation transformation = new AppendToFileTransformation();
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
            InputStream newLineStream = new ByteArrayInputStream("\n".getBytes(StandardCharsets.UTF_8));
            InputStream inputStream = new ByteArrayInputStream(csvData.getBytes(StandardCharsets.UTF_8));
        ) {
            TransformType type = TransformType.TABLE_MODIFIED;

            Path targetTxtFilePath = getTablePathInZip(tableName, targetZipFs);

            final File tempFile = File.createTempFile(tableName + "-temp", ".txt");
            Files.copy(targetTxtFilePath, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

            // Append CSV data into the target file in the temporary copy of file
            try (OutputStream os = new FileOutputStream(tempFile, true)) {
                // Append a newline in case our data doesn't include one
                // Having an extra newline is not a problem!
                os.write(newLineStream.readAllBytes());
                os.write(inputStream.readAllBytes());
            } catch (Exception e) {
                status.fail("Failed to write to target file", e);
            }

            // Copy modified file into zip
            Files.copy(tempFile.toPath(), targetTxtFilePath, StandardCopyOption.REPLACE_EXISTING);

            final int NEW_LINE_CHARACTER_CODE = 10;
            int lineCount = (int) csvData.chars().filter(c -> c == NEW_LINE_CHARACTER_CODE).count();
            zipTarget.feedTransformResult.tableTransformResults.add(new TableTransformResult(
                    tableName,
                    type,
                    0,
                    0,
                    lineCount + 1,
                    0
            ));
        } catch (Exception e) {
            status.fail("Unknown error encountered while transforming zip file", e);
        }
    }
}
