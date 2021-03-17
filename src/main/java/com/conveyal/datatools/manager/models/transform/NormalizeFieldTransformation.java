package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.TableTransformResult;
import com.conveyal.datatools.manager.models.TransformType;
import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.Table;
import com.csvreader.CsvReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.WordUtils;

import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.ZipFile;

import static com.conveyal.datatools.manager.DataManager.getConfigPropertyAsText;
import static com.conveyal.gtfs.loader.Field.getFieldIndex;

public class NormalizeFieldTransformation extends ZipTransformation {
    //private final List<String> defaultExceptions = getConfigPropertyAsText("DEFAULT_CAPITALIZATION_EXCEPTIONS");
    public String fieldName;
    public NormalizeOperation normalizeOperation;
    //public List<String> exceptions = defaultExceptions;
    public List<String> replacementPairs = Arrays.asList("@,at", "+,and");
    public List<String> removalBounds = Arrays.asList("()", "[]");
    public static NormalizeFieldTransformation create(String table, String fieldName, List<String> exceptions) {
        NormalizeFieldTransformation transformation = new NormalizeFieldTransformation();
        transformation.fieldName = fieldName;
        transformation.table = table;
        // Ensure capitalization exceptions are set to uppercase
        //if (exceptions != null) transformation.exceptions = exceptions.stream()
        //    .map(String::toUpperCase)
        //    .collect(Collectors.toList());
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
        if (fieldName == null) {
            status.fail("Field name must not be null");
            return;
        }
        if (table == null) {
            status.fail("Must specify transformation table name.");
            return;
        }
        String tableName = table + ".txt";
        String tableNamePath = "/" + tableName;
        // Run the replace transformation
        Path targetZipPath = Paths.get(zipTarget.gtfsFile.getAbsolutePath());
        try( FileSystem targetZipFs = FileSystems.newFileSystem(targetZipPath, null) ){
            Path targetTxtFilePath = targetZipFs.getPath(tableNamePath);
            Table gtfsTable = Arrays.stream(Table.tablesInOrder).filter(t -> t.name.equals(table)).findFirst().get();
            CsvReader csvReader = gtfsTable.getCsvReader(new ZipFile(zipTarget.gtfsFile), null);
            Field[] fieldsFoundInZip = gtfsTable.getFieldsFromFieldHeaders(csvReader.getHeaders(), null);
            int transformFieldIndex = getFieldIndex(fieldsFoundInZip, fieldName);
            while (csvReader.readRecord()) {
                String value = csvReader.get(transformFieldIndex);
                value = convertToTitleCase(value);
                // TODO: Run replacement pairs transformation
            }
            // Set transform type according to whether target file exists.
            TransformType type = Files.exists(targetTxtFilePath)
                    ? TransformType.TABLE_REPLACED
                    : TransformType.TABLE_ADDED;
            // Copy csv input stream into the zip file, replacing it if it already exists.
            //Files.copy(inputStream, targetTxtFilePath, StandardCopyOption.REPLACE_EXISTING);
            target.feedTransformResult.tableTransformResults.add(new TableTransformResult(tableName, type));
        } catch (Exception e) {
            status.fail("Unknown error encountered while transforming zip file", e);
        }
    }

    /**
     * Converts the provided string to Title Case,
     * accommodating for separator characters that may be immediately precede
     * (without spaces) names that we need to capitalize.
     */
    public static String convertToTitleCase(String inputString) {
        if (StringUtils.isBlank(inputString)) {
            return "";
        }

        final char[] separators = new char[] {' ', '/', '-', '+', '&', '@'};
        return WordUtils.capitalizeFully(inputString, separators);
    }

    public enum NormalizeOperation {
        SENTENCE_CASE, TITLE_CASE
    }
}
