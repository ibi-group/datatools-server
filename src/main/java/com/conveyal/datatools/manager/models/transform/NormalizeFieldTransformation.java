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
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.ZipFile;

import static com.conveyal.datatools.manager.DataManager.getConfigPropertyAsText;
import static com.conveyal.gtfs.loader.Field.getFieldIndex;

public class NormalizeFieldTransformation extends ZipTransformation {
    //private final List<String> defaultExceptions = getConfigPropertyAsText("DEFAULT_CAPITALIZATION_EXCEPTIONS");
    public String fieldName;
    public NormalizeOperation normalizeOperation;
    //public List<String> exceptions = defaultExceptions;

    public static final List<ReplacementPair> REPLACEMENT_PAIRS = Arrays.asList(
        // "+", "&" => "and"
        new ReplacementPair("[\\+&]", "and", true),
        // "@" => "at"
        new ReplacementPair("@", "at", true),
        // Contents in parentheses (and surrounding whitespace) is removed.
        new ReplacementPair("\\s*\\(.+\\)\\s*", "")
    );

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
        // Return at least a blank string.
        if (StringUtils.isBlank(inputString)) {
            return "";
        }

        final char[] SEPARATORS = new char[] {' ', '/', '-', '+', '&', '@'};
        return WordUtils.capitalizeFully(inputString, SEPARATORS);
    }

    /**
     * Replaces preset strings with replacements defined in REPLACEMENT_PAIRS,
     * and returns the result.
     */
    public static String performReplacements(String inputString) {
        String result = inputString;
        for (ReplacementPair pair : REPLACEMENT_PAIRS) {
            result = pair.replace(result);
        }
        return result;
    }


    public enum NormalizeOperation {
        SENTENCE_CASE, TITLE_CASE
    }

    private static class ReplacementPair {
        public String regex;
        public String replacement;
        public Pattern pattern;

        public ReplacementPair(String regex, String replacement) {
            this(regex, replacement, false);
        }

        public ReplacementPair(String regex, String replacement, boolean normalizeSpace) {
            this.regex = regex;

            if (normalizeSpace) {
                // If normalizeSpace is set, reduce spaces before and after the regex to one space,
                // or insert one space before and one space after if there is none.
                // Note: if the replacement must be blank, then normalizeSpace should be set to false
                // and whitespace management should be handled in the regex instead.
                this.pattern = Pattern.compile(String.format("\\s*%s\\s*", regex));
                this.replacement = String.format(" %s ", replacement);
            } else {
                this.pattern = Pattern.compile(regex);
                this.replacement = replacement;
            }
        }

        public String replace(String input) {
            // TODO: Study appendReplacement
            return pattern.matcher(input).replaceAll(replacement);
        }
    }
}
