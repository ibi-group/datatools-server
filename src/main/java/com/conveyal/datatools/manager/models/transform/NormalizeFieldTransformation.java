package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.TableTransformResult;
import com.conveyal.datatools.manager.models.TransformType;
import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.Table;
import com.csvreader.CsvReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.WordUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipFile;

import static com.conveyal.datatools.manager.DataManager.getConfigPropertyAsText;
import static com.conveyal.gtfs.loader.Field.getFieldIndex;

public class NormalizeFieldTransformation extends ZipTransformation {
    //public List<String> exceptions = defaultExceptions;
    public NormalizeOperation normalizeOperation;
    public String fieldName;

    private static final List<String> defaultExceptions;
    public static final List<ReplacementPair> CAPITALIZE_EXCEPTION_PAIRS;
    public static final List<ReplacementPair> REPLACEMENT_PAIRS = Arrays.asList(
        // "+", "&" => "and"
        new ReplacementPair("[\\+&]", "and", true),
        // "@" => "at"
        new ReplacementPair("@", "at", true),
        // Contents between () and []  (and surrounding whitespace) is removed.
        new ReplacementPair("\\s*\\(.+\\)\\s*", ""),
        new ReplacementPair("\\s*\\[.+\\]\\s*", "")
    );

    static {
        String defaultExceptionsConfig = getConfigPropertyAsText("DEFAULT_CAPITALIZATION_EXCEPTIONS");
        if (defaultExceptionsConfig != null) {
            defaultExceptions = Arrays.asList(defaultExceptionsConfig.split("\\s*,\\s*"));
            CAPITALIZE_EXCEPTION_PAIRS = defaultExceptions.stream()
                .map(ReplacementPair::makeCapitalizeExceptionPair)
                .collect(Collectors.toList());
        } else {
            defaultExceptions = new ArrayList<>();
            CAPITALIZE_EXCEPTION_PAIRS = new ArrayList<>();
        }
    }

    public static NormalizeFieldTransformation create(String table, String fieldName, List<String> exceptions) {
        NormalizeFieldTransformation transformation = new NormalizeFieldTransformation();
        transformation.fieldName = fieldName;
        transformation.table = table;
        // TODO: Add per-request exceptions below.
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

            // Output CSV, including headers.
            StringBuffer processedTableData = new StringBuffer();
            processedTableData.append(String.join(",", csvReader.getHeaders()));
            processedTableData.append("\n");

            while (csvReader.readRecord()) {
                String transformedValue = csvReader.get(transformFieldIndex);
                // Convert to title case
                // TODO: make this by request.
                transformedValue = convertToTitleCase(transformedValue);

                // Run replacement pairs transformation
                // TODO: Make this by request.
                transformedValue = performReplacements(transformedValue);

                // Re-assemble the CSV line and place in buffer.
                String[] csvValues = csvReader.getValues();
                csvValues[transformFieldIndex] = transformedValue;
                processedTableData.append(String.join(",", csvValues));
                processedTableData.append("\n");
            }

            TransformType type = TransformType.TABLE_MODIFIED;
            // Copy csv input stream into the zip file, replacing the existing file.
            InputStream inputStream =  new ByteArrayInputStream(processedTableData.toString().getBytes(StandardCharsets.UTF_8));
            Files.copy(inputStream, targetTxtFilePath, StandardCopyOption.REPLACE_EXISTING);
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
        String result = WordUtils.capitalizeFully(inputString, SEPARATORS);

        // Exceptions (e.g. acronyms) should remain capitalized.
        for (ReplacementPair pair : CAPITALIZE_EXCEPTION_PAIRS) {
            result = pair.replace(result);
        }

        return result;
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

    /**
     * This class holds the regex/replacement pair, and the cached compiled regex pattern.
     */
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

        /**
         * Perform the replacement of regex in the provided string, and return the result.
         */
        public String replace(String input) {
            // TODO: Study appendReplacement
            return pattern.matcher(input).replaceAll(replacement);
        }

        /**
         * Generates a replacement pair for the provided capitalization exception word
         * (used in getTitleCase) by creating a regex with word boundaries (\b)
         * that surround the capitalized expression to be reverted.
         */
        public static ReplacementPair makeCapitalizeExceptionPair(String exceptionWord) {
            return new ReplacementPair(
                String.format("\\b%s\\b", WordUtils.capitalizeFully(exceptionWord)),
                exceptionWord
            );
        }
    }
}
