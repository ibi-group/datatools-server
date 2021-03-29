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
    private static final List<String> defaultExceptions;
    public static final List<Substitution> defaultSubstitutions = Arrays.asList(
        // "+", "&" => "and"
        new Substitution("[\\+&]", "and", true),
        // "@" => "at"
        new Substitution("@", "at", true),
        // Contents between () and [], and surrounding whitespace, is removed.
        new Substitution("\\s*\\(.+\\)\\s*", ""),
        new Substitution("\\s*\\[.+\\]\\s*", "")
    );

    // TODO: Add JavaDoc.
    public String fieldName;
    public boolean capitalize = true;
    public boolean performSubstitutions = true;

    /**
     * Exceptions are initialized with the default configured ones,
     * and can be overridden from the UI.
     */
    public List<String> capitalizeExceptions = defaultExceptions;

    /**
     * A list of substitution objects that is initialized with the defaults above
     * and that can be overridden from the UI.
     */
    public List<Substitution> substitutions = new ArrayList<>(defaultSubstitutions);

    private List<Substitution> capitalizeSubstitutions;

    static {
        String defaultExceptionsConfig = getConfigPropertyAsText("DEFAULT_CAPITALIZATION_EXCEPTIONS");
        if (defaultExceptionsConfig != null) {
            defaultExceptions = Arrays.asList(defaultExceptionsConfig.split("\\s*,\\s*"));
        } else {
            defaultExceptions = new ArrayList<>();
        }
    }

    /**
     * Used in tests to create an instance of this transform.
     * @param table The table with the field to modify.
     * @param fieldName The field in the specified table to modify.
     * @param exceptions A list of capitalization exceptions:
     *                   - if null, the default configured exceptions will be used.
     *                   - if empty, no capitalization exceptions will be considered.
     * @param substitutions A list of substitutions:
     *                      - if null, the default configured substitutions will be performed.
     *                      - if empty, no substitutions will be performed.
     * @return An instance of the transformation with the desired settings.
     */
    public static NormalizeFieldTransformation create(
        String table, String fieldName, List<String> exceptions, List<Substitution> substitutions)
    {
        NormalizeFieldTransformation transformation = new NormalizeFieldTransformation();
        transformation.fieldName = fieldName;
        transformation.table = table;
        // Override configured defaults if corresponding args are not null.
        // (To set up no capitalization exceptions or no substitutions, pass an empty list.)
        if (exceptions != null) {
            transformation.capitalizeExceptions = exceptions;
        }
        if (substitutions != null) {
            transformation.substitutions = substitutions;
        }
        transformation.initializeExceptions();
        transformation.initializeSubstitutions();
        return transformation;
    }
    public static NormalizeFieldTransformation create(String table, String fieldName) {
        return create(table, fieldName, null, null);
    }

    private void initializeExceptions() {
        List<String> allExceptions = new ArrayList<>(capitalizeExceptions);

        capitalizeSubstitutions = allExceptions.stream()
            // Ensure capitalization exceptions are set to uppercase.
            .map(String::toUpperCase)
            .map(Substitution::makeCapitalizeExceptionPair)
            .collect(Collectors.toList());
    }

    private void initializeSubstitutions() {
        List<String> allExceptions = new ArrayList<>(capitalizeExceptions);

        capitalizeSubstitutions = allExceptions.stream()
            // Ensure capitalization exceptions are set to uppercase.
            .map(String::toUpperCase)
            .map(Substitution::makeCapitalizeExceptionPair)
            .collect(Collectors.toList());
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

        initializeExceptions();

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

                // Convert to title case, if requested.
                if (capitalize) {
                    transformedValue = convertToTitleCase(transformedValue);
                }

                // Run replacement pairs transformation, if requested.
                if (performSubstitutions) {
                    transformedValue = performSubstitutions(transformedValue);
                }

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
    public String convertToTitleCase(String inputString) {
        // Return at least a blank string.
        if (StringUtils.isBlank(inputString)) {
            return "";
        }

        final char[] SEPARATORS = new char[] {' ', '/', '-', '+', '&', '@'};
        String result = WordUtils.capitalizeFully(inputString, SEPARATORS);

        // Exceptions (e.g. acronyms) should remain capitalized.
        for (Substitution pair : capitalizeSubstitutions) {
            result = pair.replace(result);
        }

        return result;
    }

    /**
     * Performs substitutions defined for this transform, and returns the result.
     */
    public String performSubstitutions(String inputString) {
        String result = inputString;
        for (Substitution pair : defaultSubstitutions) {
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
    public static class Substitution {
        /** The regex string to match. */
        public String regex;
        /** The string that should replace regex (see effectiveReplacement below). */
        public String replacement;
        /** true if whitespace surrounding regex should be create or normalized to one space. */
        public boolean normalizeSpace;

        private Pattern pattern;
        /**
         * Replacement string that is actually used for the substitution,
         * and set according to the value of normalizeSpace.
         */
        private String effectiveReplacement;

        /** Constructor needed for persistence */
        public Substitution() {}

        public Substitution(String regex, String replacement) {
            this(regex, replacement, false);
        }

        public Substitution(String regex, String replacement, boolean normalizeSpace) {
            this.regex = regex;
            this.normalizeSpace = normalizeSpace;
            this.replacement = replacement;
        }

        /**
         * Pre-compiles the regex pattern and determines the actual replacement string
         * according to normalizeSpace.
         */
        private void initialize() {
            if (normalizeSpace) {
                // If normalizeSpace is set, reduce spaces before and after the regex to one space,
                // or insert one space before and one space after if there is none.
                // Note: if the replacement must be blank, then normalizeSpace should be set to false
                // and whitespace management should be handled in the regex instead.
                this.pattern = Pattern.compile(String.format("\\s*%s\\s*", regex));
                this.effectiveReplacement = String.format(" %s ", replacement);
            } else {
                this.pattern = Pattern.compile(regex);
                this.effectiveReplacement = replacement;
            }
        }

        /**
         * Perform the replacement of regex in the provided string, and return the result.
         */
        public String replace(String input) {
            if (pattern == null) {
                initialize();
            }

            // TODO: Study appendReplacement
            return pattern.matcher(input).replaceAll(effectiveReplacement);
        }

        /**
         * Generates a replacement pair for the provided capitalization exception word
         * (used in getTitleCase) by creating a regex with word boundaries (\b)
         * that surround the capitalized expression to be reverted.
         */
        public static Substitution makeCapitalizeExceptionPair(String word) {
            return new Substitution(
                String.format("\\b%s\\b", WordUtils.capitalizeFully(word)),
                word
            );
        }
    }
}
