package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.TableTransformResult;
import com.conveyal.datatools.manager.models.TransformType;
import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.Table;
import com.csvreader.CsvReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.WordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOG = LoggerFactory.getLogger(NormalizeFieldTransformation.class);

    private static final String defaultExceptions = getConfigPropertyAsText("DEFAULT_CAPITALIZATION_EXCEPTIONS");
    private static final String defaultSubstitutions = getConfigPropertyAsText("DEFAULT_SUBSTITUTIONS");

    // Common separator characters found on the English keyboard.
    private static final char[] SEPARATORS = " \t\n`~!@#$%^&*()-_=+[{]}\\|;:'\",<.>/?".toCharArray();
    private static final char NORMALIZE_SPACE_PREFIX = '+';
    private static final String SUBSTITUTION_SEPARATOR = "=>";

    // TODO: Add JavaDoc.
    public String fieldName;
    public boolean capitalize = true;
    public boolean performSubstitutions = true;

    /**
     * Exceptions are initialized with the default configured ones,
     * and can be overridden from the UI.
     */
    public String capitalizeExceptions = defaultExceptions;

    /**
     * Substitutions are initialized with the default configured ones,
     * and can be overridden from the UI.
     */
    public String substitutions = defaultSubstitutions;

    // These fields are reset when the setter is called
    // and initialized when executing the transform method.
    private List<Substitution> capitalizeSubstitutions;
    private List<Substitution> stringSubstitutions;

    private List<Substitution> getCapitalizeSubstitutions() {
        if (capitalizeSubstitutions == null) {
            initializeCapitalizeSubstitutions();
        }
        return capitalizeSubstitutions;
    }

    private List<Substitution> getStringSubstitutions() {
        if (stringSubstitutions == null) {
            initializeStringSubstitutions();
        }
        return stringSubstitutions;
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
        String table, String fieldName, String exceptions, String substitutions)
    {
        NormalizeFieldTransformation transformation = new NormalizeFieldTransformation();
        transformation.fieldName = fieldName;
        transformation.table = table;
        // Override configured defaults if corresponding args are not null.
        // (To set up no capitalization exceptions or no substitutions, pass an empty string.)
        if (exceptions != null) {
            transformation.capitalizeExceptions = exceptions;
        }
        if (substitutions != null) {
            transformation.substitutions = substitutions;
        }
        return transformation;
    }
    public static NormalizeFieldTransformation create(String table, String fieldName) {
        return create(table, fieldName, null, null);
    }

    /**
     * Initializes capitalizeSubstitutions so that it is a non-null list.
     */
    private void initializeCapitalizeSubstitutions() {
        capitalizeSubstitutions = StringUtils.isBlank(capitalizeExceptions)
            ? new ArrayList<>()
            : Arrays.stream(capitalizeExceptions.split("\\s*,\\s*"))
                // Ensure capitalization exceptions are set to uppercase.
                .map(String::toUpperCase)
                .map(Substitution::makeCapitalizeSubstitution)
                .collect(Collectors.toList());
    }

    /**
     * Initializes stringSubstitutions so that it is a non-null list.
     */
    private void initializeStringSubstitutions() {
        stringSubstitutions = StringUtils.isBlank(substitutions)
            ? new ArrayList<>()
            : Arrays.stream(substitutions.split("\\s*,\\s*"))
            .map(Substitution::makeStringSubstitution)
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

                // Perform substitutions, if requested.
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
            // TODO: Add stats on number of records changed.

            target.feedTransformResult.tableTransformResults.add(new TableTransformResult(tableName, type));

            LOG.info("Field normalization transformation successful!");
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

        String result = WordUtils.capitalizeFully(inputString, SEPARATORS);

        // Exceptions (e.g. acronyms) should remain capitalized.
        for (Substitution substitution : getCapitalizeSubstitutions()) {
            result = substitution.replace(result);
        }

        return result;
    }

    /**
     * Performs substitutions defined for this transform, and returns the result.
     */
    public String performSubstitutions(String inputString) {
        String result = inputString;
        for (Substitution substitution : getStringSubstitutions()) {
            result = substitution.replace(result);
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

        /** Empty constructor needed for persistence */
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
         * Generates a substitution for the provided capitalization exception word
         * (used in e.g. getTitleCase) by creating a regex with word boundaries (\b)
         * that surround the capitalized expression to be reverted.
         */
        public static Substitution makeCapitalizeSubstitution(String word) {
            return new Substitution(
                String.format("\\b%s\\b", WordUtils.capitalizeFully(word)),
                word
            );
        }

        /**
         * Generates a substitution for the provided pair in format "old => new"
         * (or "old =>+ new" if space normalization is needed).
         */
        public static Substitution makeStringSubstitution(String substitutionParts) {
            String[] parts = substitutionParts.split(SUBSTITUTION_SEPARATOR);
            // Normalize space if the second portion starts with NORMALIZE_SPACE_PREFIX
            // (remove that prefix from the replaced text).
            boolean normalizeSpace = false;
            String replacement = "";

            if (parts.length >= 2) {
                if (parts.length >= 2 && parts[1].charAt(0) == NORMALIZE_SPACE_PREFIX) {
                    normalizeSpace = true;
                    replacement = parts[1].substring(1).trim();
                } else {
                    replacement = parts[1].trim();
                }
            }
            return new Substitution(parts[0].trim(), replacement, normalizeSpace);
        }
    }
}
