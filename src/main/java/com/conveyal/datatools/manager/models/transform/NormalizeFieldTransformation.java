package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.TableTransformResult;
import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.Table;
import com.csvreader.CsvReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.WordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.StringWriter;
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

    private static final String DEFAULT_EXCEPTIONS = getConfigPropertyAsText("DEFAULT_CAPITALIZATION_EXCEPTIONS");
    private static final String DEFAULT_SUBSTITUTIONS = getConfigPropertyAsText("DEFAULT_SUBSTITUTIONS");
    // Common separator characters found on the US-English keyboard.
    private static final char[] SEPARATORS = " \t\n`~!@#$%^&*()-_=+[{]}\\|;:'\",<.>/?".toCharArray();
    private static final char NORMALIZE_SPACE_PREFIX = '+';
    private static final String SUBSTITUTION_SEPARATOR = "=>";

    /** The field name in the GTFS table being transformed. */
    public String fieldName;

    /** Whether to perform capitalizations. */
    public boolean capitalize = true;

    /** Whether to perform text substitutions. */
    public boolean performSubstitutions = true;

    /**
     * Capitalization exceptions are initialized with the default configured ones,
     * and can be overridden from the UI.
     */
    public String capitalizeExceptions = DEFAULT_EXCEPTIONS;

    /**
     * Substitutions are initialized with the default configured ones,
     * and can be overridden from the UI.
     */
    public String substitutions = DEFAULT_SUBSTITUTIONS;

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
    public void validateParameters(MonitorableJob.Status status) {
        if (fieldName == null) {
            status.fail("Field name must not be null");
        }
    }

    @Override
    public void transform(FeedTransformZipTarget zipTarget, MonitorableJob.Status status) {
        String tableName = table + ".txt";
        try(
            // Hold output before writing to ZIP
            StringWriter stringWriter = new StringWriter();
            // CSV writer used to write to zip file.
            CsvListWriter writer = new CsvListWriter(stringWriter, CsvPreference.STANDARD_PREFERENCE)
        ) {
            File tempZipFile = File.createTempFile("field-normalization", "zip");
            Path originalZipPath = Paths.get(zipTarget.gtfsFile.getAbsolutePath());
            Path tempZipPath = Paths.get(tempZipFile.getAbsolutePath());

            // Create a temporary working zip file that will replace the original.
            Files.copy(originalZipPath, tempZipPath, StandardCopyOption.REPLACE_EXISTING);

            Table gtfsTable = Arrays.stream(Table.tablesInOrder).filter(t -> t.name.equals(table)).findFirst().get();
            CsvReader csvReader = gtfsTable.getCsvReader(new ZipFile(tempZipFile.getAbsolutePath()), null);
            final String[] headers = csvReader.getHeaders();
            Field[] fieldsFoundInZip = gtfsTable.getFieldsFromFieldHeaders(headers, null);
            int transformFieldIndex = getFieldIndex(fieldsFoundInZip, fieldName);

            int modifiedRowCount = 0;

            // Write headers and processed CSV rows.
            writer.write(headers);
            while (csvReader.readRecord()) {
                String originalValue = csvReader.get(transformFieldIndex);
                String transformedValue = originalValue;

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

                // Write line to table (plus new line char).
                writer.write(csvValues);

                // Count number of CSV rows changed.
                if (!originalValue.equals(transformedValue)) {
                    modifiedRowCount++;
                }
            } // End of iteration over each row.

            writer.flush();

            // Copy csv input stream into the zip file, replacing the existing file.
            try (
                // Modify target zip file that we just read.
                FileSystem targetZipFs = FileSystems.newFileSystem(tempZipPath, null);
                // Stream for file copy operation.
                InputStream inputStream =  new ByteArrayInputStream(stringWriter.toString().getBytes(StandardCharsets.UTF_8))
            ) {
                Path targetTxtFilePath = getTablePathInZip(tableName, targetZipFs);
                Files.copy(inputStream, targetTxtFilePath, StandardCopyOption.REPLACE_EXISTING);
                zipTarget.feedTransformResult.tableTransformResults.add(
                    new TableTransformResult(tableName, 0, modifiedRowCount, 0)
                );
            }

            // Replace original zip file with temporary working zip file/
            // (This should also trigger a system IO update event, so subsequent IO calls pick up the correct file.
            Files.move(tempZipPath, originalZipPath, StandardCopyOption.REPLACE_EXISTING);
            LOG.info("Field normalization transformation successful ({} row(s) changed).", modifiedRowCount);
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
                if (parts[1].charAt(0) == NORMALIZE_SPACE_PREFIX) {
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
