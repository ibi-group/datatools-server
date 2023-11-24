package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.TableTransformResult;
import com.conveyal.datatools.manager.utils.GtfsUtils;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
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
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.ZipFile;

import static com.conveyal.datatools.manager.DataManager.getConfigProperty;
import static com.conveyal.gtfs.loader.Field.getFieldIndex;

/**
 * This transformation normalizes string values for a given field in a GTFS table by:
 * - changing the case of field values,
 * - substituting certain strings with others.
 * Exceptions to capitalization and substitutions are configurable application-wide in env.yml
 * or for each transform individually using datatools-ui.
 */
public class NormalizeFieldTransformation extends ZipTransformation {
    /**
     * Different capitalization styles to apply.
     */
    public enum CapitalizationStyle {
        TITLE_CASE
    }

    private static final Logger LOG = LoggerFactory.getLogger(NormalizeFieldTransformation.class);
    public static final String CAPITALIZATION_EXCEPTIONS_CONFIG_PATH = "modules.manager.normalizeFieldTransformation.defaultCapitalizationExceptions";
    public static final String SUBSTITUTION_CONFIG_PATH = "modules.manager.normalizeFieldTransformation.defaultSubstitutions";

    private static final List<String> DEFAULT_CAPITALIZATION_EXCEPTIONS = JsonUtil.getPOJOFromJSONAsList(
        getConfigProperty(CAPITALIZATION_EXCEPTIONS_CONFIG_PATH),
        String.class
    );
    private static final List<Substitution> DEFAULT_SUBSTITUTIONS = JsonUtil.getPOJOFromJSONAsList(
        getConfigProperty(SUBSTITUTION_CONFIG_PATH),
        Substitution.class
    );
    // Common separator characters found on the US-English keyboard.
    private static final char[] SEPARATORS = " \t\n`~!@#$%^&*()-_=+[{]}\\|;:'\",<.>/?".toCharArray();

    /** The field name in the GTFS table being transformed. */
    public String fieldName;

    /** Whether to perform capitalization. */
    public boolean capitalize = true;

    /** Capitalization style, e.g. title case or sentence case. */
    public CapitalizationStyle capitalizationStyle = CapitalizationStyle.TITLE_CASE;

    /**
     * Capitalization exceptions is for text that should remain as specified
     * in the default capitalization exceptions or in the UI.
     * (e.g., acronyms remain in uppercase, particles such as 'de' remain in lowercase.)
     */
    public List<String> capitalizationExceptions = DEFAULT_CAPITALIZATION_EXCEPTIONS;

    /**
     * Substitutions are initialized with the default configured ones,
     * and can be overridden from the UI. This list can be empty.
     */
    public List<Substitution> substitutions = DEFAULT_SUBSTITUTIONS;

    // This field is reset when the setter is called
    // and initialized when executing the transform method.
    private List<Substitution> capitalizationSubstitutions;

    private List<Substitution> getCapitalizationSubstitutions() {
        if (capitalizationSubstitutions == null) {
            initializeCapitalizeSubstitutions();
        }
        return capitalizationSubstitutions;
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
    static NormalizeFieldTransformation create(
        String table, String fieldName, List<String> exceptions, List<Substitution> substitutions)
    {
        NormalizeFieldTransformation transformation = new NormalizeFieldTransformation();
        transformation.fieldName = fieldName;
        transformation.table = table;
        // Override configured defaults if corresponding args are not null.
        // (To set up no capitalization exceptions or no substitutions, pass an empty string.)
        if (exceptions != null) {
            transformation.capitalizationExceptions = exceptions;
        }
        if (substitutions != null) {
            transformation.substitutions = substitutions;
        }
        return transformation;
    }

    /**
     * Initializes capitalizeSubstitutions so that it is a non-null list.
     */
    private void initializeCapitalizeSubstitutions() {
        capitalizationSubstitutions = capitalizationExceptions == null
            ? new ArrayList<>()
            : capitalizationExceptions.stream()
                .map(word -> new Substitution(
                    // TODO: support other capitalization styles.
                    String.format("\\b%s\\b", WordUtils.capitalizeFully(word)),
                    word
                ))
                .collect(Collectors.toList());
    }

    @Override
    public void validateParameters(MonitorableJob.Status status) {
        // fieldName must not be null
        if (fieldName == null) {
            status.fail("Field name must not be null");
            return;
        }

        // Substitutions must have valid patterns (gather invalid patterns).
        List<String> invalidPatterns = getInvalidSubstitutionPatterns(substitutions);
        if (!invalidPatterns.isEmpty()) {
            status.fail(getInvalidSubstitutionMessage(invalidPatterns));
        }
    }

    /**
     * @return A formatted error message regarding invalid substitution search patterns.
     */
    public static String getInvalidSubstitutionMessage(List<String> invalidPatterns) {
        return String.format(
            "Some substitution patterns are invalid: %s",
            String.join(", ", invalidPatterns)
        );
    }

    /**
     * @return a list of invalid substitution search patterns.
     */
    public static List<String> getInvalidSubstitutionPatterns(List<Substitution> substitutions) {
        List<String> invalidPatterns = new ArrayList<>();
        for (Substitution substitution : substitutions) {
            if (!substitution.isValid()) {
                invalidPatterns.add(substitution.pattern);
            }
        }
        return invalidPatterns;
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
            Path tempZipPath = Files.createTempFile("field-normalization", ".zip");
            Path originalZipPath = Paths.get(zipTarget.gtfsFile.getAbsolutePath());

            // Create a temporary working zip file from original.
            Files.copy(originalZipPath, tempZipPath, StandardCopyOption.REPLACE_EXISTING);

            Table gtfsTable = GtfsUtils.getGtfsTable(table);
            CsvReader csvReader = gtfsTable.getCsvReader(new ZipFile(tempZipPath.toAbsolutePath().toString()), null);
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
                    if (capitalizationStyle == CapitalizationStyle.TITLE_CASE) {
                        transformedValue = convertToTitleCase(transformedValue);
                    }
                    // TODO: Implement other capitalization styles.
                }

                // Perform substitutions if any.
                transformedValue = performSubstitutions(transformedValue);

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
            csvReader.close();
            writer.flush();

            // Copy csv input stream into the zip file, replacing the existing file.
            try (
                // Modify target zip file that we just read.
                FileSystem targetZipFs = FileSystems.newFileSystem(tempZipPath, (ClassLoader) null);
                // Stream for file copy operation.
                InputStream inputStream =  new ByteArrayInputStream(stringWriter.toString().getBytes(StandardCharsets.UTF_8))
            ) {
                Path targetTxtFilePath = getTablePathInZip(tableName, targetZipFs);
                Files.copy(inputStream, targetTxtFilePath, StandardCopyOption.REPLACE_EXISTING);
                zipTarget.feedTransformResult.tableTransformResults.add(
                    new TableTransformResult(tableName, 0, modifiedRowCount, 0)
                );
            }

            // Replace original zip file with temporary working zip file.
            // (This should also trigger a system IO update event, so subsequent IO calls pick up the correct file.
            Files.move(tempZipPath, originalZipPath, StandardCopyOption.REPLACE_EXISTING);
            LOG.info("Field normalization transformation successful, {} row(s) changed.", modifiedRowCount);
        } catch (Exception e) {
            status.fail("Unknown error encountered while transforming zip file", e);
        }
    }

    /**
     * Converts the provided string to Title Case, accommodating for capitalization exceptions
     * and separator characters that may be immediately precede
     * (without spaces) text that we need to capitalize.
     */
    public String convertToTitleCase(String inputString) {
        // Return at least a blank string.
        if (StringUtils.isBlank(inputString)) {
            return "";
        }

        String result = WordUtils.capitalizeFully(inputString, SEPARATORS);

        // Exceptions should remain as specified (e.g. acronyms).
        for (Substitution substitution : getCapitalizationSubstitutions()) {
            result = substitution.replaceAll(result);
        }

        return result;
    }

    /**
     * Performs substitutions defined for this transform, and returns the result.
     */
    public String performSubstitutions(String inputString) {
        if (substitutions == null) return inputString;
        String result = inputString;
        for (Substitution substitution : substitutions) {
            result = substitution.replaceAll(result);
        }
        return result;
    }
}
