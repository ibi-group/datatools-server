package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.TableTransformResult;
import com.conveyal.datatools.manager.models.TransformType;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.commons.io.input.BOMInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.conveyal.gtfs.loader.Table;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.WordUtils;



/**
 * This feed transformation will attempt to preserve any custom fields from an entered csv in the final GTFS output.
 */
public class PreserveCustomFieldsTransformation extends ZipTransformation {
    private static final Logger LOG = LoggerFactory.getLogger(PreserveCustomFieldsTransformation.class);
    private static List<String> tablePrimaryKeys = new ArrayList<>();
    private static Table specTable;
    /** no-arg constructor for de/serialization */
    public PreserveCustomFieldsTransformation() {}

    public static PreserveCustomFieldsTransformation create(String sourceVersionId, String table) {
        PreserveCustomFieldsTransformation transformation = new PreserveCustomFieldsTransformation();
        transformation.sourceVersionId = sourceVersionId;
        transformation.table = table;
        return transformation;
    }

    @Override
    public void validateParameters(MonitorableJob.Status status) {
        if (csvData == null) {
            status.fail("CSV data must not be null (delete table not yet supported)");
        }
    }

    public static <T> Collector<T, ?, T> toSingleton() {
        return Collectors.collectingAndThen(
                Collectors.toList(),
                list -> {
                    if (list.size() != 1) {
                        throw new IllegalStateException();
                    }
                    return list.get(0);
                }
        );
    }

    private static HashMap<String, String[]> createCsvHashMap(CSVReader reader, List<Integer> primaryKeys) throws CsvValidationException, IOException {
        HashMap<String, String[]> lookup = new HashMap<>();

        String[] nextLine;
        while ((nextLine = reader.readNext()) != null) {
            final String[] finalNextLine = nextLine;
            List<String> customCsvKeyValues = primaryKeys.stream().map(column -> finalNextLine[column]).collect(Collectors.toList());

            // Concatenate keys to make a lookup hash and add to the hash map
            String hashKey = StringUtils.join(customCsvKeyValues, "_");
            lookup.put(hashKey, finalNextLine);
        }

        return lookup;
    }

    private static void writeLine(CSVWriter writer, String[] row, List<String> customFields, Map<String, Integer> customHeaders, String[] customValues) {
        // Add new custom fields to the editor csv rows
        String[] newRow = Arrays.copyOf(row, row.length + customFields.size());
        if (customValues != null) {
            // Write the custom values, if we have a match
            for (int columnDiff = 0; columnDiff < customFields.size(); columnDiff++) {
                String customField = customFields.get(columnDiff);
                int customFieldIndex = customHeaders.get(customField);
                newRow[row.length + columnDiff] = customValues[customFieldIndex];
            }
        }
        writer.writeNext(newRow);
    }

    private static Map<String, Integer> mapHeaderColumns(String[] headers) {
        Map<String, Integer> headersMapping = new HashMap<>();
        for (int i = 0; i < headers.length; i++) headersMapping.put(headers[i], i);
        return headersMapping;
    }

    private static String getClassNameFromTable (String table) {
        String underscoreRemoved = table.replace("_", " ");
        String capitalized = WordUtils.capitalize(underscoreRemoved);
        String oneWordName = capitalized.replace(" ", "");
        if (oneWordName.substring(oneWordName.length() - 1).equals("s")) {
            oneWordName = oneWordName.substring(0, oneWordName.length() - 1);
        }
        return "com.conveyal.model." + oneWordName + "DTO";
    }

    @Override
    public void transform(FeedTransformZipTarget zipTarget, MonitorableJob.Status status) {
        String tableName = table + ".txt";
        Path targetZipPath = Paths.get(zipTarget.gtfsFile.getAbsolutePath());
        // Try to dynamically load the class for CSVBean
        //        String csvDataClassName = getClassNameFromTable(table);
//            Class<?> csvDataClass = Class.forName(csvDataClassName);

        // TODO: is there a better way to do this than using a Singleton collector?
        specTable = Arrays.stream(Table.tablesInOrder)
                .filter(t -> t.name.equals(table))
                .collect(toSingleton());
        tablePrimaryKeys = specTable.getPrimaryKeyNames();

        try( FileSystem targetZipFs = FileSystems.newFileSystem(targetZipPath, (ClassLoader) null) ){
            List<String> specTableFields = specTable.specFields().stream().map(f -> f.name).collect(Collectors.toList());
            Path targetTxtFilePath = getTablePathInZip(tableName, targetZipFs);

            // TODO: There must be a better way to do this.
            InputStream is = Files.newInputStream(targetTxtFilePath);
            final File tempFile = File.createTempFile(tableName + "-temp", ".txt");
            File output = File.createTempFile(tableName + "-output-temp", ".txt");
            FileOutputStream out = new FileOutputStream(tempFile);
            IOUtils.copy(is, out);

            FileInputStream fileInputStream = new FileInputStream(tempFile);
            // BOMInputStream to avoid any Byte Order Marks at the start of files.
            CSVReader editorFileReader = new CSVReader(new InputStreamReader(new BOMInputStream(fileInputStream), StandardCharsets.UTF_8));
            CSVReader customFileReader = new CSVReader(new StringReader(csvData));

            // Store the headers with their indices in CSV for later lookups
            String[] customHeaders = customFileReader.readNext();
            String[] editorHeaders = editorFileReader.readNext();
            Map<String, Integer> customCsvHeaders = mapHeaderColumns(customHeaders);
            Map<String, Integer> editorCsvHeaders = mapHeaderColumns(editorHeaders);

            // Find the customFields in the input csv
            List<String> customFields = Arrays.stream(customHeaders).filter(h -> !specTableFields.contains(h)).collect(Collectors.toList());
            if (customFields.size() == 0) return;

            // Find the key columns in the custom CSV
            List<Integer> customCsvKeyColumns = tablePrimaryKeys.stream()
                .map(customCsvHeaders::get)
                .collect(Collectors.toList());

            HashMap<String, String[]> lookup = createCsvHashMap(customFileReader, customCsvKeyColumns);
            CSVWriter writer = new CSVWriter(new FileWriter(output));
            writeLine(writer, editorHeaders, customFields, customCsvHeaders, customHeaders);  // Write headers before starting lookups

            String[] nextLine;
            while((nextLine = editorFileReader.readNext()) != null) {
                String[] finalNextLine = nextLine; // TODO: there must be some way around this.
                List<String> editorCsvPrimaryKeyValues = tablePrimaryKeys.stream()
                        .map(key -> finalNextLine[editorCsvHeaders.get(key)])
                        .collect(Collectors.toList()); // Map the keys to the values for the row

                String hashKey = StringUtils.join(editorCsvPrimaryKeyValues, "_");
                String[] customCsvLine = lookup.get(hashKey);
                writeLine(writer, nextLine, customFields, customCsvHeaders, customCsvLine);
            }
            writer.close();

            Files.copy(output.toPath(), targetTxtFilePath, StandardCopyOption.REPLACE_EXISTING);
            tempFile.deleteOnExit();
            output.deleteOnExit();
            zipTarget.feedTransformResult.tableTransformResults.add(new TableTransformResult(tableName, TransformType.TABLE_MODIFIED));
        } catch (NoSuchFileException e) {
            status.fail("Source version does not contain table: " + tableName, e);
        } catch(IOException e) {
            status.fail("An exception occurred when writing output with custom fields", e);
        } catch (CsvValidationException ex) {
            ex.printStackTrace();
        } catch (Exception e) {
            status.fail("Unknown error encountered while transforming zip file", e);
        }
    }
}
