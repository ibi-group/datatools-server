package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.TableTransformResult;
import com.conveyal.datatools.manager.models.TransformType;
import org.supercsv.io.CsvMapReader;
import com.conveyal.gtfs.loader.Table;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.supercsv.io.CsvMapWriter;
import org.supercsv.prefs.CsvPreference;


/**
 * This feed transformation will attempt to preserve any custom fields from an entered csv in the final GTFS output.
 */
public class PreserveCustomFieldsTransformation extends ZipTransformation {
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

    private static HashMap<String, Map<String, String>> createCsvHashMap(CsvMapReader reader, String[] headers) throws IOException {
        HashMap<String, Map<String, String>> lookup = new HashMap<>();
        Map<String, String> nextLine;
        while ((nextLine = reader.read(headers)) != null) {
            List<String> customCsvKeyValues = tablePrimaryKeys.stream().map(nextLine::get).collect(Collectors.toList());
            String hashKey = StringUtils.join(customCsvKeyValues, "_");
            lookup.put(hashKey, nextLine);
        }
        return lookup;
    }

    @Override
    public void transform(FeedTransformZipTarget zipTarget, MonitorableJob.Status status) {
        String tableName = table + ".txt";
        Path targetZipPath = Paths.get(zipTarget.gtfsFile.getAbsolutePath());

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

            CsvMapReader newEditorFileReader = new CsvMapReader(new FileReader(tempFile), CsvPreference.STANDARD_PREFERENCE);
            CsvMapReader newCustomFileReader = new CsvMapReader(new StringReader(csvData), CsvPreference.STANDARD_PREFERENCE);

            String[] customHeaders = newCustomFileReader.getHeader(true);
            final String[] editorHeaders = newEditorFileReader.getHeader(true);

            List<String> customFields = Arrays.stream(customHeaders).filter(h -> !specTableFields.contains(h)).collect(Collectors.toList());
            if (customFields.size() == 0) return;
            String[] fullHeaders = ArrayUtils.addAll(editorHeaders, customFields.toArray(new String[0]));

            HashMap<String, Map<String, String>> lookup = createCsvHashMap(newCustomFileReader, customHeaders);
            CsvMapWriter writer = new CsvMapWriter(new FileWriter(output), CsvPreference.STANDARD_PREFERENCE);
            writer.writeHeader(fullHeaders);

            Map<String, String> row;
            while ((row = newEditorFileReader.read(editorHeaders)) != null) {
                List<String> editorCsvPrimaryKeyValues = tablePrimaryKeys.stream()
                    .map(row::get)
                    .collect(Collectors.toList());

                String hashKey = StringUtils.join(editorCsvPrimaryKeyValues, "_");
                Map<String, String> customCsvValues = lookup.get(hashKey);
                Map<String, String> finalRow = row;
                customFields.stream().forEach(customField -> {
                    String value = customCsvValues == null ? null : customCsvValues.get(customField);
                    finalRow.put(customField, value);
                });
                writer.write(finalRow, fullHeaders);
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
        } catch (Exception e) {
            status.fail("Unknown error encountered while transforming zip file", e);
        }
    }
}
