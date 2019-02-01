package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.common.utils.Consts;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import static com.conveyal.datatools.manager.utils.StringUtils.getCleanName;

public class MergeFeedsJob extends MonitorableJob {

    private static final Logger LOG = LoggerFactory.getLogger(MergeFeedsJob.class);
    public static final ObjectMapper mapper = new ObjectMapper();
    private final Set<FeedVersion> feedVersions;
    public MergeFeedsResult mergeFeedsResult;
    private final String filename;
    private final MergeType mergeType;

    public MergeFeedsJob(String owner, Set<FeedVersion> feedVersions, String filename, MergeType mergeType) {
        super(owner, mergeType.equals(MergeType.REGIONAL) ? "Merging project feeds" : "Merging feed versions", JobType.MERGE_FEED_VERSIONS);
        this.feedVersions = feedVersions;
        this.filename = filename;
        this.mergeType = mergeType;
        this.mergeFeedsResult = new MergeFeedsResult();
    }

    public enum MergeType {
        REGIONAL,
        FEED_VERSIONS
    }

    @Override
    public void jobLogic() throws IOException {
        // Create temp zip file to add merged feed content to.
        File mergedTempFile = null;
        try {
            try {
                mergedTempFile = File.createTempFile(filename, ".zip");
                mergedTempFile.deleteOnExit();
            } catch (IOException e) {
                LOG.error("Could not create temp file");
                throw e;
            }

            // create the zipfile
            ZipOutputStream out = new ZipOutputStream(new FileOutputStream(mergedTempFile));

            LOG.info("Created project merge file: " + mergedTempFile.getAbsolutePath());

            // Map of feed versions to table entries contained within version's GTFS.
            Map<FeedVersion, ZipFile> zipFilesForVersions = new HashMap<>();

            // Collect zipFiles for each feedSource before merging tables.
            // FIXME : This needs to handle more than one version for the same feed source.
            for (FeedVersion version : feedVersions) {
                // modify feed version to use prepended feed id
                LOG.info("Adding {} feed to merged zip", version.parentFeedSource().name);
                try {
                    File file = version.retrieveGtfsFile();
                    if (file == null) {
                        LOG.error("No file exists for {}", version.id);
                        continue;
                    }
                    ZipFile zipFile = new ZipFile(file);
                    zipFilesForVersions.put(version, zipFile);
                } catch(Exception e) {
                    LOG.error("Zipfile for version {} not found", version.id);
                    throw e;
                }
            }

            // Determine which tables to merge (only merge GTFS+ tables for MTC extension).
            final ArrayNode tablesToMerge = DataManager.isExtensionEnabled("mtc")
                ? ((ArrayNode) DataManager.gtfsConfig).addAll((ArrayNode) DataManager.gtfsPlusConfig)
                : (ArrayNode) DataManager.gtfsConfig;
            int numberOfTables = tablesToMerge.size();
            // Loop over GTFS tables and merge each feed one table at a time.
            for (int i = 0; i < numberOfTables; i++) {
                JsonNode tableNode = tablesToMerge.get(i);
                byte[] tableOut = constructMergedTable(tableNode, zipFilesForVersions);

                // If at least one feed has the table (i.e., tableOut is not null), include it in the merged feed.
                if (tableOut != null) {
                    String tableName = tableNode.get("name").asText();
                    double percentComplete = Math.round((double) i / numberOfTables * 10000d) / 100d;
                    status.update( "Merging " + tableName, percentComplete);
                    // Create entry for zip file.
                    ZipEntry tableEntry = new ZipEntry(tableName);
                    LOG.info("Writing {} to merged feed", tableName);
                    try {
                        out.putNextEntry(tableEntry);
                        out.write(tableOut);
                        out.closeEntry();
                    } catch (IOException e) {
                        String message = String.format("Error writing to table %s", tableName);
                        LOG.error(message, e);
                        status.fail(message, e);
                    }
                }
            }
            // Close output stream for zip file.
            out.close();
            // Handle writing file to storage (local or s3).
            if (mergeType.equals(MergeType.REGIONAL)) {
                status.update(false, "Saving merged feed.", 95);
                // Store the project merged zip locally or on s3
                if (DataManager.useS3) {
                    String s3Key = "project/" + filename;
                    FeedStore.s3Client.putObject(DataManager.feedBucket, s3Key, mergedTempFile);
                    LOG.info("Storing merged project feed at s3://{}/{}", DataManager.feedBucket, s3Key);
                } else {
                    try {
                        FeedVersion.feedStore.newFeed(filename, new FileInputStream(mergedTempFile), null);
                    } catch (IOException e) {
                        e.printStackTrace();
                        LOG.error("Could not store feed for project {}", filename);
                        throw e;
                    }
                }
            } else {
                // Create a new feed version from the file.
                // Feed source should be the same for each version if using the FEED_VERSIONS merge type.
                FeedSource source = feedVersions.iterator().next().parentFeedSource();
                FeedVersion mergedVersion = new FeedVersion(source);
                try {
                    FeedVersion.feedStore.newFeed(mergedVersion.id, new FileInputStream(mergedTempFile), null);
                } catch (IOException e) {
                    LOG.error("Could not store merged feed for new version");
                    throw e;
                }
                // Handle the processing of the new merged version (note: s3 upload is handled within this job.
                ProcessSingleFeedJob processSingleFeedJob = new ProcessSingleFeedJob(mergedVersion, owner, true);
                addNextJob(processSingleFeedJob);
            }
            status.update(false, "Merged feed created successfully.", 100, true);
        } catch (Exception e) {
            LOG.error("Failed to merge feeds");
            // Bubble up exception to MonitorableJob
            throw e;
        } finally {
            // Delete temp file in finally clause to ensure it is deleted even if the job fails.
            mergedTempFile.delete();
        }
    }

    /**
     * Merge the specified table for multiple GTFS feeds.
     * @param tableNode tableNode to merge
     * @param zipFilesForVersions map of feedSources to zipFiles from which to extract the .txt tables
     * @return single merged table for feeds or null if the table did not exist for any feed
     */
    private static byte[] constructMergedTable(JsonNode tableNode, Map<FeedVersion, ZipFile> zipFilesForVersions) throws IOException {

        String tableName = tableNode.get("name").asText();
        ByteArrayOutputStream tableOut = new ByteArrayOutputStream();
        ArrayNode fieldsNode = mapper.createArrayNode();
        List<String> headers = new ArrayList<>();
        for (JsonNode fieldNode : tableNode.get("fields")) {
            String fieldName = fieldNode.get("name").asText();
            // Clean up spec file to exclude datatools/editor specific fields for the merge job.
            boolean isSpecField = !(fieldNode.has("datatools") && fieldNode.get("datatools").asBoolean());
            if (isSpecField) {
                fieldsNode.add(fieldNode);
                headers.add(fieldName);
            }
        }

        try {
            // write headers to table
            tableOut.write(String.join(",", headers).getBytes());
            tableOut.write("\n".getBytes());

            // Iterate over each zip file.
            for ( Map.Entry<FeedVersion, ZipFile> mapEntry : zipFilesForVersions.entrySet()) {
                FeedVersion version = mapEntry.getKey();
                FeedSource fs = version.parentFeedSource();
                // Generate ID prefix to scope GTFS identifiers to avoid conflicts.
                String idScope = getCleanName(fs.name) + version.version;
                ZipFile zipFile = mapEntry.getValue();
                // Get a list of the files contained within the zip file to find the table we're currently working on.
                final Enumeration<? extends ZipEntry> entries = zipFile.entries();
                while (entries.hasMoreElements()) {
                    final ZipEntry entry = entries.nextElement();
                    if (tableName.equals(entry.getName())) {
                        LOG.info("Adding {} table for {}{}", entry.getName(), fs.name, version.version);

                        InputStream inputStream = zipFile.getInputStream(entry);

                        BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
                        // Read in first line (row headers)
                        String line = in.readLine();
                        List<String> fieldsFoundInZip = Arrays.asList(line.split(","));

                        // Iterate over rows in table, writing them to the out file.
                        while ((line = in.readLine()) != null) {
                            String[] newValues = new String[fieldsNode.size()];
                            String[] values = line.split(Consts.COLUMN_SPLIT, -1);
                            if (values.length == 1) {
                                LOG.warn("Found blank line. Skipping...");
                                continue;
                            }
                            // Piece together the row to write, which should look practically identical to the original
                            // row except for the identifiers receiving a prefix to avoid ID conflicts.
                            for(int v = 0; v < fieldsNode.size(); v++) {
                                JsonNode fieldNode = fieldsNode.get(v);
                                String fieldName = fieldNode.get("name").asText();

                                // Get index of field from GTFS spec as it appears in feed
                                int index = fieldsFoundInZip.indexOf(fieldName);
                                String val = "";
                                try {
                                    index = fieldsFoundInZip.indexOf(fieldName);
                                    if(index != -1) {
                                        val = values[index];
                                    }
                                } catch (ArrayIndexOutOfBoundsException e) {
                                    LOG.warn("Index {} out of bounds for file {} and feed {}", index, entry.getName(), fs.name);
                                    continue;
                                }
                                // Determine if field is GTFS identifier.
                                String fieldType = fieldNode.get("inputType").asText();
                                // If field is a GTFS identifier (e.g., route_id, stop_id, etc.), add scoped prefix.
                                newValues[v] = fieldType.contains("GTFS") && !val.isEmpty()
                                    ? String.join(":", idScope, val)
                                    : val;
                            }
                            // Write line to table (plus new line char).
                            String newLine = String.join(",", newValues);
                            tableOut.write(newLine.getBytes());
                            tableOut.write("\n".getBytes());
                        }
                    }
                }
            }
        } catch (IOException e) {
            LOG.error(
                "Error merging feed sources: {}",
                zipFilesForVersions.keySet().stream().map(fs -> fs.name).collect(Collectors.toList()).toString()
            );
            throw e;
        }
        return tableOut.toByteArray();
    }
}
