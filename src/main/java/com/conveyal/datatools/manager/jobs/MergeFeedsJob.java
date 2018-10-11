package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.common.utils.Consts;
import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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

import static spark.Spark.halt;

public class MergeFeedsJob extends MonitorableJob {

    private static final Logger LOG = LoggerFactory.getLogger(MergeFeedsJob.class);
    private final Set<FeedVersion> feedVersions;
    private final String filename;

    public MergeFeedsJob(String owner, Set<FeedVersion> feedVersions, String filename) {
        super(owner, "Merging feed versions", JobType.MERGE_FEED_VERSIONS);
        this.feedVersions = feedVersions;
        this.filename = filename;
    }

    @Override
    public void jobLogic() {
        // create temp merged zip file to add feed content to
        File mergedFile = null;
        try {
            mergedFile = File.createTempFile(filename, ".zip");
            mergedFile.deleteOnExit();

        } catch (IOException e) {
            LOG.error("Could not create temp file");
            e.printStackTrace();
            halt(400, SparkUtils.formatJSON("Unknown error while merging feeds.", 400));
        }

        // create the zipfile
        ZipOutputStream out;
        try {
            out = new ZipOutputStream(new FileOutputStream(mergedFile));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        LOG.info("Created project merge file: " + mergedFile.getAbsolutePath());

        // map of feed versions to table entries contained within version's GTFS
        Map<FeedSource, ZipFile> feedSourceMap = new HashMap<>();

        // collect zipFiles for each feedSource before merging tables
        for (FeedVersion version : feedVersions) {
            FeedSource fs = Persistence.feedSources.getById(version.feedSourceId);
            // modify feed version to use prepended feed id
            LOG.info("Adding {} feed to merged zip", fs.name);
            try {
                File file = version.retrieveGtfsFile();
                if (file == null) {
                    LOG.error("No file exists for {}", version.id);
                    continue;
                }
                ZipFile zipFile = new ZipFile(file);
                feedSourceMap.put(fs, zipFile);
            } catch(Exception e) {
                e.printStackTrace();
                LOG.error("Zipfile for version {} not found", version.id);
            }
        }

        // loop through GTFS tables
        // FIXME: add iteration over GTFS+ tables
        int numberOfTables = DataManager.gtfsConfig.size();
        for(int i = 0; i < numberOfTables; i++) {
            JsonNode tableNode = DataManager.gtfsConfig.get(i);
            byte[] tableOut = mergeTables(tableNode, feedSourceMap);

            // if at least one feed has the table, include it
            if (tableOut != null) {

                String tableName = tableNode.get("name").asText();
                synchronized (status) {
                    status.message = "Merging " + tableName;
                    status.percentComplete = Math.round((double) i / numberOfTables * 10000d) / 100d;
                }
                // create entry for zip file
                ZipEntry tableEntry = new ZipEntry(tableName);
                try {
                    out.putNextEntry(tableEntry);
                    LOG.info("Writing {} to merged feed", tableName);
                    out.write(tableOut);
                    out.closeEntry();
                } catch (IOException e) {
                    LOG.error("Error writing to table {}", tableName);
                    e.printStackTrace();
                }
            }
        }
        try {
            out.close();
        } catch (IOException e) {
            LOG.error("Error closing zip file");
            e.printStackTrace();
        }
        synchronized (status) {
            status.message = "Saving merged feed.";
            status.percentComplete = 95.0;
        }
        // Store the project merged zip locally or on s3
//        if (DataManager.useS3) {
//            String s3Key = "project/" + project.id + ".zip";
//            FeedStore.s3Client.putObject(DataManager.feedBucket, s3Key, mergedFile);
//            LOG.info("Storing merged project feed at s3://{}/{}", DataManager.feedBucket, s3Key);
//        } else {
//            try {
//                FeedVersion.feedStore.newFeed(project.id + ".zip", new FileInputStream(mergedFile), null);
//            } catch (FileNotFoundException e) {
//                e.printStackTrace();
//                LOG.error("Could not store feed for project {}", project.id);
//            }
//        }
        // delete temp file
        mergedFile.delete();

        status.update(false, "Merged feed created successfully.", 100, true);
    }

    /**
     * Merge the specified table for multiple GTFS feeds.
     * @param tableNode tableNode to merge
     * @param feedSourceMap map of feedSources to zipFiles from which to extract the .txt tables
     * @return single merged table for feeds
     */
    private static byte[] mergeTables(JsonNode tableNode, Map<FeedSource, ZipFile> feedSourceMap) {

        String tableName = tableNode.get("name").asText();
        ByteArrayOutputStream tableOut = new ByteArrayOutputStream();

        ArrayNode fieldsNode = (ArrayNode) tableNode.get("fields");
        List<String> headers = new ArrayList<>();
        for (int i = 0; i < fieldsNode.size(); i++) {
            JsonNode fieldNode = fieldsNode.get(i);
            String fieldName = fieldNode.get("name").asText();
            // Clean up spec file to exclude datatools/editor specific fields for the merge job.
            boolean notInSpec = fieldNode.has("datatools") && fieldNode.get("datatools").asBoolean();
            if (notInSpec) {
                fieldsNode.remove(i);
            }
            headers.add(fieldName);
        }

        try {
            // write headers to table
            tableOut.write(String.join(",", headers).getBytes());
            tableOut.write("\n".getBytes());

            // iterate over feed source to zipfile map
            for ( Map.Entry<FeedSource, ZipFile> mapEntry : feedSourceMap.entrySet()) {
                FeedSource fs = mapEntry.getKey();
                ZipFile zipFile = mapEntry.getValue();
                final Enumeration<? extends ZipEntry> entries = zipFile.entries();
                while (entries.hasMoreElements()) {
                    final ZipEntry entry = entries.nextElement();
                    if(tableName.equals(entry.getName())) {
                        LOG.info("Adding {} table for {}", entry.getName(), fs.name);

                        InputStream inputStream = zipFile.getInputStream(entry);

                        BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
                        String line = in.readLine();
                        String[] fields = line.split(",");

                        List<String> fieldList = Arrays.asList(fields);


                        // iterate over rows in table
                        while((line = in.readLine()) != null) {
                            String[] newValues = new String[fieldsNode.size()];
                            String[] values = line.split(Consts.COLUMN_SPLIT, -1);
                            if (values.length == 1) {
                                LOG.warn("Found blank line. Skipping...");
                                continue;
                            }
                            for(int v = 0; v < fieldsNode.size(); v++) {
                                JsonNode fieldNode = fieldsNode.get(v);
                                String fieldName = fieldNode.get("name").asText();

                                // get index of field from GTFS spec as it appears in feed
                                int index = fieldList.indexOf(fieldName);
                                String val = "";
                                try {
                                    index = fieldList.indexOf(fieldName);
                                    if(index != -1) {
                                        val = values[index];
                                    }
                                } catch (ArrayIndexOutOfBoundsException e) {
                                    LOG.warn("Index {} out of bounds for file {} and feed {}", index, entry.getName(), fs.name);
                                    continue;
                                }

                                String fieldType = fieldNode.get("inputType").asText();

                                // if field is a gtfs identifier, prepend with feed id/name
                                if (fieldType.contains("GTFS") && !val.isEmpty()) {
                                    newValues[v] = fs.name + ":" + val;
                                }
                                else {
                                    newValues[v] = val;
                                }
                            }
                            String newLine = String.join(",", newValues);

                            // write line to table (plus new line char)
                            tableOut.write(newLine.getBytes());
                            tableOut.write("\n".getBytes());
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("Error merging feed sources: {}", feedSourceMap.keySet().stream().map(fs -> fs.name).collect(Collectors.toList()).toString());
            halt(400, SparkUtils.formatJSON("Error merging feed sources", 400, e));
        }
        return tableOut.toByteArray();
    }
}
