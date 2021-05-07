package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.gtfsplus.GtfsPlusValidation;
import com.conveyal.datatools.manager.jobs.ProcessSingleFeedJob;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.JobUtils;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import static com.conveyal.datatools.common.utils.SparkUtils.formatJobMessage;
import static com.conveyal.datatools.common.utils.SparkUtils.copyRequestStreamIntoFile;
import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;
import static com.conveyal.datatools.manager.models.FeedRetrievalMethod.PRODUCED_IN_HOUSE_GTFS_PLUS;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;

/**
 * This handles the GTFS+ specific HTTP endpoints, which allow for validating GTFS+ tables,
 * downloading GTFS+ files to a client for editing (for example), and uploading/publishing a GTFS+ zip as
 * (for example, one that has been edited) as a new feed version. Here is the workflow in sequence:
 * 
 * 1. User uploads feed version (with or without GTFS+ tables).
 * 2. User views validation to determine if errors need amending.
 * 3. User makes edits (in client) and uploads modified GTFS+.
 * 4. Once user is satisfied with edits. User publishes as new feed version.
 * 
 * Created by demory on 4/13/16.
 */
public class GtfsPlusController {

    public static final Logger LOG = LoggerFactory.getLogger(GtfsPlusController.class);

    private static final FeedStore gtfsPlusStore = new FeedStore(DataManager.GTFS_PLUS_SUBDIR);

    /**
     * Upload a GTFS+ file based on a specific feed version and replace (or create)
     * the file in the GTFS+ specific feed store.
     */
    private static Boolean uploadGtfsPlusFile (Request req, Response res) {
        String feedVersionId = req.params("versionid");
        File newGtfsFile = gtfsPlusStore.getFeedFile(feedVersionId);
        copyRequestStreamIntoFile(req, newGtfsFile);
        return true;
    }

    /**
     * Download a GTFS+ file for a specific feed version. If no edited GTFS+ file
     * has been uploaded for the feed version, the original feed version will be returned.
     */
    private static HttpServletResponse getGtfsPlusFile(Request req, Response res) {
        String feedVersionId = req.params("versionid");
        LOG.info("Downloading GTFS+ file for FeedVersion " + feedVersionId);

        // check for saved
        File file = gtfsPlusStore.getFeed(feedVersionId);
        if (file == null) {
            return getGtfsPlusFromGtfs(feedVersionId, req, res);
        }
        LOG.info("Returning updated GTFS+ data");
        return SparkUtils.downloadFile(file, file.getName() + ".zip", req, res);
    }

    /**
     * Download only the GTFS+ tables in a zip for a specific feed version.
     */
    private static HttpServletResponse getGtfsPlusFromGtfs(String feedVersionId, Request req, Response res) {
        LOG.info("Extracting GTFS+ data from main GTFS feed");
        FeedVersion version = Persistence.feedVersions.getById(feedVersionId);

        File gtfsPlusFile = null;

        // create a set of valid GTFS+ table names
        Set<String> gtfsPlusTables = new HashSet<>();
        for (int i = 0; i < DataManager.gtfsPlusConfig.size(); i++) {
            JsonNode tableNode = DataManager.gtfsPlusConfig.get(i);
            gtfsPlusTables.add(tableNode.get("name").asText());
        }

        try {
            // create a new zip file to only contain the GTFS+ tables
            gtfsPlusFile = File.createTempFile(version.id + "_gtfsplus", ".zip");
            ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(gtfsPlusFile));

            // iterate through the existing GTFS file, copying any GTFS+ tables
            ZipFile gtfsFile = new ZipFile(version.retrieveGtfsFile());
            final Enumeration<? extends ZipEntry> entries = gtfsFile.entries();
            byte[] buffer = new byte[512];
            while (entries.hasMoreElements()) {
                final ZipEntry entry = entries.nextElement();
                if (!gtfsPlusTables.contains(entry.getName())) continue;

                // create a new empty ZipEntry and copy the contents
                ZipEntry newEntry = new ZipEntry(entry.getName());
                zos.putNextEntry(newEntry);
                InputStream in = gtfsFile.getInputStream(entry);
                while (0 < in.available()){
                    int read = in.read(buffer);
                    zos.write(buffer,0,read);
                }
                in.close();
                zos.closeEntry();
            }
            zos.close();
        } catch (IOException e) {
            logMessageAndHalt(req, 500, "An error occurred while trying to create a gtfs file", e);
        }

        return SparkUtils.downloadFile(gtfsPlusFile, gtfsPlusFile.getName() + ".zip", req, res);
    }

    /** HTTP endpoint used to return the last modified timestamp for a GTFS+ feed. Essentially this is used as a way to
     * determine whether any GTFS+ edits have been made to
     */
    private static Long getGtfsPlusFileTimestamp(Request req, Response res) {
        String feedVersionId = req.params("versionid");

        // check for saved GTFS+ data
        File file = gtfsPlusStore.getFeed(feedVersionId);
        if (file == null) {
            FeedVersion feedVersion = Persistence.feedVersions.getById(feedVersionId);
            if (feedVersion == null) {
                logMessageAndHalt(req, 400, "Feed version ID is not valid");
                return null;
            }
            return feedVersion.fileTimestamp;
        } else {
            return file.lastModified();
        }
    }

    /**
     * Publishes the edited/saved GTFS+ file as a new feed version for the feed source.
     * This is the final stage in the GTFS+ validation/editing workflow described in the
     * class's javadoc.
     */
    private static String publishGtfsPlusFile(Request req, Response res) {
        Auth0UserProfile profile = req.attribute("user");
        String feedVersionId = req.params("versionid");
        LOG.info("Publishing GTFS+ for " + feedVersionId);
        File plusFile = gtfsPlusStore.getFeed(feedVersionId);
        if (plusFile == null || !plusFile.exists()) {
            logMessageAndHalt(req, 400, "No saved GTFS+ data for version");
        }

        FeedVersion feedVersion = Persistence.feedVersions.getById(feedVersionId);

        // create a set of valid GTFS+ table names
        Set<String> gtfsPlusTables = new HashSet<>();
        for (int i = 0; i < DataManager.gtfsPlusConfig.size(); i++) {
            JsonNode tableNode = DataManager.gtfsPlusConfig.get(i);
            gtfsPlusTables.add(tableNode.get("name").asText());
        }

        File newFeed = null;

        try {
            // First, create a new zip file to only contain the GTFS+ tables
            newFeed = File.createTempFile(feedVersionId + "_new", ".zip");
            ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(newFeed));

            // Next, iterate through the existing GTFS file, copying all non-GTFS+ tables.
            ZipFile gtfsFile = new ZipFile(feedVersion.retrieveGtfsFile());
            final Enumeration<? extends ZipEntry> entries = gtfsFile.entries();
            byte[] buffer = new byte[512];
            while (entries.hasMoreElements()) {
                final ZipEntry entry = entries.nextElement();
                // skip GTFS+ and non-standard tables
                if (gtfsPlusTables.contains(entry.getName()) || entry.getName().startsWith("_")) continue;

                // create a new empty ZipEntry and copy the contents
                ZipEntry newEntry = new ZipEntry(entry.getName());
                zos.putNextEntry(newEntry);
                InputStream in = gtfsFile.getInputStream(entry);
                while (0 < in.available()){
                    int read = in.read(buffer);
                    zos.write(buffer,0,read);
                }
                in.close();
                zos.closeEntry();
            }

            // iterate through the GTFS+ file, copying all entries
            ZipFile plusZipFile = new ZipFile(plusFile);
            final Enumeration<? extends ZipEntry> plusEntries = plusZipFile.entries();
            while (plusEntries.hasMoreElements()) {
                final ZipEntry entry = plusEntries.nextElement();

                ZipEntry newEntry = new ZipEntry(entry.getName());
                zos.putNextEntry(newEntry);
                InputStream in = plusZipFile.getInputStream(entry);
                while (0 < in.available()){
                    int read = in.read(buffer);
                    zos.write(buffer,0,read);
                }
                in.close();
                zos.closeEntry();
            }
            zos.close();
        } catch (IOException e) {
            logMessageAndHalt(req, 500, "Error creating combined GTFS/GTFS+ file", e);
        }
        // Create a new feed version to represent the published GTFS+.
        FeedVersion newFeedVersion = new FeedVersion(feedVersion.parentFeedSource(), PRODUCED_IN_HOUSE_GTFS_PLUS);
        File newGtfsFile = null;
        try {
            newGtfsFile = newFeedVersion.newGtfsFile(new FileInputStream(newFeed));
        } catch (IOException e) {
            e.printStackTrace();
            logMessageAndHalt(req, 500, "Error reading GTFS file input stream", e);
        }
        if (newGtfsFile == null) {
            logMessageAndHalt(req, 500, "GTFS input file must not be null");
            return null;
        }
        newFeedVersion.originNamespace = feedVersion.namespace;

        // Must be handled by executor because it takes a long time.
        ProcessSingleFeedJob processSingleFeedJob = new ProcessSingleFeedJob(newFeedVersion, profile, true);
        JobUtils.heavyExecutor.execute(processSingleFeedJob);

        return formatJobMessage(processSingleFeedJob.jobId, "Feed version is processing.");
    }

    /**
     * HTTP endpoint that validates GTFS+ tables for a specific feed version (or its saved/edited GTFS+).
     */
    private static GtfsPlusValidation getGtfsPlusValidation(Request req, Response res) {
        String feedVersionId = req.params("versionid");
        GtfsPlusValidation gtfsPlusValidation = null;
        try {
            gtfsPlusValidation = GtfsPlusValidation.validate(feedVersionId);
        } catch(Exception e) {
            logMessageAndHalt(req, 500, "Could not read GTFS+ zip file", e);
        }
        return gtfsPlusValidation;
    }

    /**
     * HTTP endpoint to delete the GTFS+ specific edits made for a feed version. In other words, this will revert to
     * referencing the original GTFS+ files for a feed version. Note: this will not delete the feed version itself.
     */
    private static String deleteGtfsPlusFile(Request req, Response res) {
        String feedVersionId = req.params("versionid");
        File file = gtfsPlusStore.getFeed(feedVersionId);
        if (file == null) {
            logMessageAndHalt(req, HttpStatus.NOT_FOUND_404, "No GTFS+ file found for feed version");
            return null;
        }
        file.delete();
        return SparkUtils.formatJSON("message", "GTFS+ edits deleted successfully.");
    }

    public static void register(String apiPrefix) {
        post(apiPrefix + "secure/gtfsplus/:versionid", GtfsPlusController::uploadGtfsPlusFile, JsonUtil.objectMapper::writeValueAsString);
        get(apiPrefix + "secure/gtfsplus/:versionid", GtfsPlusController::getGtfsPlusFile);
        delete(apiPrefix + "secure/gtfsplus/:versionid", GtfsPlusController::deleteGtfsPlusFile);
        get(apiPrefix + "secure/gtfsplus/:versionid/timestamp", GtfsPlusController::getGtfsPlusFileTimestamp, JsonUtil.objectMapper::writeValueAsString);
        get(apiPrefix + "secure/gtfsplus/:versionid/validation", GtfsPlusController::getGtfsPlusValidation, JsonUtil.objectMapper::writeValueAsString);
        post(apiPrefix + "secure/gtfsplus/:versionid/publish", GtfsPlusController::publishGtfsPlusFile, JsonUtil.objectMapper::writeValueAsString);
    }
}
