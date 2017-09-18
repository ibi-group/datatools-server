package com.conveyal.datatools.manager.models;


import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.HashUtils;
import com.conveyal.gtfs.BaseGTFSCache;
import com.conveyal.gtfs.GTFS;
import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.loader.TableReader;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.r5.common.R5Version;
import com.conveyal.r5.point_to_point.builder.TNBuilderConfig;
import com.conveyal.r5.transit.TransportNetwork;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.eventbus.EventBus;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.geotools.geojson.geom.GeometryJSON;
import org.mapdb.Fun.Tuple2;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.conveyal.datatools.manager.models.Deployment.downloadOsmExtract;
import static com.conveyal.datatools.manager.utils.StringUtils.getCleanName;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.pull;
import static com.mongodb.client.model.Updates.set;
import static spark.Spark.halt;

/**
 * Represents a version of a feed.
 * @author mattwigway
 *
 */
@JsonInclude(Include.ALWAYS)
@JsonIgnoreProperties(ignoreUnknown = true)
public class FeedVersion extends Model implements Serializable {
    private static final long serialVersionUID = 1L;
    private static ObjectMapper mapper = new ObjectMapper();
    public static final Logger LOG = LoggerFactory.getLogger(FeedVersion.class);
    private static final String validationSubdir = "validation/";
    // FIXME: make this private?
    public static FeedStore feedStore = new FeedStore();

    /**
     * We generate IDs manually, but we need a bit of information to do so
     */
    public FeedVersion(FeedSource source) {
        this.updated = new Date();
        this.feedSourceId = source.id;
        this.name = formattedTimestamp() + " Version";

       this.id = generateFeedVersionId(source);

        // infer the version
//        FeedVersion prev = source.retrieveLatest();
//        if (prev != null) {
//            this.version = prev.version + 1;
//        }
//        else {
//            this.version = 1;
//        }
        int count = source.feedVersionCount();
        this.version = count + 1;
    }

    private String generateFeedVersionId(FeedSource source) {
        // ISO time
        DateFormat df = new SimpleDateFormat("yyyyMMdd'T'HHmmssX");

        // since we store directly on the file system, this lets users look at the DB directly
        // TODO: no need to BaseGTFSCache.cleanId once we rely on GTFSCache to store the feed.
        return BaseGTFSCache.cleanId(getCleanName(source.name) + "-" + df.format(this.updated) + "-" + source.id) + ".zip";
    }

    /**
     * Create an uninitialized feed version. This should only be used for dump/restore.
     */
    public FeedVersion() {
        // do nothing
    }

    /**
     * The feed source this is associated with
     */
    @JsonView(JsonViews.DataDump.class)
    public String feedSourceId;

    @JsonIgnore
    public transient TransportNetwork transportNetwork;

    @JsonView(JsonViews.UserInterface.class)
    @JsonProperty("feedSource")
    public FeedSource parentFeedSource() {
        return Persistence.feedSources.getById(feedSourceId);
    }

    @JsonIgnore
    public FeedVersion previousVersion() {
        return Persistence.feedVersions.getOneFiltered(and(
                eq("version", this.version - 1), eq("feedSourceId", this.id)), null);
    }

    @JsonView(JsonViews.UserInterface.class)
    @JsonProperty("previousVersionId")
    public String previousVersionId() {
        FeedVersion p = previousVersion();
        return p != null ? p.id : null;
    }

    // TODO check that this filter is functional
    @JsonIgnore
    public FeedVersion nextVersion() {
        return Persistence.feedVersions.getOneFiltered(and(
                eq("version", this.version + 1), eq("feedSourceId", this.id)), null);
    }

    @JsonView(JsonViews.UserInterface.class)
    @JsonProperty("nextVersionId")
    public String nextVersionId() {
        FeedVersion p = nextVersion();
        return p != null ? p.id : null;
    }

    /**
     * The hash of the feed file, for quick checking if the file has been updated
     */
    @JsonView(JsonViews.DataDump.class)
    public String hash;

    @JsonIgnore
    public File retrieveGtfsFile() {
        return feedStore.getFeed(id);
    }

    public File newGtfsFile(InputStream inputStream) {
        File file = feedStore.newFeed(id, inputStream, parentFeedSource());
        // FIXME: Should we be doing updates like this?
        Persistence.feedVersions.update(id, String.format("{fileSize: %d}", file.length()));
        LOG.info("New GTFS file saved: {}", id);
        return file;
    }
    public File newGtfsFile(InputStream inputStream, Long lastModified) {
        File file = newGtfsFile(inputStream);
        if (lastModified != null) {
            this.fileTimestamp = lastModified;
            file.setLastModified(lastModified);
        }
        else {
            this.fileTimestamp = file.lastModified();
        }
        // FIXME
        Persistence.feedVersions.update(id, String.format("{fileTimestamp: %d}", this.fileTimestamp));
        return file;
    }
    // FIXME return sql-loader Feed object.
    @JsonIgnore
    public Feed retrieveFeed() {
        return new Feed(DataManager.GTFS_DATA_SOURCE, namespace);
    }

    /** The results of validating this feed */
    @JsonView(JsonViews.DataDump.class)
    public FeedValidationResult validationResult;

    @JsonView(JsonViews.UserInterface.class)
    @JsonProperty("validationSummary")
    public FeedValidationResultSummary validationSummary() {
        return new FeedValidationResultSummary(validationResult);
    }


    /** When this version was uploaded/fetched */
    public Date updated;

    /** The version of the feed, starting with 0 for the first and so on */
    public int version;

    /** A name for this version. Defaults to creation date if not specified by user */
    public String name;

    public Long fileSize;

    public Long fileTimestamp;

    /** SQL namespace for GTFS data */
    public String namespace;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @JsonIgnore
    public String formattedTimestamp() {
        SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy H:mm");
        return format.format(this.updated);
    }

    /**
     * Validate a version of GTFS. This method actually does a little more processing than just validation.
     * Because validate() is run on all GTFS feeds whether they're fetched, created from an editor snapshot,
     * uploaded manually, or god knows however else, we need a single function to handle loading a feed into
     * the relational database, validating the data, and storing that validation result. Since those
     * processes more or less happen in a tight sequence, we handle all of that here.
     */
    public void validate(EventBus eventBus) {
        if (eventBus == null) {
            eventBus = new EventBus();
        }
        Map<String, Object> statusMap = new HashMap<>();

        // STEP 1. LOAD GTFS feed into relational database
        try {
            statusMap.put("message", "Unpacking feed...");
            statusMap.put("percentComplete", 15.0);
            statusMap.put("error", false);
            eventBus.post(statusMap);

            // Get SQL schema namespace for the feed version. This is needed for reconnecting with feeds
            // in the database.
            String gtfsFilePath = retrieveGtfsFile().getPath();
            namespace = GTFS.load(gtfsFilePath, DataManager.GTFS_DATA_SOURCE);
            LOG.info("Loaded GTFS into SQL {}", namespace);
        } catch (Exception e) {
            String errorString = String.format("Error loading GTFS feed for version: %s", this.id);
            LOG.warn(errorString, e);
            statusMap.put("message", errorString);
            statusMap.put("percentComplete", 0.0);
            statusMap.put("error", true);
            eventBus.post(statusMap);
            return;
        }

        // if load was unsuccessful, commitAndClose with error status
        if(namespace == null) {
            String errorString = String.format("Could not load GTFS for FeedVersion %s", id);
            LOG.warn(errorString);
            statusMap.put("message", errorString);
            statusMap.put("percentComplete", 0.0);
            statusMap.put("error", true);
            eventBus.post(statusMap);
            return;
        }

        // STEP 2. Upload GTFS to S3 (storage on local machine is done when feed is fetched/uploaded)
        if (DataManager.useS3) {
            try {
                FileInputStream fileStream = new FileInputStream(retrieveGtfsFile());
                FeedVersion.feedStore.uploadToS3(fileStream, this.id, this.parentFeedSource());

                // TODO: delete local copy of feed version after successful upload?
            } catch (FileNotFoundException e) {
                LOG.error("Could not upload version {} to s3 bucket", this.id);
                e.printStackTrace();
            }
        }

        // STEP 3. VALIDATE GTFS feed
        try {
            // make feed public... this shouldn't take very long
            FeedSource fs = parentFeedSource();
            if (fs.isPublic) {
                fs.makePublic();
            }
            statusMap.put("message", "Validating feed...");
            statusMap.put("percentComplete", 30.0);
            statusMap.put("error", false);
            eventBus.post(statusMap);
            LOG.info("Beginning validation...");

            // run validation on feed version
            GTFS.validate(namespace, DataManager.GTFS_DATA_SOURCE);

            LOG.info("Calculating stats...");
            try {
                // getAverageRevenueTime occurs in FeedValidationResult, so we surround it with a try/catch just in case it fails
                // FIXME: create feedValidationResult with Feed object that has # of routes/stops/etc.
                Feed feed = retrieveFeed();
                validationResult = new FeedValidationResult(feed);
                feed.close();
                Persistence.feedVersions.updateField(this.id, "validationResult", validationResult);
                // This may take a while for very large feeds.
                LOG.info("Calculating # of trips per date of service");
//                tripsPerDate = stats.getTripCountPerDateOfService();
            }catch (Exception e) {
                String message = String.format("Unable to validate feed %s", this.id);
                LOG.error(message, e);
                statusMap.put("message", message);
                statusMap.put("percentComplete", 0.0);
                statusMap.put("error", true);
                eventBus.post(statusMap);
                validationResult = new FeedValidationResult(LoadStatus.OTHER_FAILURE, "Could not calculate validation properties.");
                return;
            }
        } catch (Exception e) {
            String message = String.format("Unable to validate feed %s", this.id);
            LOG.error(message, e);
            statusMap.put("message", message);
            statusMap.put("percentComplete", 0.0);
            statusMap.put("error", true);
            eventBus.post(statusMap);
            validationResult = new FeedValidationResult(LoadStatus.OTHER_FAILURE, "Could not calculate validation properties.");
            return;
        }

        // STEP 4. STORE validation result as json file on s3
        // FIXME: this will likely change with sql-loading?
        File tempFile = null;
        try {
            statusMap.put("message", "Saving validation results...");
            statusMap.put("percentComplete", 80.0);
            statusMap.put("error", false);
            eventBus.post(statusMap);

            // Use temp file
            tempFile = File.createTempFile(this.id, ".json");
            tempFile.deleteOnExit();
            Map<String, Object> validation = new HashMap<>();
            // FIXME: ensure errors loaded from sql the correct way
            validation.put("errors", 0);
            // FIXME: retrieveById tripsPerDate from sql
//            validation.put("tripsPerDate", tripsPerDate);
            GeometryJSON g = new GeometryJSON();
            // FIXME: retrieveById merged buffers from sql
            Geometry buffers = null; // gtfsFeed.getMergedBuffers();
            validation.put("mergedBuffers", buffers != null ? g.toString(buffers) : null);
            mapper.writeValue(tempFile, validation);
        } catch (IOException e) {
            e.printStackTrace();
        }
        LOG.info("Total errors after validation: {}", validationResult.errorCount);
        saveValidationResult(tempFile);
    }

    public JsonNode retrieveValidationResult(boolean revalidate) {
        if (revalidate) {
            LOG.warn("Revalidation requested.  Validating feed.");
            this.validate();
            // TODO: change to 202 status code
            halt(503, SparkUtils.formatJSON("Try again later. Validating feed", 503));
        }
        String keyName = validationSubdir + this.id + ".json";
        InputStream objectData = null;
        if (DataManager.feedBucket != null && DataManager.useS3) {
            try {
                LOG.info("Getting validation results from s3");
                S3Object object = FeedStore.s3Client.getObject(new GetObjectRequest(DataManager.feedBucket, keyName));
                objectData = object.getObjectContent();
            } catch (AmazonS3Exception e) {
                // if json file does not exist, validate feed.
                this.validate();
                halt(503, "Try again later. Validating feed");
            } catch (AmazonServiceException ase) {
                LOG.error("Error downloading from s3");
                ase.printStackTrace();
            }

        }
        // if s3 upload set to false
        else {
            File file = new File(FeedStore.basePath + "/" + keyName);
            try {
                objectData = new FileInputStream(file);
            } catch (Exception e) {
                LOG.warn("Validation does not exist.  Validating feed.");
                this.validate();
                halt(503, "Try again later. Validating feed");
            }
        }
        return ensureValidationIsCurrent(objectData);
    }

    private JsonNode ensureValidationIsCurrent(InputStream objectData) {
        JsonNode n;
        // Process the objectData stream.
        try {
            n = mapper.readTree(objectData);
            if (!n.has("errors") || !n.has("tripsPerDate")) {
                throw new Exception("Validation for feed version not up to date");
            }
            return n;
        } catch (IOException e) {
            // if json file does not exist, validate feed.
            this.validate();
            halt(503, "Try again later. Validating feed");
        } catch (Exception e) {
            e.printStackTrace();
            this.validate();
            halt(503, "Try again later. Validating feed");
        }
        return null;
    }

    private void saveValidationResult(File file) {
        String keyName = validationSubdir + this.id + ".json";

        // upload to S3, if we have bucket name and use s3 storage
        if(DataManager.feedBucket != null && DataManager.useS3) {
            try {
                LOG.info("Uploading validation json to S3");
                FeedStore.s3Client.putObject(new PutObjectRequest(
                        DataManager.feedBucket, keyName, file));
            } catch (AmazonServiceException ase) {
                LOG.error("Error uploading validation json to S3", ase);
            }
        }
        // save to validation directory in gtfs folder
        else {
            File validationDir = new File(FeedStore.basePath + "/" + validationSubdir);
            // ensure directory exists
            validationDir.mkdir();
            try {
                FileUtils.copyFile(file, new File(FeedStore.basePath + "/" + keyName));
            } catch (IOException e) {
                LOG.error("Error saving validation json to local disk", e);
            }
        }
    }

    public void validate() {
        validate(null);
    }

    public void hash () {
        this.hash = HashUtils.hashFile(retrieveGtfsFile());
    }

    public TransportNetwork buildTransportNetwork(EventBus eventBus) {
        // return null if validation result is null (probably means something went wrong with validation, plus we won't have feed bounds).
        if (this.validationResult == null || validationResult.loadStatus == LoadStatus.OTHER_FAILURE) {
            return null;
        }

        if (eventBus == null) {
            eventBus = new EventBus();
        }

        // Fetch OSM extract
        Map<String, Object> statusMap = new HashMap<>();
        statusMap.put("message", "Fetching OSM extract...");
        statusMap.put("percentComplete", 10.0);
        statusMap.put("error", false);
        eventBus.post(statusMap);

        Rectangle2D bounds = this.validationResult.bounds;

        if (bounds == null) {
            String message = String.format("Could not build network for %s because feed bounds are unknown.", this.id);
            LOG.warn(message);
            statusMap.put("message", message);
            statusMap.put("percentComplete", 10.0);
            statusMap.put("error", true);
            eventBus.post(statusMap);
            return null;
        }

        File osmExtract = downloadOSMFile(bounds);
        if (!osmExtract.exists()) {
            InputStream is = downloadOsmExtract(this.validationResult.bounds);
            OutputStream out;
            try {
                out = new FileOutputStream(osmExtract);
                IOUtils.copy(is, out);
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // Create/save r5 network
        statusMap.put("message", "Creating transport network...");
        statusMap.put("percentComplete", 50.0);
        statusMap.put("error", false);
        eventBus.post(statusMap);

        List<GTFSFeed> feedList = new ArrayList<>();
        // FIXME: fix sql-loader integration to work with r5 TransportNetwork
//        feedList.add(retrieveFeed());
        TransportNetwork tn;
        try {
            tn = TransportNetwork.fromFeeds(osmExtract.getAbsolutePath(), feedList, TNBuilderConfig.defaultConfig());
        } catch (Exception e) {
            String message = String.format("Unknown error encountered while building network for %s.", this.id);
            LOG.warn(message);
            statusMap.put("message", message);
            statusMap.put("percentComplete", 100.0);
            statusMap.put("error", true);
            eventBus.post(statusMap);
            e.printStackTrace();
            return null;
        }
        tn.transitLayer.buildDistanceTables(null);
        File tnFile = transportNetworkPath();
        try {
            tn.write(tnFile);
            return transportNetwork;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @JsonIgnore
    private static File downloadOSMFile(Rectangle2D bounds) {
        if (bounds != null) {
            String baseDir = FeedStore.basePath.getAbsolutePath() + File.separator + "osm";
            File osmPath = new File(String.format("%s/%.6f_%.6f_%.6f_%.6f", baseDir, bounds.getMaxX(), bounds.getMaxY(), bounds.getMinX(), bounds.getMinY()));
            if (!osmPath.exists()) {
                osmPath.mkdirs();
            }
            return new File(osmPath.getAbsolutePath() + "/data.osm.pbf");
        }
        else {
            return null;
        }
    }

    public TransportNetwork buildTransportNetwork() {
        return buildTransportNetwork(null);
    }


    /**
     * Does this feed version have any critical errors that would prevent it being loaded to OTP?
     * @return whether feed version has critical errors
     */
    public boolean hasCriticalErrors() {
        return hasCriticalErrorsExceptingDate() || (LocalDate.now()).isAfter(validationResult.endDate);
    }

    /**
     * Does this feed have any critical errors other than possibly being expired?
     * @return whether feed version has critical errors (outside of expiration)
     */
    private boolean hasCriticalErrorsExceptingDate() {
        if (validationResult == null)
            return true;

        return validationResult.loadStatus != LoadStatus.SUCCESS ||
            validationResult.stopTimesCount == 0 ||
            validationResult.tripCount == 0 ||
            validationResult.agencyCount == 0;

    }

    @JsonView(JsonViews.UserInterface.class)
    @JsonProperty("noteCount")
    public int noteCount() {
        return this.noteIds != null ? this.noteIds.size() : 0;
    }

    // FIXME remove this? or make into a proper getter?
    @JsonInclude(Include.NON_NULL)
    @JsonView(JsonViews.UserInterface.class)
    @JsonProperty("fileTimestamp")
    public Long fileTimestamp() {
        if (fileTimestamp != null) {
            return fileTimestamp;
        }

        this.fileTimestamp = feedStore.getFeedLastModified(id);

        return this.fileTimestamp;
    }

    // FIXME remove this? or make into a proper getter?
    @JsonInclude(Include.NON_NULL)
    @JsonView(JsonViews.UserInterface.class)
    @JsonProperty("fileSize")
    public Long fileSize() {
        if (fileSize != null) {
            return fileSize;
        }

        this.fileSize = feedStore.getFeedSize(id);

        return fileSize;
    }

    /**
     * Delete this feed version and clean up, removing references to it and derived objects and state.
     * Steps:
     * If we are deleting the latest version, change the memoized "last fetched" value in the FeedSource.
     * Delete the GTFS Zip file locally or on S3
     * Remove this feed version from all Deployments [shouldn't we be updating the version rather than deleting it?]
     * Remove the transport network file from the local disk
     * Finally delete the version object from the database.
     */
    public void delete() {
        try {
            // reset lastModified if feed is latest version
            LOG.info("Deleting feed version {}", this.id);
            String id = this.id;
            FeedSource fs = parentFeedSource();
            FeedVersion latest = fs.retrieveLatest();
            if (latest != null && latest.id.equals(this.id)) {
                // Even if there are previous feed versions, we set to null to allow re-fetching the version that was just deleted
                // TODO instead, set it to the fetch time of the previous feed version
                Persistence.feedSources.update(fs.id, "{lastFetched:null}");
            }
            feedStore.deleteFeed(id);
            // Remove this FeedVersion from all Deployments associated with this FeedVersion's FeedSource's Project
            // TODO TEST THOROUGHLY THAT THIS UPDATE EXPRESSION IS CORRECT
            // Although outright deleting the feedVersion from deployments could be surprising and shouldn't be done anyway.
            Persistence.deployments.getMongoCollection().updateMany(eq("projectId", this.parentFeedSource().projectId),
                    pull("feedVersionIds", this.id));
            transportNetworkPath().delete();
            Persistence.feedVersions.removeById(this.id);
            this.parentFeedSource().renumberFeedVersions();
            LOG.info("Version {} deleted", id);
        } catch (Exception e) {
            LOG.warn("Error deleting version", e);
        }
    }

    @JsonIgnore
    private String r5Path() {
        // r5 networks MUST be stored in separate directories (in this case under feed source ID
        // because of shared osm.mapdb used by r5 networks placed in same dir
        File r5 = new File(String.join(File.separator, FeedStore.basePath.getAbsolutePath(), this.feedSourceId));
        if (!r5.exists()) {
            r5.mkdirs();
        }
        return r5.getAbsolutePath();
    }

    @JsonIgnore
    public File transportNetworkPath() {
        return new File(String.join(File.separator, r5Path(), id + "_" + R5Version.describe + "_network.dat"));
    }
}
