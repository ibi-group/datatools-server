package com.conveyal.datatools.manager.models;


import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.FileInputStream;
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
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.controllers.api.GtfsApiController;
import com.conveyal.datatools.manager.persistence.DataStore;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.utils.HashUtils;
import com.conveyal.gtfs.BaseGTFSCache;
import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.api.ApiMain;
import com.conveyal.gtfs.validator.json.LoadStatus;
import com.conveyal.gtfs.stats.FeedStats;
import com.conveyal.r5.common.R5Version;
import com.conveyal.r5.point_to_point.builder.TNBuilderConfig;
import com.conveyal.r5.transit.TransportNetwork;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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

import static com.conveyal.datatools.manager.models.Deployment.getOsmExtract;
import static com.conveyal.datatools.manager.utils.StringUtils.getCleanName;
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
    public static final String validationSubdir = "validation/";
    static DataStore<FeedVersion> versionStore = new DataStore<>("feedversions");
    private static FeedStore feedStore = new FeedStore();

    static {
        // set up indexing on feed versions by feed source, indexed by <FeedSource ID, version>
        versionStore.secondaryKey("version", (key, fv) -> new Tuple2(fv.feedSourceId, fv.version));
    }

    /**
     * We generate IDs manually, but we need a bit of information to do so
     */
    public FeedVersion(FeedSource source) {
        this.updated = new Date();
        this.feedSourceId = source.id;

       this.id = generateFeedVersionId(source);

        // infer the version
//        FeedVersion prev = source.getLatest();
//        if (prev != null) {
//            this.version = prev.version + 1;
//        }
//        else {
//            this.version = 1;
//        }
        int count = source.getFeedVersionCount();
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
    public FeedSource getFeedSource() {
        return FeedSource.get(feedSourceId);
    }

    @JsonIgnore
    public FeedVersion getPreviousVersion() {
        return versionStore.find("version", new Tuple2(this.feedSourceId, this.version - 1));
    }

    @JsonView(JsonViews.UserInterface.class)
    public String getPreviousVersionId() {
        FeedVersion p = getPreviousVersion();
        return p != null ? p.id : null;
    }

    @JsonIgnore
    public FeedVersion getNextVersion() {
        return versionStore.find("version", new Tuple2(this.feedSourceId, this.version + 1));
    }

    @JsonView(JsonViews.UserInterface.class)
    public String getNextVersionId() {
        FeedVersion p = getNextVersion();
        return p != null ? p.id : null;
    }

    /**
     * The hash of the feed file, for quick checking if the file has been updated
     */
    @JsonView(JsonViews.DataDump.class)
    public String hash;

    @JsonIgnore
    public File getGtfsFile() {
        return feedStore.getFeed(id);
    }

    public File newGtfsFile(InputStream inputStream) {
        File file = feedStore.newFeed(id, inputStream, getFeedSource());
        this.fileSize = file.length();
        this.save();
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
        this.save();
        return file;
    }
    @JsonIgnore
    public GTFSFeed getGtfsFeed() {
        // clean feed version ID for use with GTFSCache/GTFS API ID system (appends ".zip" to IDs passed in)
        String apiId = id.replace(".zip", "");
        return ApiMain.getFeedSource(apiId).feed;
    }

    /** The results of validating this feed */
    @JsonView(JsonViews.DataDump.class)
    public FeedValidationResult validationResult;

    @JsonView(JsonViews.UserInterface.class)
    public FeedValidationResultSummary getValidationSummary() {
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

    public String getName() {
        return name != null ? name : (getFormattedTimestamp() + " Version");
    }

    @JsonIgnore
    public String getFormattedTimestamp() {
        SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy H:mm");
        return format.format(this.updated);
    }

    public static FeedVersion get(String id) {
        return versionStore.getById(id);
    }

    public static Collection<FeedVersion> getAll() {
        return versionStore.getAll();
    }

    public void validate(EventBus eventBus) {
        if (eventBus == null) {
            eventBus = new EventBus();
        }
        Map<String, Object> statusMap = new HashMap<>();
        GTFSFeed gtfsFeed;
        try {
            statusMap.put("message", "Unpacking feed...");
            statusMap.put("percentComplete", 15.0);
            statusMap.put("error", false);
            eventBus.post(statusMap);

            /* First getGtfsFeed() call triggers feed load from zip file into gtfsCache
            This may take a while for large feeds */
            gtfsFeed = getGtfsFeed();
        } catch (Exception e) {
            String errorString = String.format("Error getting GTFS feed for version: %s", this.id);
            e.printStackTrace();
            LOG.warn(errorString);
            statusMap.put("message", errorString);
            statusMap.put("percentComplete", 0.0);
            statusMap.put("error", true);
            eventBus.post(statusMap);
            return;
        }

        if(gtfsFeed == null) {
            String errorString = String.format("Could not get GTFSFeed object for FeedVersion %s", id);
            LOG.warn(errorString);
//            eventBus.post(new StatusEvent(errorString, 0, true));
            statusMap.put("message", errorString);
            statusMap.put("percentComplete", 0.0);
            statusMap.put("error", true);
            eventBus.post(statusMap);
            return;
        }

        Map<LocalDate, Integer> tripsPerDate;

        try {
            // make feed public... this shouldn't take very long
            FeedSource fs = this.getFeedSource();
            if (fs.isPublic) {
                fs.makePublic();
            }
//            eventBus.post(new StatusEvent("Validating feed...", 30, false));
            statusMap.put("message", "Validating feed...");
            statusMap.put("percentComplete", 30.0);
            statusMap.put("error", false);
            eventBus.post(statusMap);
            LOG.info("Beginning validation...");
            gtfsFeed.validate();
            LOG.info("Calculating stats...");
            FeedStats stats = gtfsFeed.calculateStats();
            try {
                // getAverageRevenueTime occurs in FeedValidationResult, so we surround it with a try/catch just in case it fails
                validationResult = new FeedValidationResult(gtfsFeed, stats);
                // This may take a while for very large feeds.
                LOG.info("Calculating # of trips per date of service");
                tripsPerDate = stats.getTripCountPerDateOfService();
            }catch (Exception e) {
                e.printStackTrace();
                statusMap.put("message", "Unable to validate feed.");
                statusMap.put("percentComplete", 0.0);
                statusMap.put("error", true);
                eventBus.post(statusMap);
                e.printStackTrace();
                this.validationResult = new FeedValidationResult(LoadStatus.OTHER_FAILURE, "Could not calculate validation properties.");
                return;
            }
        } catch (Exception e) {
            LOG.error("Unable to validate feed {}", this.id);
//            eventBus.post(new StatusEvent("Unable to validate feed.", 0, true));
            statusMap.put("message", "Unable to validate feed.");
            statusMap.put("percentComplete", 0.0);
            statusMap.put("error", true);
            eventBus.post(statusMap);
            e.printStackTrace();
//            this.validationResult = null;
            validationResult = new FeedValidationResult(LoadStatus.OTHER_FAILURE, "Could not calculate validation properties.");
//            halt(400, "Error validating feed...");
            return;
        }

        File tempFile = null;
        try {
//            eventBus.post(new StatusEvent("Saving validation results...", 80, false));
            statusMap.put("message", "Saving validation results...");
            statusMap.put("percentComplete", 80.0);
            statusMap.put("error", false);
            eventBus.post(statusMap);
            // Use tempfile
            tempFile = File.createTempFile(this.id, ".json");
            tempFile.deleteOnExit();
            Map<String, Object> validation = new HashMap<>();
            validation.put("errors", gtfsFeed.errors);
            validation.put("tripsPerDate", tripsPerDate);
            GeometryJSON g = new GeometryJSON();
            Geometry buffers = gtfsFeed.getMergedBuffers();
            validation.put("mergedBuffers", buffers != null ? g.toString(buffers) : null);
            mapper.writeValue(tempFile, validation);
        } catch (IOException e) {
            e.printStackTrace();
        }
        LOG.info("Total errors after validation: {}", validationResult.errorCount);
        saveValidationResult(tempFile);
    }

    @JsonIgnore
    public JsonNode getValidationResult(boolean revalidate) {
        if (revalidate) {
            LOG.warn("Revalidation requested.  Validating feed.");
            this.validate();
            this.save();
            halt(503, "Try again later. Validating feed");
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
                this.save();
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
                this.save();
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
            this.save();
            halt(503, "Try again later. Validating feed");
        } catch (Exception e) {
            e.printStackTrace();
            this.validate();
            this.save();
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

    public void save () {
        save(true);
    }

    public void save(boolean commit) {
        if (commit)
            versionStore.save(this.id, this);
        else
            versionStore.saveWithoutCommit(this.id, this);
    }

    public void hash () {
        this.hash = HashUtils.hashFile(getGtfsFile());
    }

    public static void commit() {
        versionStore.commit();
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

        File osmExtract = getOSMFile(bounds);
        if (!osmExtract.exists()) {
            InputStream is = getOsmExtract(this.validationResult.bounds);
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
        feedList.add(getGtfsFeed());
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
        this.transportNetwork = tn;
        this.transportNetwork.transitLayer.buildDistanceTables(null);
        File tnFile = getTransportNetworkPath();
        try {
            tn.write(tnFile);
            return transportNetwork;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @JsonIgnore
    public static File getOSMFile(Rectangle2D bounds) {
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
    public boolean hasCriticalErrorsExceptingDate () {
        if (validationResult == null)
            return true;

        return validationResult.loadStatus != LoadStatus.SUCCESS ||
            validationResult.stopTimesCount == 0 ||
            validationResult.tripCount == 0 ||
            validationResult.agencyCount == 0;

    }

    @JsonView(JsonViews.UserInterface.class)
    public int getNoteCount() {
        return this.noteIds != null ? this.noteIds.size() : 0;
    }

    @JsonInclude(Include.NON_NULL)
    @JsonView(JsonViews.UserInterface.class)
    public Long getFileTimestamp() {
        if (fileTimestamp != null) {
            return fileTimestamp;
        }

        this.fileTimestamp = feedStore.getFeedLastModified(id);
        this.save();

        return this.fileTimestamp;
    }

    @JsonInclude(Include.NON_NULL)
    @JsonView(JsonViews.UserInterface.class)
    public Long getFileSize() {
        if (fileSize != null) {
            return fileSize;
        }

        this.fileSize = feedStore.getFeedSize(id);
        this.save();

        return fileSize;
    }

    /**
     * Delete this feed version.
     */
    public void delete() {
        try {
            // reset lastModified if feed is latest version
            System.out.println("deleting version");
            String id = this.id;
            FeedSource fs = getFeedSource();
            FeedVersion latest = fs.getLatest();
            if (latest != null && latest.id.equals(this.id)) {
                fs.lastFetched = null;
                fs.save();
            }
            feedStore.deleteFeed(id);

            for (Deployment d : Deployment.getAll()) {
                d.feedVersionIds.remove(this.id);
            }

            getTransportNetworkPath().delete();


            versionStore.delete(this.id);
            LOG.info("Version {} deleted", id);
        } catch (Exception e) {
            LOG.warn("Error deleting version", e);
        }
    }
    @JsonIgnore
    public String getR5Path () {
        // r5 networks MUST be stored in separate directories (in this case under feed source ID
        // because of shared osm.mapdb used by r5 networks placed in same dir
        File r5 = new File(String.join(File.separator, FeedStore.basePath.getAbsolutePath(), this.feedSourceId));
        if (!r5.exists()) {
            r5.mkdirs();
        }
        return r5.getAbsolutePath();
    }

    @JsonIgnore
    public File getTransportNetworkPath () {
        return new File(String.join(File.separator, getR5Path(), id + "_" + R5Version.describe + "_network.dat"));
    }
}
