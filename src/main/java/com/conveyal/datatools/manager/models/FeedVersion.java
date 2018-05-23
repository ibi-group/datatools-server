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
import java.util.Date;
import java.util.List;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.HashUtils;
import com.conveyal.gtfs.BaseGTFSCache;
import com.conveyal.gtfs.GTFS;
import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.loader.FeedLoadResult;
import com.conveyal.gtfs.validator.ValidationResult;
import com.conveyal.r5.common.R5Version;
import com.conveyal.r5.point_to_point.builder.TNBuilderConfig;
import com.conveyal.r5.transit.TransportNetwork;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.bson.codecs.pojo.annotations.BsonProperty;

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

/**
 * Represents a version of a feed.
 * @author mattwigway
 *
 */
@JsonInclude(Include.ALWAYS)
@JsonIgnoreProperties(ignoreUnknown = true)
public class FeedVersion extends Model implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final String VERSION_ID_DATE_FORMAT = "yyyyMMdd'T'HHmmssX";
    private static final String HUMAN_READABLE_TIMESTAMP_FORMAT = "MM/dd/yyyy H:mm";
    private static final Logger LOG = LoggerFactory.getLogger(FeedVersion.class);
    // FIXME: move this out of FeedVersion (also, it should probably not be public)?
    public static FeedStore feedStore = new FeedStore();

    /**
     * We generate IDs manually, but we need a bit of information to do so
     */
    public FeedVersion(FeedSource source) {
        this.updated = new Date();
        this.feedSourceId = source.id;
        this.name = formattedTimestamp() + " Version";
        this.id = generateFeedVersionId(source);
        int count = source.feedVersionCount();
        this.version = count + 1;
    }

    private String generateFeedVersionId(FeedSource source) {
        // ISO time
        DateFormat df = new SimpleDateFormat(VERSION_ID_DATE_FORMAT);

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

    public FeedSource.FeedRetrievalMethod retrievalMethod;

    @JsonView(JsonViews.UserInterface.class)
    @JsonProperty("feedSource")
    public FeedSource parentFeedSource() {
        return Persistence.feedSources.getById(feedSourceId);
    }

    /**
     * Finds the previous version (i.e., the version loaded directly before the current version in time order).
     * @return the previous feed version or <code>null</code> if this is the first version
     */
    public FeedVersion previousVersion() {
        return Persistence.feedVersions.getOneFiltered(and(
                eq("version", this.version - 1), eq("feedSourceId", this.feedSourceId)), null);
    }

    /**
     * JSON view to show the previous version ID.
     */
    @JsonView(JsonViews.UserInterface.class)
    @JsonProperty("previousVersionId")
    public String previousVersionId() {
        FeedVersion p = previousVersion();
        return p != null ? p.id : null;
    }

    /**
     * Finds the next version (i.e., the version loaded directly after the current version in time order).
     * @return the next feed version or <code>null</code> if this is the latest version
     */
    public FeedVersion nextVersion() {
        return Persistence.feedVersions.getOneFiltered(and(
                eq("version", this.version + 1), eq("feedSourceId", this.feedSourceId)), null);
    }

    /**
     * JSON view to show the next version ID.
     */
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

    public File retrieveGtfsFile() {
        return feedStore.getFeed(id);
    }

    public File newGtfsFile(InputStream inputStream) {
        File file = feedStore.newFeed(id, inputStream, parentFeedSource());
        // fileSize field will not be stored until new FeedVersion is stored in MongoDB (usually in
        // the final steps of ValidateFeedJob).
        this.fileSize = file.length();
        LOG.info("New GTFS file saved: {} ({} bytes)", id, this.fileSize);
        return file;
    }
    public File newGtfsFile(InputStream inputStream, Long lastModified) {
        File file = newGtfsFile(inputStream);
        // fileTimestamp field will not be stored until new FeedVersion is stored in MongoDB (usually in
        // the final steps of ValidateFeedJob).
        if (lastModified != null) {
            this.fileTimestamp = lastModified;
            file.setLastModified(lastModified);
        } else {
            this.fileTimestamp = file.lastModified();
        }
        return file;
    }
    // FIXME return sql-loader Feed object.
    @JsonIgnore
    public Feed retrieveFeed() {
        if (feedLoadResult != null) {
            return new Feed(DataManager.GTFS_DATA_SOURCE, feedLoadResult.uniqueIdentifier);
        } else {
            return null;
        }
    }

    /** The results of validating this feed */
    public ValidationResult validationResult;

    public FeedLoadResult feedLoadResult;

    @JsonView(JsonViews.UserInterface.class)
    @BsonProperty("validationSummary")
    public FeedValidationResultSummary validationSummary() {
        return new FeedValidationResultSummary(validationResult, feedLoadResult);
    }


    /** When this version was uploaded/fetched */
    public Date updated;

    /** The version of the feed, starting with 0 for the first and so on */
    public int version;

    /** A name for this version. Defaults to creation date if not specified by user */
    public String name;

    /** The size of the original GTFS file uploaded/fetched */
    public Long fileSize;

    /** The last modified timestamp of the original GTFS file uploaded/fetched */
    public Long fileTimestamp;

    /** SQL namespace for GTFS data */
    public String namespace;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String formattedTimestamp() {
        SimpleDateFormat format = new SimpleDateFormat(HUMAN_READABLE_TIMESTAMP_FORMAT);
        return format.format(this.updated);
    }

    public void load(MonitorableJob.Status status, boolean isNewVersion) {
        File gtfsFile;
        // STEP 1. LOAD GTFS feed into relational database
        try {
            status.update(false,"Unpacking feed...", 15.0);
            // Get SQL schema namespace for the feed version. This is needed for reconnecting with feeds
            // in the database.
            gtfsFile = retrieveGtfsFile();
            if (gtfsFile.length() == 0) {
                throw new IOException("Empty GTFS file supplied");
            }
            // If feed version has not been hashed, hash it here.
            if (hash == null) hash = HashUtils.hashFile(gtfsFile);
            String gtfsFilePath = gtfsFile.getPath();
            this.feedLoadResult = GTFS.load(gtfsFilePath, DataManager.GTFS_DATA_SOURCE);
            // FIXME? duplication of namespace (also stored as feedLoadResult.uniqueIdentifier)
            this.namespace = feedLoadResult.uniqueIdentifier;
            LOG.info("Loaded GTFS into SQL {}", feedLoadResult.uniqueIdentifier);
        } catch (Exception e) {
            String errorString = String.format("Error loading GTFS feed for version: %s", this.id);
            LOG.warn(errorString, e);
            status.update(true, errorString, 0);
            // FIXME: Delete local copy of feed version after failed load?
            return;
        }

        // FIXME: is this the right approach?
        // if load was unsuccessful, update status and return
        if(this.feedLoadResult == null) {
            String errorString = String.format("Could not load GTFS for FeedVersion %s", id);
            LOG.error(errorString);
            status.update(true, errorString, 0);
            // FIXME: Delete local copy of feed version after failed load?
            return;
        }

        // STEP 2. Upload GTFS to S3 (storage on local machine is done when feed is fetched/uploaded)
        if (DataManager.useS3) {
            try {
                boolean fileUploaded = false;
                if (isNewVersion) {
                    // Only upload file to S3 if it is a new version (otherwise, it would have been downloaded from here.
                    fileUploaded = FeedVersion.feedStore.uploadToS3(gtfsFile, this.id, this.parentFeedSource());
                }
                if (fileUploaded || !isNewVersion) {
                    // Note: If feed is not a new version, it is presumed to already exist on S3, so uploading is not required.
                    // Delete local copy of feed version after successful s3 upload
                    boolean fileDeleted = gtfsFile.delete();
                    if (fileDeleted) {
                        LOG.info("Local GTFS file deleted after s3 upload");
                    } else {
                        LOG.error("Local GTFS file failed to delete. Server may encounter storage capacity issues!");
                    }
                } else {
                    LOG.error("Local GTFS file not uploaded not successfully to s3!");
                }
                // FIXME: should this happen here?
                FeedSource fs = parentFeedSource();
                if (fs.isPublic) {
                    // make feed version public... this shouldn't take very long
                    fs.makePublic();
                }
            } catch (Exception e) {
                LOG.error("Could not upload version {} to s3 bucket", this.id);
                e.printStackTrace();
            }
        }
    }

    /**
     * Validate a version of GTFS. This method actually does a little more processing than just validation.
     * Because validate() is run on all GTFS feeds whether they're fetched, created from an editor snapshot,
     * uploaded manually, or god knows however else, we need a single function to handle loading a feed into
     * the relational database, validating the data, and storing that validation result. Since those
     * processes more or less happen in a tight sequence, we handle all of that here.
     *
     * This function is called in the job logic of a MonitorableJob. When the job is complete, the validated
     * FeedVersion will be stored in MongoDB along with the ValidationResult and other fields populated during
     * validation.
     */
    public void validate(MonitorableJob.Status status) {

        // Sometimes this method is called when no status object is available.
        if (status == null) status = new MonitorableJob.Status();

        // VALIDATE GTFS feed
        try {
            LOG.info("Beginning validation...");
            // run validation on feed version
            // FIXME: pass status to validate? Or somehow listen to events?
            validationResult = GTFS.validate(feedLoadResult.uniqueIdentifier, DataManager.GTFS_DATA_SOURCE);
        } catch (Exception e) {
            String message = String.format("Unable to validate feed %s", this.id);
            LOG.error(message, e);
            status.update(true, message, 100, true);
            // FIXME create validation result with new constructor?
            validationResult = new ValidationResult();
            validationResult.fatalException = "failure!";
            return;
        }
    }

    public void validate() {
        validate(null);
    }

    public void hash () {
        this.hash = HashUtils.hashFile(retrieveGtfsFile());
    }

    /**
     * This reads in
     * @return
     */
    public TransportNetwork readTransportNetwork() {
        TransportNetwork transportNetwork = null;
        try {
            transportNetwork = TransportNetwork.read(transportNetworkPath());
            // check to see if distance tables are built yet... should be removed once better caching strategy is implemented.
            if (transportNetwork.transitLayer.stopToVertexDistanceTables == null) {
                transportNetwork.transitLayer.buildDistanceTables(null);
            }
        } catch (Exception e) {
            LOG.error("Could not read transport network for version {}", id);
            e.printStackTrace();
        }
        return transportNetwork;
    }

    public TransportNetwork buildTransportNetwork(MonitorableJob.Status status) {
        // return null if validation result is null (probably means something went wrong with validation, plus we won't have feed bounds).
        if (this.validationResult == null || validationResult.fatalException != null) {
            return null;
        }

        // Sometimes this method is called when no status object is available.
        if (status == null) status = new MonitorableJob.Status();

        // Fetch OSM extract
        status.update(false, "Fetching OSM extract...", 10);

        // FIXME: don't convert to Rectangle2D?
        Rectangle2D bounds = this.validationResult.fullBounds.toRectangle2D();

        if (bounds == null) {
            String message = String.format("Could not build network for %s because feed bounds are unknown.", this.id);
            LOG.warn(message);
            status.update(true, message, 10);
            return null;
        }

        File osmExtract = downloadOSMFile(bounds);
        if (!osmExtract.exists()) {
            InputStream is = downloadOsmExtract(bounds);
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
        status.update(false, "Creating transport network...", 50);

        // FIXME: fix sql-loader integration to work with r5 TransportNetwork. Currently it provides an empty list of
        // feeds.
        List<GTFSFeed> feedList = new ArrayList<>();
//        feedList.add(retrieveFeed());
        TransportNetwork tn;
        try {
            tn = TransportNetwork.fromFeeds(osmExtract.getAbsolutePath(), feedList, TNBuilderConfig.defaultConfig());
        } catch (Exception e) {
            String message = String.format("Unknown error encountered while building network for %s.", this.id);
            LOG.warn(message, e);
            // Delete the OSM extract directory because it is probably corrupted and may cause issues for the next
            // version loaded with the same bounds.
            File osmDirectory = osmExtract.getParentFile();
            LOG.info("Deleting OSM dir for this version {}", osmDirectory.getAbsolutePath());
            try {
                FileUtils.deleteDirectory(osmDirectory);
            } catch (IOException e1) {
                LOG.error("Could not delete OSM dir", e);
            }
            status.update(true, message, 100);
            status.exceptionType = e.getMessage();
            status.exceptionDetails = ExceptionUtils.getStackTrace(e);
            return null;
        }
        tn.transitLayer.buildDistanceTables(null);
        File tnFile = transportNetworkPath();
        try {
            tn.write(tnFile);
            return tn;
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
        return hasCriticalErrorsExceptingDate() ||
                validationResult.lastCalendarDate == null ||
                (LocalDate.now()).isAfter(validationResult.lastCalendarDate);
    }

    /**
     * Does this feed have any critical errors other than possibly being expired?
     * @return whether feed version has critical errors (outside of expiration)
     */
    private boolean hasCriticalErrorsExceptingDate() {
        if (validationResult == null)
            return true;

        return validationResult.fatalException != null ||
            feedLoadResult.stopTimes.rowCount == 0 ||
            feedLoadResult.trips.rowCount == 0 ||
            feedLoadResult.agency.rowCount == 0;

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

        // FIXME: this is really messy.
        Long timestamp = feedStore.getFeedLastModified(id);
        Persistence.feedVersions.updateField(id, "fileTimestamp", timestamp);

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

        // FIXME: this is really messy.
        Long feedVersionSize = feedStore.getFeedSize(id);
        Persistence.feedVersions.updateField(id, "fileSize", feedVersionSize);

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
