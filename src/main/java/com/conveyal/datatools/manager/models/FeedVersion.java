package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.common.utils.Scheduler;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.HashUtils;
import com.conveyal.gtfs.BaseGTFSCache;
import com.conveyal.gtfs.GTFS;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.loader.FeedLoadResult;
import com.conveyal.gtfs.validator.ValidationResult;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import org.bson.codecs.pojo.annotations.BsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Date;
import java.util.UUID;

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
        String uuid = UUID.randomUUID().toString();
        return BaseGTFSCache.cleanId(String.join("-", getCleanName(source.name), df.format(this.updated), source.id, uuid)) + ".zip";
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

    public File newGtfsFile(InputStream inputStream) throws IOException {
        File file = feedStore.newFeed(id, inputStream, parentFeedSource());
        // fileSize field will not be stored until new FeedVersion is stored in MongoDB (usually in
        // the final steps of ValidateFeedJob).
        this.fileSize = file.length();
        LOG.info("New GTFS file saved: {} ({} bytes)", id, this.fileSize);
        return file;
    }

    /**
     * Construct a connection to the SQL tables for this feed version's namespace to access its stored GTFS data.
     */
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
        return new FeedValidationResultSummary(this);
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

    /**
     * Indicates the namespace from which this version originated. For example, if it was published from a snapshot
     * namespace or a GTFS+ feed, this field will reference that source namespace.
     */
    public String originNamespace;

    /**
     * Indicates when a feed version was published to an external source. If null, the version has not been sent. This
     * field is currently in use only for the MTC extension and is reset to null after the published version has been
     * registered externally.
     * */
    public Date sentToExternalPublisher;

    /**
     * Indicates when a feed version was published to an external source. If null, the version has not been processed by
     * the external publisher or has not been sent (see {@link #sentToExternalPublisher}. This field is currently in use
     * only for the MTC extension and is reset to null after the published version has been registered externally.
     * */
    public Date processedByExternalPublisher;

    public String formattedTimestamp() {
        SimpleDateFormat format = new SimpleDateFormat(HUMAN_READABLE_TIMESTAMP_FORMAT);
        return format.format(this.updated);
    }

    public void load(MonitorableJob.Status status, boolean isNewVersion) {
        File gtfsFile;
        // STEP 1. LOAD GTFS feed into relational database
        try {
            status.update("Unpacking feed...", 15.0);
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
            status.fail(String.format("Error loading GTFS feed for version: %s", this.id), e);
            // FIXME: Delete local copy of feed version after failed load?
            return;
        }

        // FIXME: is this the right approach?
        // if load was unsuccessful, update status and return
        if(this.feedLoadResult == null) {
            status.fail(String.format("Could not load GTFS for FeedVersion %s", id));
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
            status.update("Validating feed...", 33);
            validationResult = GTFS.validate(feedLoadResult.uniqueIdentifier, DataManager.GTFS_DATA_SOURCE);
        } catch (Exception e) {
            status.fail(String.format("Unable to validate feed %s", this.id), e);
            // FIXME create validation result with new constructor?
            validationResult = new ValidationResult();
            validationResult.fatalException = "failure!";
        }
    }

    public void validate() {
        validate(null);
    }

    public void hash () {
        this.hash = HashUtils.hashFile(retrieveGtfsFile());
    }

    /**
     * Get the OSM file for the given bounds if it exists on disk.
     *
     * FIXME: Use osm-lib to handle caching OSM data.
     */
    private static File retrieveCachedOSMFile(Rectangle2D bounds) {
        if (bounds != null) {
            String baseDir = FeedStore.basePath.getAbsolutePath() + File.separator + "osm";
            File osmPath = new File(String.format("%s/%.6f_%.6f_%.6f_%.6f", baseDir, bounds.getMaxX(), bounds.getMaxY(), bounds.getMinX(), bounds.getMinY()));
            if (!osmPath.exists()) {
                osmPath.mkdirs();
            }
            return new File(osmPath.getAbsolutePath() + "/data.osm.pbf");
        } else {
            return null;
        }
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
            !validationSummary().bounds.areValid() ||
            feedLoadResult.stopTimes.rowCount == 0 ||
            feedLoadResult.trips.rowCount == 0 ||
            feedLoadResult.agency.rowCount == 0;

    }

    @JsonView(JsonViews.UserInterface.class)
    @JsonProperty("noteCount")
    public int noteCount() {
        return this.noteIds != null ? this.noteIds.size() : 0;
    }

    /**
     * Delete this feed version and clean up, removing references to it and derived objects and state.
     * Steps:
     * 1. If we are deleting the latest version, change the memoized "last fetched" value in the FeedSource.
     * 2. Delete the GTFS Zip file locally or on S3
     * 3. Remove this feed version from all Deployments [shouldn't we be updating the version rather than deleting it?]
     * 4. Remove the transport network file from the local disk
     * 5. Finally delete the version object from the database.
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
            // Delete feed version tables in GTFS database
            GTFS.delete(this.namespace, DataManager.GTFS_DATA_SOURCE);
            // Remove this FeedVersion from all Deployments associated with this FeedVersion's FeedSource's Project
            // TODO TEST THOROUGHLY THAT THIS UPDATE EXPRESSION IS CORRECT
            // Although outright deleting the feedVersion from deployments could be surprising and shouldn't be done anyway.
            Persistence.deployments.getMongoCollection().updateMany(eq("projectId", this.parentFeedSource().projectId),
                    pull("feedVersionIds", this.id));
            Persistence.feedVersions.removeById(this.id);
            this.parentFeedSource().renumberFeedVersions();

            // recalculate feed expiration notifications in case the latest version has changed
            Scheduler.scheduleExpirationNotifications(fs);

            LOG.info("Version {} deleted", id);
        } catch (Exception e) {
            LOG.warn("Error deleting version", e);
        }
    }
}
