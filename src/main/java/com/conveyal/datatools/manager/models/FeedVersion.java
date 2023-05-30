package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.common.utils.Scheduler;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.jobs.ValidateFeedJob;
import com.conveyal.datatools.manager.jobs.ValidateMobilityDataFeedJob;
import com.conveyal.datatools.manager.jobs.validation.RouteTypeValidatorBuilder;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.HashUtils;
import com.conveyal.gtfs.BaseGTFSCache;
import com.conveyal.gtfs.GTFS;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.loader.FeedLoadResult;
import com.conveyal.gtfs.validator.MTCValidator;
import com.conveyal.gtfs.validator.ValidationResult;
import com.conveyal.gtfs.validator.model.Priority;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import org.bson.Document;
import org.bson.codecs.pojo.annotations.BsonProperty;
import org.mobilitydata.gtfsvalidator.runner.ValidationRunner;
import org.mobilitydata.gtfsvalidator.runner.ValidationRunnerConfig;
import org.mobilitydata.gtfsvalidator.util.VersionResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Date;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.conveyal.datatools.manager.DataManager.GTFS_DATA_SOURCE;
import static com.conveyal.datatools.manager.DataManager.isExtensionEnabled;
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
     * Input feed versions used to create a merged version.
     */
    public Set<String> inputVersions;

    /**
     * This is not the recommended way to create a new feed version. This constructor should primarily be used in
     * testing, when shorthand is OK. Generally, the retrieval method should be provided for more precision about where
     * the version came from.
     */
    public FeedVersion(FeedSource source) {
        this(source, source.retrievalMethod);
    }

    /**
     * This is the recommended constructor for creating a new feed version. This is generally called before the GTFS
     * file has been supplied because we store the GTFS file at a location based the feed version ID (which is generated
     * in this constructor). Call {@link FeedStore#getFeedFile} to determine where its GTFS should be stored and
     * {@link #assignGtfsFileAttributes} to associate characteristics of the GTFS file with the version.
     * @param source            the parent feed source
     * @param retrievalMethod   how the version's GTFS was supplied to Data Tools
     */
    public FeedVersion(FeedSource source, FeedRetrievalMethod retrievalMethod) {
        this.updated = new Date();
        this.feedSourceId = source.id;
        this.name = formattedTimestamp() + " Version";
        // We generate IDs manually, but we need a bit of information to do so
        this.id = generateFeedVersionId(source);
        this.retrievalMethod = retrievalMethod;
    }

    public FeedVersion(FeedSource source, Snapshot snapshot) {
        this(source, snapshot.retrievalMethod);
        // Set feed version properties.
        originNamespace = snapshot.namespace;
        name = snapshot.name + " Snapshot Export";
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
    public String feedSourceId;

    public FeedRetrievalMethod retrievalMethod;

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

    /**
     * Store a new GTFS file from an input stream representing the GTFS zip file.
     */
    public File newGtfsFile(InputStream inputStream) throws IOException {
        File file = feedStore.newFeed(id, inputStream, parentFeedSource());
        assignGtfsFileAttributes(file);
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

    /** The results of loading this feed into the GTFS database */
    public FeedLoadResult feedLoadResult;

    /** The results of transforming this feed into the GTFS database */
    public FeedTransformResult feedTransformResult;

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

    public Document mobilityDataResult;

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
            // If somehow feed version has not already had GTFS file attributes assigned during stages prior to load,
            // handle this here.
            assignGtfsFileAttributes(gtfsFile);
            String gtfsFilePath = gtfsFile.getPath();
            this.feedLoadResult = GTFS.load(gtfsFilePath, DataManager.GTFS_DATA_SOURCE);
            if (this.feedLoadResult.fatalException != null) {
                status.fail("Could not load feed due to " + feedLoadResult.fatalException);
                return;
            }
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

        // VALIDATE GTFS feed.
        try {
            LOG.info("Beginning validation...");

            // FIXME: pass status to validate? Or somehow listen to events?
            status.update("Validating feed...", 33);

            // Validate the feed version.
            // Certain extensions, if enabled, have extra validators
            if (isExtensionEnabled("mtc")) {
                validationResult = GTFS.validate(feedLoadResult.uniqueIdentifier, DataManager.GTFS_DATA_SOURCE,
                    RouteTypeValidatorBuilder::buildRouteValidator,
                    MTCValidator::new
                );
            } else {
                validationResult = GTFS.validate(feedLoadResult.uniqueIdentifier, DataManager.GTFS_DATA_SOURCE,
                    RouteTypeValidatorBuilder::buildRouteValidator
                );
            }
        } catch (Exception e) {
            status.fail(String.format("Unable to validate feed %s", this.id), e);
            // FIXME create validation result with new constructor?
            validationResult = new ValidationResult();
            validationResult.fatalException = "failure!";
        }
    }

    public void validateMobility(MonitorableJob.Status status) {

        // Sometimes this method is called when no status object is available.
        if (status == null) status = new MonitorableJob.Status();

        // VALIDATE GTFS feed
        try {
            LOG.info("Beginning MobilityData validation...");
            status.update("MobilityData Analysis...", 11);

            // Wait for the file to be entirely copied into the directory.
            // 5 seconds + ~1 second per 10mb
            Thread.sleep(5000 + (this.fileSize / 10000));
            File gtfsZip = this.retrieveGtfsFile();
            // Namespace based folders avoid clash for validation being run on multiple versions of a feed.
            // TODO: do we know that there will always be a namespace?
            String validatorOutputDirectory = "/tmp/datatools_gtfs/" + this.namespace + "/";

            status.update("MobilityData Analysis...", 20);
            // Set up MobilityData validator.
            ValidationRunnerConfig.Builder builder = ValidationRunnerConfig.builder();
            builder.setGtfsSource(gtfsZip.toURI());
            builder.setOutputDirectory(Path.of(validatorOutputDirectory));
            ValidationRunnerConfig mbValidatorConfig = builder.build();

            status.update("MobilityData Analysis...", 40);
            // Run MobilityData validator
            ValidationRunner runner = new ValidationRunner(new VersionResolver());
            runner.run(mbValidatorConfig);

            status.update("MobilityData Analysis...", 80);
            // Read generated report and save to Mongo.
            String json;
            try (FileReader fr = new FileReader(validatorOutputDirectory + "report.json")) {
                BufferedReader in = new BufferedReader(fr);
                json = in.lines().collect(Collectors.joining(System.lineSeparator()));
            }

            // This will persist the document to Mongo.
            this.mobilityDataResult = Document.parse(json);
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

    /**
     * Does this feed version have any critical errors that would prevent it being loaded to OTP?
     * @return whether the feed version has any critical errors
     */
    public boolean hasCriticalErrors() {
        return hasValidationAndLoadErrors() ||
            hasFeedVersionExpired() ||
            hasHighSeverityErrorTypes();
    }

    /**
     * Does this feed have any validation or load errors?
     * @return whether feed version has any validation or load errors
     */
    private boolean hasValidationAndLoadErrors() {
        if (validationResult == null)
            return true;
        return validationResult.fatalException != null ||
            !validationSummary().bounds.areValid() ||
            feedLoadResult.stopTimes.rowCount == 0 ||
            feedLoadResult.trips.rowCount == 0 ||
            feedLoadResult.agency.rowCount == 0;
    }

    /**
     * Has this feed expired?
     * @return If the validation result last calendar date is null or has expired return true, else return false.
     */
    private boolean hasFeedVersionExpired() {
        return validationResult.lastCalendarDate == null ||
            LocalDate.now().isAfter(validationResult.lastCalendarDate);
    }

    /**
     * Has this feed version produced any high severity error types when being validated?
     * @return whether high severity error types have been flagged.
     */
    private boolean hasHighSeverityErrorTypes() {
        return hasSpecificErrorTypes(Stream.of(NewGTFSErrorType.values())
            .filter(type -> type.priority == Priority.HIGH));
    }

    /**
     * Checks for issues that block feed publishing, consistent with UI.
     */
    public boolean hasBlockingIssuesForPublishing() {
        if (this.validationResult.fatalException != null) return true;

        return hasSpecificErrorTypes(Stream.of(
            NewGTFSErrorType.ILLEGAL_FIELD_VALUE,
            NewGTFSErrorType.MISSING_COLUMN,
            NewGTFSErrorType.REFERENTIAL_INTEGRITY,
            NewGTFSErrorType.SERVICE_WITHOUT_DAYS_OF_WEEK,
            NewGTFSErrorType.TABLE_MISSING_COLUMN_HEADERS,
            NewGTFSErrorType.TABLE_IN_SUBDIRECTORY,
            NewGTFSErrorType.WRONG_NUMBER_OF_FIELDS
        ));
    }

    /**
     * Determines whether this feed has specific error types.
     */
    private boolean hasSpecificErrorTypes(Stream<NewGTFSErrorType> errorTypes) {
        Set<String> highSeverityErrorTypes = errorTypes
            .map(NewGTFSErrorType::toString)
            .collect(Collectors.toSet());
        try (Connection connection = GTFS_DATA_SOURCE.getConnection()) {
            String sql = String.format("select distinct error_type from %s.errors", namespace);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            LOG.info(preparedStatement.toString());
            ResultSet resultSet = preparedStatement.executeQuery();
            // Check if any of the error types found in the table are "high priority/severity"
            while (resultSet.next()) {
                String errorType = resultSet.getString(1);
                if (highSeverityErrorTypes.contains(errorType)) return true;
            }
        } catch (SQLException e) {
            LOG.error("Unable to determine if feed version {} produced any high severity error types", name, e);
            // If the SQL query failed, there is likely something wrong with the error table, which suggests the feed
            // is invalid for one reason or another.
            return true;
        }

        return false;
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
     * 4. Finally delete the version object from the database.
     */
    public void delete() {
        try {
            // reset lastModified if feed is latest version
            LOG.info("Deleting feed version {}", this.id);
            String id = this.id;
            // Remove any notes for this feed version
            retrieveNotes(true).forEach(Note::delete);
            FeedSource fs = parentFeedSource();
            FeedVersion latest = fs.retrieveLatest();
            if (latest != null && latest.id.equals(this.id)) {
                // Even if there are previous feed versions, we set to null to allow re-fetching the version that was just deleted
                // TODO instead, set it to the fetch time of the previous feed version
                fs.lastFetched = null;
                Persistence.feedSources.replace(fs.id, fs);
            }
            ensurePublishedVersionIdIsUnset(fs);

            feedStore.deleteFeed(id);
            // Delete feed version tables in GTFS database
            GTFS.delete(this.namespace, DataManager.GTFS_DATA_SOURCE);
            LOG.info("Dropped version's GTFS tables from Postgres.");
            // Remove this FeedVersion from all Deployments associated with this FeedVersion's FeedSource's Project
            // TODO TEST THOROUGHLY THAT THIS UPDATE EXPRESSION IS CORRECT
            // Although outright deleting the feedVersion from deployments could be surprising and shouldn't be done anyway.
            Persistence.deployments.getMongoCollection().updateMany(eq("projectId", this.parentFeedSource().projectId),
                    pull("feedVersionIds", this.id));
            Persistence.feedVersions.removeById(this.id);
            fs.renumberFeedVersions();

            // recalculate feed expiration notifications in case the latest version has changed
            Scheduler.scheduleExpirationNotifications(fs);

            LOG.info("Version {} deleted", id);
        } catch (Exception e) {
            LOG.warn("Error deleting version", e);
        }
    }

    /**
     * If this feed version is referenced in the parent feed source by publishedVersionId,
     * ensure that the field is set to null.
     */
    private void ensurePublishedVersionIdIsUnset(FeedSource fs) {
        if (this.namespace != null && this.namespace.equals(fs.publishedVersionId)) {
            Persistence.feedSources.updateField(fs.id, "publishedVersionId", null);
        }
    }

    /**
     * Assign characteristics from a new GTFS file to the feed version. This should generally be called directly after
     * constructing the feed version (assuming the GTFS file is available). Characteristics set from file include:
     * - last modified (file timestamp)
     * - length (file size)
     * - hash
     * @param newGtfsFile   the new GTFS file
     * @param lastModifiedOverride  optional override of the file's last modified value
     */
    public void assignGtfsFileAttributes(File newGtfsFile, Long lastModifiedOverride) {
        if (lastModifiedOverride != null) {
            newGtfsFile.setLastModified(lastModifiedOverride);
            fileTimestamp = lastModifiedOverride;
        } else {
            fileTimestamp = newGtfsFile.lastModified();
        }
        fileSize = newGtfsFile.length();
        if (hash == null) hash = HashUtils.hashFile(newGtfsFile);
    }

    /**
     * Convenience wrapper for {@link #assignGtfsFileAttributes} that does not override file's last modified.
     */
    public void assignGtfsFileAttributes(File newGtfsFile) {
        assignGtfsFileAttributes(newGtfsFile, null);
    }

    /**
     * Determines whether this feed version matches another one specified, i.e.,
     * whether the otherVersion doesn't have a different hash, thus has not changed, compared to this one.
     * @param otherVersion The version to compare the hash to.
     * @return true if the otherVersion hash is the same, false if the hashes differ or the otherVersion is null.
     */
    public boolean isSameAs(FeedVersion otherVersion) {
        return otherVersion != null && this.hash.equals(otherVersion.hash);
    }

    /**
     * {@link ValidateFeedJob} and {@link ValidateMobilityDataFeedJob} both require to save a feed version after their
     * subsequent validation checks have completed. Either could finish first, therefore this method makes sure that
     * only one instance is saved (the last to finish updates).
     */
    public void persistFeedVersionAfterValidation(boolean isNewVersion) {
        if (isNewVersion && Persistence.feedVersions.getById(id) == null) {
            int count = parentFeedSource().feedVersionCount();
            version = count + 1;
            Persistence.feedVersions.create(this);
        } else {
            Persistence.feedVersions.replace(id, this);
        }
    }
}
