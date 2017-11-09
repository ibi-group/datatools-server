package com.conveyal.datatools.manager.models;

import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.editor.datastore.GlobalTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.jobs.NotifyUsersForSubscriptionJob;
import com.conveyal.datatools.manager.persistence.DataStore;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.utils.HashUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonView;
import com.google.common.eventbus.EventBus;
import org.mapdb.Fun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.HaltException;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.conveyal.datatools.manager.utils.StringUtils.getCleanName;
import static spark.Spark.halt;

/**
 * Created by demory on 3/22/16.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FeedSource extends Model implements Cloneable {
    private static final long serialVersionUID = 1L;

    public static final Logger LOG = LoggerFactory.getLogger(FeedSource.class);

    private static DataStore<FeedSource> sourceStore = new DataStore<FeedSource>("feedsources");

    /**
     * The collection of which this feed is a part
     */
    //@JsonView(JsonViews.DataDump.class)
    public String projectId;

    public String[] regions = {"1"};
    /**
     * Get the Project of which this feed is a part
     */
    @JsonIgnore
    public Project getProject () {
        return projectId != null ? Project.get(projectId) : null;
    }

    public String getOrganizationId () {
        Project project = getProject();
        return project == null ? null : project.organizationId;
    }

    @JsonIgnore
    public List<Region> getRegionList () {
        return Region.getAll().stream().filter(r -> Arrays.asList(regions).contains(r.id)).collect(Collectors.toList());
    }

    public void setProject(Project proj) {
        this.projectId = proj.id;
        this.save();
        proj.save();
    }

    /** The name of this feed source, e.g. MTA New York City Subway */
    public String name;

    /** Is this feed public, i.e. should it be listed on the
     * public feeds page for download?
     */
    public boolean isPublic;

    /** Is this feed deployable? */
    public boolean deployable;

    /**
     * How do we receive this feed?
     */
    public FeedRetrievalMethod retrievalMethod;

    /**
     * When was this feed last fetched?
     */
    public Date lastFetched;

    /**
     * When was this feed last updated?
     */
    public transient Date lastUpdated;

    /**
     * From whence is this feed fetched?
     */
    public URL url;

    /**
     * Where the feed exists on s3
     */
    public URL s3Url;

    /**
     * What is the GTFS Editor snapshot for this feed?
     *
     * This is the String-formatted snapshot ID, which is the base64-encoded ID and the version number.
     */
    public String snapshotVersion;

    public String publishedVersionId;

    /**
     * Create a new feed.
     */
    public FeedSource (String name) {
        super();
        this.name = name;
        this.retrievalMethod = FeedRetrievalMethod.MANUALLY_UPLOADED;
    }

    /**
     * No-arg constructor to yield an uninitialized feed source, for dump/restore.
     * Should not be used in general code.
     */
    public FeedSource () {
        this(null);
    }

    /**
     * Fetch the latest version of the feed.
     */
    public FeedVersion fetch (EventBus eventBus, String fetchUser) {
        Map<String, Object> statusMap = new HashMap<>();
        statusMap.put("message", "Downloading file");
        statusMap.put("percentComplete", 20.0);
        statusMap.put("error", false);
        eventBus.post(statusMap);

        FeedVersion latest = getLatest();

        // We create a new FeedVersion now, so that the fetched date is (milliseconds) before
        // fetch occurs. That way, in the highly unlikely event that a feed is updated while we're
        // fetching it, we will not miss a new feed.
        FeedVersion version = new FeedVersion(this);

        // build the URL from which to fetch
        URL url = this.url;
        LOG.info("Fetching from {}", url.toString());

        // make the request, using the proper HTTP caching headers to prevent refetch, if applicable
        HttpURLConnection conn;
        try {
            conn = (HttpURLConnection) url.openConnection();
        } catch (IOException e) {
            String message = String.format("Unable to open connection to %s; not fetching feed %s", url, this.name);
            LOG.error(message);
            statusMap.put("message", message);
            statusMap.put("percentComplete", 0.0);
            statusMap.put("error", true);
            eventBus.post(statusMap);
            halt(400, message);
            return null;
        } catch (ClassCastException e) {
            String message = String.format("Unable to open connection to %s; not fetching %s feed", url, this.name);
            LOG.error(message);
            statusMap.put("message", message);
            statusMap.put("percentComplete", 0.0);
            statusMap.put("error", true);
            eventBus.post(statusMap);
            halt(400, message);
            return null;
        } catch (NullPointerException e) {
            String message = String.format("Unable to open connection to %s; not fetching %s feed", url, this.name);
            LOG.error(message);
            statusMap.put("message", message);
            statusMap.put("percentComplete", 0.0);
            statusMap.put("error", true);
            eventBus.post(statusMap);
            halt(400, message);
            return null;
        }

        conn.setDefaultUseCaches(true);

        // lastFetched is set to null when the URL changes and when latest feed version is deleted
        if (latest != null && this.lastFetched != null)
            conn.setIfModifiedSince(Math.min(latest.updated.getTime(), this.lastFetched.getTime()));

        File newGtfsFile;

        try {
            conn.connect();
            String message;
            switch (conn.getResponseCode()) {
                case HttpURLConnection.HTTP_NOT_MODIFIED:
                    message = String.format("Feed %s has not been modified", this.name);
                    LOG.warn(message);
                    statusMap.put("message", message);
                    statusMap.put("percentComplete", 100.0);
                    statusMap.put("error", true);
                    eventBus.post(statusMap);
                    halt(304, SparkUtils.formatJSON(message, 304));
                    return null;
                case HttpURLConnection.HTTP_OK:
                case HttpURLConnection.HTTP_MOVED_TEMP:
                case HttpURLConnection.HTTP_MOVED_PERM:
                    message = String.format("Saving %s feed.", this.name);
                    LOG.info(message);
                    statusMap.put("message", message);
                    statusMap.put("percentComplete", 75.0);
                    statusMap.put("error", false);
                    eventBus.post(statusMap);
                    newGtfsFile = version.newGtfsFile(conn.getInputStream());
                    break;
                default:
                    message = String.format("HTTP status (%d: %s) retrieving %s feed", conn.getResponseCode(), conn.getResponseMessage(), this.name);
                    LOG.error(message);
                    statusMap.put("message", message);
                    statusMap.put("percentComplete", 100.0);
                    statusMap.put("error", true);
                    eventBus.post(statusMap);
                    halt(400, SparkUtils.formatJSON(message, 400));
                    return null;
            }
        } catch (IOException e) {
            String message = String.format("Unable to connect to %s; not fetching %s feed", url, this.name);
            LOG.error(message);
            statusMap.put("message", message);
            statusMap.put("percentComplete", 100.0);
            statusMap.put("error", true);
            eventBus.post(statusMap);
            e.printStackTrace();
            halt(400, SparkUtils.formatJSON(message, 400, e));
            return null;
        } catch (HaltException e) {
            LOG.warn("Halt thrown", e);
            throw e;
        }

        // note that anything other than a new feed fetched successfully will have already returned from the function
//        version.hash();
        version.hash = HashUtils.hashFile(newGtfsFile);


        if (latest != null && version.hash.equals(latest.hash)) {
            String message = String.format("Feed %s was fetched but has not changed; server operators should add If-Modified-Since support to avoid wasting bandwidth", this.name);
            LOG.warn(message);
            newGtfsFile.delete();
            version.delete();
            statusMap.put("message", message);
            statusMap.put("percentComplete", 100.0);
            statusMap.put("error", true);
            eventBus.post(statusMap);
            halt(304);
            return null;
        }
        else {
            version.userId = this.userId;

            this.lastFetched = version.updated;
            this.save();

            NotifyUsersForSubscriptionJob notifyFeedJob = new NotifyUsersForSubscriptionJob("feed-updated", this.id, "New feed version created for " + this.name);
            DataManager.lightExecutor.execute(notifyFeedJob);

            String message = String.format("Fetch complete for %s", this.name);
            LOG.info(message);
            statusMap.put("message", message);
            statusMap.put("percentComplete", 100.0);
            statusMap.put("error", false);
            eventBus.post(statusMap);
            version.setUserById(fetchUser);
            version.fileTimestamp = conn.getLastModified();
            return version;
        }
    }

    public int compareTo(FeedSource o) {
        return this.name.compareTo(o.name);
    }

    public String toString () {
        return "<FeedSource " + this.name + " (" + this.id + ")>";
    }

    public void save () {
        save(true);
    }
    public void setName(String name){
        this.name = name;
        this.save();
    }
    public void save (boolean commit) {
        if (commit)
            sourceStore.save(this.id, this);
        else
            sourceStore.saveWithoutCommit(this.id, this);
    }

    /**
     * Get the latest version of this feed
     * @return the latest version of this feed
     */
    @JsonIgnore
    public FeedVersion getLatest () {
        FeedVersion v = FeedVersion.versionStore.findFloor("version", new Fun.Tuple2(this.id, Fun.HI));

        // the ID doesn't necessarily match, because it will fall back to the previous source in the store if there are no versions for this source
        if (v == null || !v.feedSourceId.equals(this.id))
            return null;

        return v;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonView(JsonViews.UserInterface.class)
    public String getLatestVersionId () {
        FeedVersion latest = getLatest();
        return latest != null ? latest.id : null;
    }

    /**
     * We can't pass the entire latest feed version back, because it contains references back to this feedsource,
     * so Jackson doesn't work. So instead we specifically expose the validation results and the latest update.
     * @return
     */
    // TODO: use summarized feed source here. requires serious refactoring on client side.
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonView(JsonViews.UserInterface.class)
    public Date getLastUpdated() {
        FeedVersion latest = getLatest();
        return latest != null ? latest.updated : null;
    }


    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonView(JsonViews.UserInterface.class)
    public FeedValidationResultSummary getLatestValidation () {
        FeedVersion latest = getLatest();
        FeedValidationResult result = latest != null ? latest.validationResult : null;
        return result != null ?new FeedValidationResultSummary(result) : null;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonView(JsonViews.UserInterface.class)
    public boolean getEditedSinceSnapshot() {
//        FeedTx tx;
//        try {
//            tx = VersionedDataStore.getFeedTx(id);
//        } catch (Exception e) {
//
//        }
//        return tx.editedSinceSnapshot.get();
        return false;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonView(JsonViews.UserInterface.class)
    public Map<String, Map<String, String>> getExternalProperties() {

        Map<String, Map<String, String>> resourceTable = new HashMap<>();

        for(String resourceType : DataManager.feedResources.keySet()) {
            Map<String, String> propTable = new HashMap<>();

            ExternalFeedSourceProperty.getAll().stream()
                    .filter(prop -> prop.getFeedSourceId().equals(this.id))
                    .forEach(prop -> propTable.put(prop.name, prop.value));

            resourceTable.put(resourceType, propTable);
        }
        return resourceTable;
    }

    public static FeedSource get(String id) {
        return sourceStore.getById(id);
    }

    public static Collection<FeedSource> getAll() {
        return sourceStore.getAll();
    }

    /**
     * Get all of the feed versions for this source
     * @return collection of feed versions
     */
    @JsonIgnore
    public Collection<FeedVersion> getFeedVersions() {
        // TODO Indices
        return FeedVersion.getAll().stream()
                .filter(v -> this.id.equals(v.feedSourceId))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    @JsonView(JsonViews.UserInterface.class)
    public int getFeedVersionCount() {
        return getFeedVersions().size();
    }

    @JsonView(JsonViews.UserInterface.class)
    public int getNoteCount() {
        return this.noteIds != null ? this.noteIds.size() : 0;
    }

    public String getPublicKey () {
        return "public/" + getCleanName(this.name) + ".zip";
    }

    public void makePublic() {
        String sourceKey = FeedStore.s3Prefix + this.id + ".zip";
        String publicKey = getPublicKey();
        String versionId = this.getLatestVersionId();
        String latestVersionKey = FeedStore.s3Prefix + versionId;

        // only deploy to public if storing feeds on s3 (no mechanism for downloading/publishing
        // them otherwise)
        if (DataManager.useS3) {
            boolean sourceExists = FeedStore.s3Client.doesObjectExist(DataManager.feedBucket, sourceKey);
            ObjectMetadata sourceMetadata = sourceExists
                    ? FeedStore.s3Client.getObjectMetadata(DataManager.feedBucket, sourceKey)
                    : null;
            boolean latestExists = FeedStore.s3Client.doesObjectExist(DataManager.feedBucket, latestVersionKey);
            ObjectMetadata latestVersionMetadata = latestExists
                    ? FeedStore.s3Client.getObjectMetadata(DataManager.feedBucket, latestVersionKey)
                    : null;
            boolean latestVersionMatchesSource = sourceMetadata != null &&
                    latestVersionMetadata != null &&
                    sourceMetadata.getETag().equals(latestVersionMetadata.getETag());
            if (sourceExists && latestVersionMatchesSource) {
                LOG.info("copying feed {} to s3 public folder", this);
                FeedStore.s3Client.setObjectAcl(DataManager.feedBucket, sourceKey, CannedAccessControlList.PublicRead);
                FeedStore.s3Client.copyObject(DataManager.feedBucket, sourceKey, DataManager.feedBucket, publicKey);
                FeedStore.s3Client.setObjectAcl(DataManager.feedBucket, publicKey, CannedAccessControlList.PublicRead);
            } else {
                LOG.warn("Latest feed source {} on s3 at {} does not exist or does not match latest version. Using latest version instead.", this, sourceKey);
                if (FeedStore.s3Client.doesObjectExist(DataManager.feedBucket, latestVersionKey)) {
                    LOG.info("copying feed version {} to s3 public folder", versionId);
                    FeedStore.s3Client.setObjectAcl(DataManager.feedBucket, latestVersionKey, CannedAccessControlList.PublicRead);
                    FeedStore.s3Client.copyObject(DataManager.feedBucket, latestVersionKey, DataManager.feedBucket, publicKey);
                    FeedStore.s3Client.setObjectAcl(DataManager.feedBucket, publicKey, CannedAccessControlList.PublicRead);

                    // also copy latest version to feedStore latest
                    FeedStore.s3Client.copyObject(DataManager.feedBucket, latestVersionKey, DataManager.feedBucket, sourceKey);
                }
            }
        }
    }

    public void makePrivate() {
        String sourceKey = FeedStore.s3Prefix + this.id + ".zip";
        String publicKey = getPublicKey();
        if (FeedStore.s3Client.doesObjectExist(DataManager.feedBucket, sourceKey)) {
            LOG.info("removing feed {} from s3 public folder", this);
            FeedStore.s3Client.setObjectAcl(DataManager.feedBucket, sourceKey, CannedAccessControlList.AuthenticatedRead);
            FeedStore.s3Client.deleteObject(DataManager.feedBucket, publicKey);
        }
    }

    /**
     * Represents ways feeds can be retrieved
     */
    public enum FeedRetrievalMethod {
        FETCHED_AUTOMATICALLY, // automatically retrieved over HTTP on some regular basis
        MANUALLY_UPLOADED, // manually uploaded by someone, perhaps the agency, or perhaps an internal user
        PRODUCED_IN_HOUSE // produced in-house in a GTFS Editor instance
    }

    public static void commit() {
        sourceStore.commit();
    }

    /**
     * Delete this feed source and everything that it contains.
     */
    public void delete() {
        getFeedVersions().forEach(FeedVersion::delete);

        // delete latest copy of feed source
        if (DataManager.useS3) {
            DeleteObjectsRequest delete = new DeleteObjectsRequest(DataManager.feedBucket);
            delete.withKeys("public/" + this.name + ".zip", FeedStore.s3Prefix + this.id + ".zip");
            FeedStore.s3Client.deleteObjects(delete);
        }

        // Delete editor feed mapdb
        // TODO: does the mapdb folder need to be deleted separately?
        GlobalTx gtx = VersionedDataStore.getGlobalTx();
        if (!gtx.feeds.containsKey(id)) {
            gtx.rollback();
        }
        else {
            gtx.feeds.remove(id);
            gtx.commit();
        }

        ExternalFeedSourceProperty.getAll().stream()
                .filter(prop -> prop.getFeedSourceId().equals(this.id))
                .forEach(ExternalFeedSourceProperty::delete);

        // TODO: add delete for osm extract and r5 network (maybe that goes with version)

        sourceStore.delete(this.id);
    }

    public FeedSource clone () throws CloneNotSupportedException {
        return (FeedSource) super.clone();
    }

}
