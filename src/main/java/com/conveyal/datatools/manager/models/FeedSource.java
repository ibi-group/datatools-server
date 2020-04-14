package com.conveyal.datatools.manager.models;

import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.GTFS;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Sorts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.conveyal.datatools.manager.utils.StringUtils.getCleanName;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

/**
 * Created by demory on 3/22/16.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FeedSource extends Model implements Cloneable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FeedSource.class);

    /**
     * The collection of which this feed is a part
     */
    //@JsonView(JsonViews.DataDump.class)
    public String projectId;

    /**
     * Get the Project of which this feed is a part
     */
    public Project retrieveProject() {
        return projectId != null ? Persistence.projects.getById(projectId) : null;
    }

    @JsonProperty("organizationId")
    public String organizationId () {
        Project project = retrieveProject();
        return project == null ? null : project.organizationId;
    }

    // TODO: Add back in regions once they have been refactored
//    public List<Region> retrieveRegionList () {
//        return Region.retrieveAll().stream().filter(r -> Arrays.asList(regions).contains(r.id)).collect(Collectors.toList());
//    }

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
     * FIXME: this is currently dynamically determined by lastUpdated() with calls retrieveLatest().
     */
//    public transient Date lastUpdated;

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

    /**
     * The SQL namespace for the most recently verified published {@link FeedVersion}.
     *
     * FIXME During migration to RDBMS for GTFS data, this field changed to map to the SQL unique ID,
     *  however the name of the field suggests it maps to the feed version ID stored in MongoDB. Now
     *  that both published namespace/version ID are available in {@link #publishedValidationSummary()}
     *  it might make sense to migrate this field back to the versionID for MTC (or rename it to
     *  publishedNamespace). Both efforts would require some level of db migration + code changes.
     */
    public String publishedVersionId;

    public String editorNamespace;

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

    public int compareTo(FeedSource o) {
        return this.name.compareTo(o.name);
    }

    public String toString () {
        return "<FeedSource " + this.name + " (" + this.id + ")>";
    }

    /**
     * Get the latest version of this feed
     * @return the latest version of this feed
     */
    @JsonIgnore
    public FeedVersion retrieveLatest() {
        FeedVersion newestVersion = Persistence.feedVersions
                .getOneFiltered(eq("feedSourceId", this.id), Sorts.descending("version"));
        if (newestVersion == null) {
            // Is this what happens if there are none?
            return null;
        }
        return newestVersion;
    }

    /**
     * Fetches the published {@link FeedVersion} for this feed source according to the
     * {@link #publishedVersionId} field (which currently maps to {@link FeedVersion#namespace}.
     */
    public FeedVersion retrievePublishedVersion() {
        if (this.publishedVersionId == null) return null;
        FeedVersion publishedVersion = Persistence.feedVersions
            // Sort is unnecessary here.
            .getOneFiltered(eq("namespace", this.publishedVersionId), Sorts.descending("version"));
        if (publishedVersion == null) {
            // Is this what happens if there are none?
            return null;
        }
        return publishedVersion;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonView(JsonViews.UserInterface.class)
    @JsonProperty("publishedValidationSummary")
    private FeedValidationResultSummary publishedValidationSummary() {
        FeedVersion publishedVersion = retrievePublishedVersion();
        return publishedVersion != null ? new FeedValidationResultSummary(publishedVersion) : null;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonView(JsonViews.UserInterface.class)
    @JsonProperty("latestVersionId")
    public String latestVersionId() {
        FeedVersion latest = retrieveLatest();
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
    @JsonProperty("lastUpdated")
    public Date lastUpdated() {
        FeedVersion latest = retrieveLatest();
        return latest != null ? latest.updated : null;
    }


    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonView(JsonViews.UserInterface.class)
    @JsonProperty("latestValidation")
    public FeedValidationResultSummary latestValidation() {
        FeedVersion latest = retrieveLatest();
        return latest != null ? new FeedValidationResultSummary(latest) : null;
    }

    // TODO: figure out some way to indicate whether feed has been edited since last snapshot (i.e, there exist changes)
//    @JsonInclude(JsonInclude.Include.NON_NULL)
//    @JsonView(JsonViews.UserInterface.class)
//    public boolean getEditedSinceSnapshot() {
////        FeedTx tx;
////        try {
////            tx = VersionedDataStore.getFeedTx(id);
////        } catch (Exception e) {
////
////        }
////        return tx.editedSinceSnapshot.retrieveById();
//        return false;
//    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonView(JsonViews.UserInterface.class)
    @JsonProperty("externalProperties")
    public Map<String, Map<String, String>> externalProperties() {

        Map<String, Map<String, String>> resourceTable = new HashMap<>();

        for(String resourceType : DataManager.feedResources.keySet()) {
            Map<String, String> propTable = new HashMap<>();

            // Get all external properties for the feed source/resource type and fill prop table.
            Persistence.externalFeedSourceProperties
                .getFiltered(and(eq("feedSourceId", this.id), eq("resourceType", resourceType)))
                .forEach(prop -> propTable.put(prop.name, prop.value));

            resourceTable.put(resourceType, propTable);
        }
        return resourceTable;
    }

    /**
     * Get all of the feed versions for this source
     * @return collection of feed versions
     */
    @JsonIgnore
    public Collection<FeedVersion> retrieveFeedVersions() {
        return Persistence.feedVersions.getFiltered(eq("feedSourceId", this.id));
    }

    /**
     * Get all of the snapshots for this source
     * @return collection of snapshots
     */
    @JsonIgnore
    public Collection<Snapshot> retrieveSnapshots() {
        return Persistence.snapshots.getFiltered(eq(Snapshot.FEED_SOURCE_REF, this.id));
    }

    /**
     * Get all of the test deployments for this feed source.
     * @return collection of deloyments
     */
    @JsonIgnore
    public Collection<Deployment> retrieveDeployments () {
        return Persistence.deployments.getFiltered(eq(Snapshot.FEED_SOURCE_REF, this.id));
    }

//    @JsonView(JsonViews.UserInterface.class)
//    @JsonProperty("feedVersionCount")
    public int feedVersionCount() {
        return retrieveFeedVersions().size();
    }

    @JsonView(JsonViews.UserInterface.class)
    @JsonProperty("noteCount")
    public int noteCount() {
        return this.noteIds != null ? this.noteIds.size() : 0;
    }

    public String toPublicKey() {
        return "public/" + getCleanName(this.name) + ".zip";
    }

    public void makePublic() {
        String sourceKey = FeedStore.s3Prefix + this.id + ".zip";
        String publicKey = toPublicKey();
        String versionId = this.latestVersionId();
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
        String publicKey = toPublicKey();
        if (FeedStore.s3Client.doesObjectExist(DataManager.feedBucket, sourceKey)) {
            LOG.info("removing feed {} from s3 public folder", this);
            FeedStore.s3Client.setObjectAcl(DataManager.feedBucket, sourceKey, CannedAccessControlList.AuthenticatedRead);
            FeedStore.s3Client.deleteObject(DataManager.feedBucket, publicKey);
        }
    }

    // TODO don't number the versions just timestamp them
    // FIXME for a brief moment feed version numbers are incoherent. Do this in a single operation or eliminate feed version numbers.
    public void renumberFeedVersions() {
        int i = 1;
        FindIterable<FeedVersion> orderedFeedVersions = Persistence.feedVersions.getMongoCollection()
                .find(eq("feedSourceId", this.id))
                .sort(Sorts.ascending("updated"));
        for (FeedVersion feedVersion : orderedFeedVersions) {
            // Yes it's ugly to pass in a string, but we need to change the parameter type of update to take a Document.
            Persistence.feedVersions.update(feedVersion.id, String.format("{version:%d}", i));
            i += 1;
        }
    }

    // TODO don't number the snapshots just timestamp them
    // FIXME for a brief moment snapshot numbers are incoherent. Do this in a single operation or eliminate snapshot version numbers.
    public void renumberSnapshots() {
        int i = 1;
        FindIterable<Snapshot> orderedSnapshots = Persistence.snapshots.getMongoCollection()
                .find(eq(Snapshot.FEED_SOURCE_REF, this.id))
                .sort(Sorts.ascending("snapshotTime"));
        for (Snapshot snapshot : orderedSnapshots) {
            // Yes it's ugly to pass in a string, but we need to change the parameter type of update to take a Document.
            Persistence.snapshots.updateField(snapshot.id, "version", i);
            i += 1;
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

    /**
     * Delete this feed source and everything that it contains.
     *
     * FIXME: Use a Mongo transaction to handle the deletion of these related objects.
     */
    public void delete() {
        try {
            // Remove all feed version records for this feed source
            retrieveFeedVersions().forEach(FeedVersion::delete);
            // Remove all snapshot records for this feed source
            retrieveSnapshots().forEach(Snapshot::delete);
            // Delete active editor buffer if exists.
            if (this.editorNamespace != null) {
                GTFS.delete(this.editorNamespace, DataManager.GTFS_DATA_SOURCE);
            }
            // Delete latest copy of feed source on S3.
            if (DataManager.useS3) {
                DeleteObjectsRequest delete = new DeleteObjectsRequest(DataManager.feedBucket);
                delete.withKeys("public/" + this.name + ".zip", FeedStore.s3Prefix + this.id + ".zip");
                FeedStore.s3Client.deleteObjects(delete);
            }
            // Remove all external properties for this feed source.
            Persistence.externalFeedSourceProperties.removeFiltered(eq("feedSourceId", this.id));

            // FIXME: Should this delete related feed versions from the SQL database (for both published versions and
            // editor snapshots)?

            // Finally, delete the feed source mongo document.
            Persistence.feedSources.removeById(this.id);
        } catch (Exception e) {
            LOG.error("Could not delete feed source", e);
        }
    }

    public FeedSource clone () throws CloneNotSupportedException {
        return (FeedSource) super.clone();
    }
}
