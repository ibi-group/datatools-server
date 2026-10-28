package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.CancellationContext;
import com.conveyal.datatools.common.status.FeedSourceJob;
import com.conveyal.datatools.editor.jobs.ExportSnapshotToGTFSJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Snapshot;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.JobUtils;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;

/**
 * Create (i.e., publish) a new feed version in the manager derived from an editor snapshot. NOTE: This could just
 * create a new snapshot, and then validate those tables. However, we want to verify that we're exporting correctly to
 * GTFS, so it's probably best to export the snapshot to a GTFS zip file and then run that zip file through the
 * standard processing to construct a new feed version. There may be a more clever way to handle this, but storage is
 * cheap enough.
 */
public class CreateFeedVersionFromSnapshotJob extends FeedSourceJob {
    public static final Logger LOG = LoggerFactory.getLogger(CreateFeedVersionFromSnapshotJob.class);

    private final FeedVersion feedVersion;
    private final Snapshot snapshot;
    private final boolean publishProprietaryFiles;
    private boolean started;

    public CreateFeedVersionFromSnapshotJob(FeedSource feedSource, Snapshot snapshot, Auth0UserProfile owner, boolean publishProprietaryFiles) {
        super(owner, "Creating Feed Version from Snapshot for " + feedSource.name, JobType.CREATE_FEEDVERSION_FROM_SNAPSHOT);
        this.feedVersion = new FeedVersion(feedSource, snapshot);
        this.snapshot = snapshot;
        this.publishProprietaryFiles = publishProprietaryFiles;
    }

    @Override
    public void jobLogic() {
        started = true;
        feedVersion.originNamespace = snapshot.namespace;
        status.update("Exporting snapshot to GTFS...", 10);
        // Add the jobs to handle this operation in order.
        addNextJob(
            // First export the snapshot to GTFS.
            new ExportSnapshotToGTFSJob(owner, snapshot, feedVersion, publishProprietaryFiles),
            // Then, process feed version once GTFS file written.
            new ProcessSingleFeedJob(feedVersion, owner, true)
        );
    }

    @Override
    protected void rollbackCancellation() throws Exception {
        if (!started) return;
        String namespace = feedVersion.feedLoadResult == null
            ? feedVersion.namespace
            : feedVersion.feedLoadResult.uniqueIdentifier;
        CancellationContext.rollbackResources(
            () -> FeedVersion.feedStore.deleteFeed(feedVersion.id),
            () -> Files.deleteIfExists(FeedVersion.feedStore.getFeedFile(feedVersion.id).toPath()),
            () -> Files.deleteIfExists(new File(System.getProperty("java.io.tmpdir"), feedVersion.id).toPath()),
            () -> FeedVersion.deleteDBSchema(namespace),
            () -> {
                if (namespace != null) {
                    FileUtils.deleteDirectory(new File("/tmp/datatools_gtfs", namespace));
                }
            },
            () -> {
                if (DataManager.isModuleEnabled("gtfsplus")) {
                    FeedStore store = new FeedStore(DataManager.GTFS_PLUS_SUBDIR);
                    CancellationContext.rollbackResources(
                        () -> store.deleteFeed(feedVersion.id + ".db"),
                        () -> store.deleteFeed(feedVersion.id + ".db.p"),
                        () -> Files.deleteIfExists(store.getFeedFile(feedVersion.id + ".db").toPath()),
                        () -> Files.deleteIfExists(store.getFeedFile(feedVersion.id + ".db.p").toPath())
                    );
                }
            },
            () -> {
                if (Persistence.feedVersions.getById(feedVersion.id) != null) {
                    if (!Persistence.feedVersions.removeById(feedVersion.id)) {
                        throw new IllegalStateException("Could not remove feed version " + feedVersion.id);
                    }
                }
            }
        );
    }

    /**
     * @return feed source ID for the requestor to use to re-fetch versions once job is complete
     */
    @JsonProperty
    public String getFeedSourceId () {
        return snapshot.feedSourceId;
    }
}