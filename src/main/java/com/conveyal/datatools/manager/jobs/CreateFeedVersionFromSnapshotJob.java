package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.FeedSourceJob;
import com.conveyal.datatools.editor.jobs.ExportSnapshotToGTFSJob;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Snapshot;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public CreateFeedVersionFromSnapshotJob(FeedSource feedSource, Snapshot snapshot, Auth0UserProfile owner) {
        super(owner, "Creating Feed Version from Snapshot for " + feedSource.name, JobType.CREATE_FEEDVERSION_FROM_SNAPSHOT);
        this.feedVersion = new FeedVersion(feedSource, snapshot);
        this.snapshot = snapshot;
    }

    @Override
    public void jobLogic() {
        status.update("Exporting snapshot to GTFS...", 10);
        // Add the jobs to handle this operation in order.
        addNextJob(
            // First export the snapshot to GTFS.
            new ExportSnapshotToGTFSJob(owner, snapshot, feedVersion),
            // Then, process feed version once GTFS file written.
            new ProcessSingleFeedJob(feedVersion, owner, true)
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