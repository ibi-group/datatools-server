package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.editor.jobs.ExportSnapshotToGTFSJob;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Snapshot;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by demory on 7/27/16.
 */
public class CreateFeedVersionFromSnapshotJob extends MonitorableJob {
    public static final Logger LOG = LoggerFactory.getLogger(CreateFeedVersionFromSnapshotJob.class);

    public FeedVersion feedVersion;
    private final Snapshot snapshot;

    public CreateFeedVersionFromSnapshotJob(FeedVersion feedVersion, Snapshot snapshot, String owner) {
        super(owner, "Creating Feed Version from Snapshot for " + feedVersion.parentFeedSource().name, JobType.CREATE_FEEDVERSION_FROM_SNAPSHOT);
        this.feedVersion = feedVersion;
        this.snapshot = snapshot;
        status.message = "Initializing...";
    }

    @Override
    public void jobLogic() {
        // Set feed version properties.
        feedVersion.retrievalMethod = FeedSource.FeedRetrievalMethod.PRODUCED_IN_HOUSE;
        feedVersion.name = snapshot.name + " Snapshot Export";
        // FIXME: This should probably just create a new snapshot, and then validate those tables.
        // First export the snapshot to GTFS.
        ExportSnapshotToGTFSJob exportSnapshotToGTFSJob = new ExportSnapshotToGTFSJob(owner, snapshot, feedVersion.id);
        // Process feed version once GTFS file written.
        ProcessSingleFeedJob processSingleFeedJob = new ProcessSingleFeedJob(feedVersion, owner, true);
        addNextJob(exportSnapshotToGTFSJob, processSingleFeedJob);
        status.update("Beginning export...", 10);
    }

    @JsonProperty
    public String getFeedSourceId () {
        return snapshot.feedSourceId;
    }
}