package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.editor.jobs.ProcessGtfsSnapshotMerge;
import com.conveyal.datatools.editor.models.Snapshot;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process/validate a single GTFS feed
 * @author mattwigway
 *
 */
public class ProcessSingleFeedJob implements Runnable {
    FeedVersion feedVersion;
    private String owner;
    private static final Logger LOG = LoggerFactory.getLogger(ProcessSingleFeedJob.class);

    /**
     * Create a job for the given feed version.
     * @param feedVersion
     */
    public ProcessSingleFeedJob (FeedVersion feedVersion, String owner) {
        this.feedVersion = feedVersion;
        this.owner = owner;
//        this.status = new MonitorableJob.Status();
//        status.message = "Fetching...";
//        status.percentComplete = 0.0;
//        status.uploading = true;
    }

    public void run() {
        LOG.info("Processing feed for {}", feedVersion.id);

        // set up the validation job to run first
        ValidateFeedJob validateJob = new ValidateFeedJob(feedVersion, owner);

        // use this FeedVersion to seed Editor DB provided no snapshots for feed already exist
        if(DataManager.isModuleEnabled("editor")) {
            // chain snapshot-creation job if no snapshots currently exist for feed
            if (Snapshot.getSnapshots(feedVersion.feedSourceId).size() == 0) {
                ProcessGtfsSnapshotMerge processGtfsSnapshotMergeJob = new ProcessGtfsSnapshotMerge(feedVersion, owner);
                validateJob.addNextJob(processGtfsSnapshotMergeJob);
            }
        }

        // chain on a network builder job, if applicable
        if(DataManager.isModuleEnabled("validator")) {
            validateJob.addNextJob(new BuildTransportNetworkJob(feedVersion, owner));
        }

        // validate job should be run here (rather than threaded/added to thread pool)
        // so that the chain of jobs continues in the same thread
        validateJob.run();
    }

}
