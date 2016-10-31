package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.editor.jobs.ProcessGtfsSnapshotMerge;
import com.conveyal.datatools.editor.models.Snapshot;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedVersion;

import java.util.Collection;


/**
 * Process/validate a single GTFS feed
 * @author mattwigway
 *
 */
public class ProcessSingleFeedJob implements Runnable {
    FeedVersion feedVersion;
    private String owner;

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

        new Thread(validateJob).start();
    }

}
