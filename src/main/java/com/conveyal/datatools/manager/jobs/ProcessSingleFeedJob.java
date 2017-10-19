package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.editor.jobs.ProcessGtfsSnapshotMerge;
import com.conveyal.datatools.editor.models.Snapshot;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Process/validate a single GTFS feed
 * @author mattwigway
 *
 */
public class ProcessSingleFeedJob extends MonitorableJob {
    private FeedVersion feedVersion;
    private String owner;
    private static final Logger LOG = LoggerFactory.getLogger(ProcessSingleFeedJob.class);

    /**
     * Create a job for the given feed version.
     * @param feedVersion
     */
    public ProcessSingleFeedJob (FeedVersion feedVersion, String owner) {
        super(owner, "Processing new GTFS feed version", JobType.UNKNOWN_TYPE);
        this.feedVersion = feedVersion;
        this.owner = owner;
        status.update(false,  "Processing...", 0);
        status.uploading = true;
    }

    /**
     * Getter that allows a client to know the ID of the feed version that will be created as soon as the upload is
     * initiated; however, we will not store the FeedVersion in the mongo application database until the upload and
     * processing is completed. This prevents clients from manipulating GTFS data before it is entirely imported.
     */
    public String getFeedVersionId () {
        return feedVersion.id;
    }

    @Override
    public void jobLogic () {
        LOG.info("Processing feed for {}", feedVersion.id);

        // set up the validation job to run first
        ValidateFeedJob validateJob = new ValidateFeedJob(feedVersion, owner);
        addNextJob(validateJob);

        // use this FeedVersion to seed Editor DB provided no snapshots for feed already exist
        if(DataManager.isModuleEnabled("editor")) {
            // chain snapshot-creation job if no snapshots currently exist for feed
            if (Snapshot.getSnapshots(feedVersion.feedSourceId).size() == 0) {
                ProcessGtfsSnapshotMerge processGtfsSnapshotMergeJob = new ProcessGtfsSnapshotMerge(feedVersion, owner);
                addNextJob(processGtfsSnapshotMergeJob);
            }
        }

        // chain on a network builder job, if applicable
        if(DataManager.isModuleEnabled("validator")) {
            addNextJob(new BuildTransportNetworkJob(feedVersion, owner));
        }
    }

    @Override
    public void jobFinished () {
        if (!status.error) {
            // At this point all GTFS data has been loaded and validated, so we record the FeedVersion into mongo.
            Persistence.feedVersions.create(feedVersion);
            status.update(false, "New version saved.", 100, true);
        } else {
            LOG.warn("Not storing version {} because error occurred during processing.", feedVersion.id);
        }
    }

}
