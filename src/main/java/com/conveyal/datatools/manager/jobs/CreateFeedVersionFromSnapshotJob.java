package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.editor.controllers.api.SnapshotController;
import com.conveyal.datatools.editor.models.Snapshot;
import com.conveyal.datatools.manager.models.FeedVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.Map;

import static spark.Spark.halt;

/**
 * Created by demory on 7/27/16.
 */
public class CreateFeedVersionFromSnapshotJob  extends MonitorableJob {
    public static final Logger LOG = LoggerFactory.getLogger(CreateFeedVersionFromSnapshotJob.class);

    public FeedVersion feedVersion;
    private String snapshotId;
    private Status status;

    public CreateFeedVersionFromSnapshotJob (FeedVersion feedVersion, String snapshotId, String owner) {
        super(owner, "Creating Feed Version from Snapshot for " + feedVersion.feedSource().name, JobType.CREATE_FEEDVERSION_FROM_SNAPSHOT);
        this.feedVersion = feedVersion;
        this.snapshotId = snapshotId;
        this.status = new Status();
        status.message = "Initializing...";
    }

    @Override
    public void run() {
        File file = null;

        try {
            file = File.createTempFile("snapshot", ".zip");
            SnapshotController.writeSnapshotAsGtfs(snapshotId, file);
        } catch (Exception e) {
            e.printStackTrace();
            String message = "Unable to create temp file for snapshot";
            LOG.error(message);
            synchronized (status) {
                status.error = true;
                status.message = message;
                status.completed = true;
            }
        }

        try {
            feedVersion.newGtfsFile(new FileInputStream(file));
        } catch (Exception e) {
            LOG.error("Unable to open input stream from upload");
            String message = "Unable to read uploaded feed";
            synchronized (status) {
                status.error = true;
                status.message = message;
                status.completed = true;
            }
        }

        feedVersion.setName(Snapshot.get(snapshotId).name + " Snapshot Export");
        feedVersion.hash();
        feedVersion.save();
        synchronized (status) {
            status.message = "Version created successfully.";
            status.completed = true;
            status.percentComplete = 100.0;
        }
        jobFinished();
    }

    @Override
    public Status getStatus() {
        synchronized (status) {
            return status.clone();
        }
    }

    @Override
    public void handleStatusEvent(Map statusMap) {
        synchronized (status) {
            status.message = (String) statusMap.get("message");
            status.percentComplete = (double) statusMap.get("percentComplete");
            status.error = (boolean) statusMap.get("error");
        }
    }
}
