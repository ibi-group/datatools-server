package com.conveyal.datatools.editor.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Snapshot;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.gtfs.loader.FeedLoadResult;
import com.conveyal.gtfs.loader.JdbcGtfsExporter;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class ExportSnapshotToGTFSJob extends MonitorableJob {

    private static final Logger LOG = LoggerFactory.getLogger(ExportSnapshotToGTFSJob.class);
    private final Snapshot snapshot;

    public ExportSnapshotToGTFSJob(String owner, Snapshot snapshot) {
        super(owner, "Exporting snapshot " + snapshot.name, JobType.EXPORT_SNAPSHOT_TO_GTFS);
        this.snapshot = snapshot;
    }

    @JsonProperty
    public Snapshot getSnapshot () {
        return snapshot;
    }

    @Override
    public void jobLogic() {
        File tempFile;
        try {
            tempFile = File.createTempFile("snapshot", "zip");
        } catch (IOException e) {
            e.printStackTrace();
            status.fail("Error creating local file for snapshot.", e);
            return;
        }
        JdbcGtfsExporter exporter = new JdbcGtfsExporter(snapshot.feedLoadResult.uniqueIdentifier, tempFile.getAbsolutePath(), DataManager.GTFS_DATA_SOURCE);
        FeedLoadResult result = exporter.exportTables();

        // FIXME: replace with use of refactored FeedStore.
        // Store the project merged zip locally or on s3
        if (DataManager.useS3) {
            String s3Key = "snapshots/" + snapshot.id + ".zip";
            FeedStore.s3Client.putObject(DataManager.feedBucket, s3Key, tempFile);
            LOG.info("Storing merged project feed at s3://{}/{}", DataManager.feedBucket, s3Key);
        } else {
            try {
                FeedVersion.feedStore.newFeed(snapshot.id + ".zip", new FileInputStream(tempFile), null);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                LOG.error("Could not store feed for snapshot {}", snapshot.id);
            }
        }
    }
}
