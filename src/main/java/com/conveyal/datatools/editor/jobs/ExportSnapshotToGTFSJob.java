package com.conveyal.datatools.editor.jobs;

import com.amazonaws.AmazonServiceException;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.common.utils.aws.CheckedAWSException;
import com.conveyal.datatools.common.utils.aws.S3Utils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Snapshot;
import com.conveyal.gtfs.loader.FeedLoadResult;
import com.conveyal.gtfs.loader.JdbcGtfsExporter;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * This job will export a database snapshot (i.e., namespace) to a GTFS file. If a feed version is supplied in the
 * constructor, it will assume that the GTFS file is intended for ingestion into Data Tools as a new feed version.
 */
public class ExportSnapshotToGTFSJob extends MonitorableJob {

    private static final Logger LOG = LoggerFactory.getLogger(ExportSnapshotToGTFSJob.class);
    private final Snapshot snapshot;
    private final FeedVersion feedVersion;
    private File tempFile;

    public ExportSnapshotToGTFSJob(Auth0UserProfile owner, Snapshot snapshot, FeedVersion feedVersion) {
        super(owner, "Exporting snapshot " + snapshot.name, JobType.EXPORT_SNAPSHOT_TO_GTFS);
        this.snapshot = snapshot;
        this.feedVersion = feedVersion;
        status.update("Starting database snapshot...", 10);
    }

    public ExportSnapshotToGTFSJob(Auth0UserProfile owner, Snapshot snapshot) {
        this(owner, snapshot, null);
    }

    @JsonProperty
    public Snapshot getSnapshot () {
        return snapshot;
    }

    @Override
    public void jobLogic() {
        // Determine if storing/publishing new feed version for snapshot. If not, all we're doing is writing the
        // snapshot to a GTFS file.
        boolean isNewVersion = feedVersion != null;
        try {
            tempFile = File.createTempFile("snapshot", "zip");
        } catch (IOException e) {
            e.printStackTrace();
            status.fail("Error creating local file for snapshot.", e);
            return;
        }
        JdbcGtfsExporter exporter = new JdbcGtfsExporter(snapshot.namespace, tempFile.getAbsolutePath(), DataManager.GTFS_DATA_SOURCE, true);
        FeedLoadResult result = exporter.exportTables();
        if (result.fatalException != null) {
            status.fail(String.format("Error (%s) encountered while exporting database tables.", result.fatalException));
            return;
        }

        // Override snapshot ID if exporting feed for use as new feed version.
        String filename = isNewVersion ? feedVersion.id : snapshot.id + ".zip";
        String bucketPrefix = isNewVersion ? "gtfs" : "snapshots";
        // FIXME: replace with use of refactored FeedStore.
        // Store the GTFS zip locally or on s3.
        status.update("Writing snapshot to GTFS file", 90);
        if (DataManager.useS3) {
            String s3Key = String.format("%s/%s", bucketPrefix, filename);
            try {
                S3Utils.getDefaultS3Client().putObject(S3Utils.DEFAULT_BUCKET, s3Key, tempFile);
            } catch (AmazonServiceException | CheckedAWSException e) {
                status.fail("Failed to upload file to S3", e);
                return;
            }
            LOG.info("Storing snapshot GTFS at {}", S3Utils.getDefaultBucketUriForKey(s3Key));
        } else {
            try {
                File gtfsFile = FeedVersion.feedStore.newFeed(filename, new FileInputStream(tempFile), null);
                if (isNewVersion) feedVersion.assignGtfsFileAttributes(gtfsFile);
            } catch (IOException e) {
                status.fail(String.format("Could not store feed for snapshot %s", snapshot.id), e);
            }
        }
    }

    @Override
    public void jobFinished () {
        if (!status.error) status.completeSuccessfully("Export complete!");
        // Delete snapshot temp file.
        if (tempFile != null) {
            LOG.info("Deleting temporary GTFS file for exported snapshot at {}", tempFile.getAbsolutePath());
            boolean deleted = tempFile.delete();
            if (!deleted) {
                LOG.warn("Temp file {} not deleted. This may contribute to storage space shortages.", tempFile.getAbsolutePath());
            }
        }
    }
}
