package com.conveyal.datatools.editor.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Snapshot;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.loader.FeedLoadResult;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;

import static com.conveyal.gtfs.GTFS.makeSnapshot;

public class CreateSnapshotJob extends MonitorableJob {
    private static final Logger LOG = LoggerFactory.getLogger(CreateSnapshotJob.class);
    private final FeedSource feedSource;
    private final String namespace;
    private final boolean isBuffer;
    private Snapshot snapshot;

    public CreateSnapshotJob(FeedSource feedSource, String namespace, String owner, boolean isBuffer) {
        super(owner, "Creating snapshot for " + feedSource.name, JobType.CREATE_SNAPSHOT);
        this.feedSource = feedSource;
        this.namespace = namespace;
        this.isBuffer = isBuffer;
        status.update(false,  "Creating snapshot...", 0);
    }

    @JsonProperty
    public String getFeedSourceId () {
        return feedSource.id;
    }

    @Override
    public void jobLogic() {
        Collection<Snapshot> existingSnapshots = feedSource.retrieveSnapshots();
        int version = existingSnapshots.size();
        FeedLoadResult loadResult = makeSnapshot(namespace, DataManager.GTFS_DATA_SOURCE);
        this.snapshot = new Snapshot(feedSource.id, version, loadResult);
        snapshot.name = "New snapshot " + new Date().toString();
        snapshot.snapshotTime = loadResult.completionTime;
    }

    @Override
    public void jobFinished () {
        if (!status.error) {
            LOG.info("Storing snapshot {}", snapshot.id);
            Persistence.snapshots.create(snapshot);
            if (isBuffer) {
                LOG.info("Updating active snapshot to {}", snapshot.id);
                // If creating the initial snapshot for the editor buffer, set the editor namespace field to
                // the new feed's namespace.
                Persistence.feedSources.updateField(
                        feedSource.id,
                        "editorNamespace",
                        snapshot.feedLoadResult.uniqueIdentifier
                );
                // FIXME Add original namespace as origin copy of data in list of snapshots field.
            } else {
                // Add to list of non-active snapshots.

            }
            status.update(false, "Created snapshot!", 100, true);
        }
    }
}
