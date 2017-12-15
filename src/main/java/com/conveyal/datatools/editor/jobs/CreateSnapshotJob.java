package com.conveyal.datatools.editor.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.loader.FeedLoadResult;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.conveyal.gtfs.GTFS.makeSnapshot;

public class CreateSnapshotJob extends MonitorableJob {
    private static final Logger LOG = LoggerFactory.getLogger(CreateSnapshotJob.class);
    private final FeedVersion feedVersion;
    private final boolean isBuffer;

    public CreateSnapshotJob(FeedVersion feedVersion, String owner, boolean isBuffer) {
        super(owner, "Creating snapshot for " + feedVersion.parentFeedSource().name, JobType.CREATE_SNAPSHOT);
        this.feedVersion = feedVersion;
        this.isBuffer = isBuffer;
    }

    @JsonProperty
    public String getFeedVersionId () {
        return feedVersion.id;
    }

    @JsonProperty
    public String getFeedSourceId () {
        return feedVersion.parentFeedSource().id;
    }

    @Override
    public void jobLogic() {
        FeedLoadResult loadResult = makeSnapshot(feedVersion.namespace, DataManager.GTFS_DATA_SOURCE);
        if (isBuffer) {
            // If creating the initial snapshot for the editor buffer, set the editor namespace field to
            // the new feed's namespace.
            Persistence.feedSources.updateField(
                    feedVersion.feedSourceId,
                    "editorNamespace",
                    loadResult.uniqueIdentifier
            );
        }
    }
}
