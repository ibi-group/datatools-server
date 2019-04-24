package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.manager.models.Snapshot;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.mapdb.Fun;

import java.util.Date;

/**
 * A one-time token used to download a feed.
 *
 * Created by demory on 4/14/16.
 */
public class FeedDownloadToken extends Model {

    private static final long serialVersionUID = 1L;

    public String feedVersionId;
    public String snapshotId;

    public Date timestamp;

    public FeedDownloadToken () { }

    public FeedDownloadToken (FeedVersion feedVersion) {
        feedVersionId = feedVersion.id;
        timestamp = new Date();
    }

    public FeedDownloadToken (Snapshot snapshot) {
        snapshotId = snapshot.id;
        timestamp = new Date();
    }

    public FeedDownloadToken (Project project) {
        feedVersionId = project.id;
        timestamp = new Date();
    }

    @JsonProperty("feedVersion")
    public FeedVersion retrieveFeedVersion() {
        if (feedVersionId != null) return Persistence.feedVersions.getById(feedVersionId);
        else return null;
    }

    @JsonProperty("snapshot")
    public Snapshot retrieveSnapshot() {
        if (snapshotId != null) return Persistence.snapshots.getById(snapshotId);
        else return null;
    }

    // TODO: Need to update feedVersionId field name to be more generic (downloadTargetId)
    public Project retrieveProject() {
        return Persistence.projects.getById(feedVersionId);
    }

    public boolean isValid () {
        return true;
    }

}
