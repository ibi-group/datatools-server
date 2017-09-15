package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.editor.models.Snapshot;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.mapdb.Fun;

import java.util.Date;

/**
 * A one-time token used to download a feed.
 *
 * Created by demory on 4/14/16.
 */
public class FeedDownloadToken extends Model {

    private static final long serialVersionUID = 1L;

    private String feedVersionId;
    private Fun.Tuple2<String, Integer> snapshotId;

    private Date timestamp;

    public FeedDownloadToken () { }

    public FeedDownloadToken (FeedVersion feedVersion) {
        super();
        feedVersionId = feedVersion.id;
        timestamp = new Date();
    }

    public FeedDownloadToken (Snapshot snapshot) {
        super();
        snapshotId = snapshot.id;
        timestamp = new Date();
    }

    public FeedDownloadToken (Project project) {
        super();
        feedVersionId = project.id;
        timestamp = new Date();
    }

    public FeedVersion retrieveFeedVersion() {
        if (feedVersionId != null) return Persistence.feedVersions.getById(feedVersionId);
        else return null;
    }

    public Snapshot retrieveSnapshot() {
        if (snapshotId != null) return Snapshot.get(snapshotId);
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
