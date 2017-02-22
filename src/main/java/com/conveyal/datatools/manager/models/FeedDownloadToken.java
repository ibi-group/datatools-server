package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.editor.models.Snapshot;
import com.conveyal.datatools.manager.persistence.DataStore;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.mapdb.Fun;

import java.util.Date;

/**
 * A one-time token used to download a feed.
 *
 * Created by demory on 4/14/16.
 */
public class FeedDownloadToken extends Model {

    private static final long serialVersionUID = 1L;
    private static DataStore<FeedDownloadToken> tokenStore = new DataStore<FeedDownloadToken>("feeddownloadtokens");

    private String feedVersionId;
    private Fun.Tuple2<String, Integer> snapshotId;

    private Date timestamp;

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

    public static FeedDownloadToken get (String id) {
        return tokenStore.getById(id);
    }

    @JsonIgnore
    public FeedVersion getFeedVersion () {
        if (feedVersionId != null) return FeedVersion.get(feedVersionId);
        else return null;
    }

    @JsonIgnore
    public Snapshot getSnapshot () {
        if (snapshotId != null) return Snapshot.get(snapshotId);
        else return null;
    }

    @JsonIgnore
    public Project getProject () {
        return Project.get(feedVersionId);
    }

    public boolean isValid () {
        return true;
    }

    public void save () {
        tokenStore.save(id, this);
    }

    public void delete () {
        tokenStore.delete(id);
    }
}
