package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.manager.persistence.DataStore;

import java.util.Date;

/**
 * A one-time token used to download a feed.
 *
 * Created by demory on 4/14/16.
 */
public class FeedDownloadToken extends Model {

    private static DataStore<FeedDownloadToken> tokenStore = new DataStore<FeedDownloadToken>("feeddownloadtokens");

    private String feedVersionId;

    private Date timestamp;

    public FeedDownloadToken (FeedVersion feedVersion) {
        super();
        feedVersionId = feedVersion.id;
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

    public FeedVersion getFeedVersion () {
        return FeedVersion.get(feedVersionId);
    }

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
