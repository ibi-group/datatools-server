package com.conveyal.datatools.common.status;

import com.conveyal.datatools.manager.auth.Auth0UserProfile;

/**
 * This class should be used for any job that operates on a FeedSource.
 */
public abstract class FeedSourceJob extends MonitorableJob {
    public FeedSourceJob(Auth0UserProfile owner, String name, JobType type) {
        super(owner, name, type);
    }

    public abstract String getFeedSourceId();
}
