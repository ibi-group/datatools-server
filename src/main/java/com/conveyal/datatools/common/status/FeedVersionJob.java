package com.conveyal.datatools.common.status;

import com.conveyal.datatools.manager.auth.Auth0UserProfile;

/**
 * This class should be used for any job that operates on a FeedVersion.
 */
public abstract class FeedVersionJob extends FeedSourceJob {
    public FeedVersionJob(Auth0UserProfile owner, String name, JobType type) {
        super(owner, name, type);
    }

    public abstract String getFeedVersionId();
}
