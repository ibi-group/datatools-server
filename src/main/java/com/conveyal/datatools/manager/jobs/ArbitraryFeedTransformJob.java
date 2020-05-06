package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedTransformation;
import com.conveyal.datatools.manager.models.FeedVersion;

public class ArbitraryFeedTransformJob extends MonitorableJob {

    private final FeedVersion target;
    private final FeedTransformation transformation;

    public ArbitraryFeedTransformJob(Auth0UserProfile owner, FeedVersion target, FeedTransformation transformation) {
        super(owner, "Transform " + target.name, JobType.ARBITRARY_FEED_TRANSFORM);
        this.target = target;
        this.transformation = transformation;
    }

    @Override
    public void jobLogic() {
        transformation.transform(target, status);
    }
}
