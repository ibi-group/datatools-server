package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.transform.DbTransformation;
import com.conveyal.datatools.manager.models.transform.FeedTransformDbTarget;
import com.conveyal.datatools.manager.models.transform.FeedTransformTarget;
import com.conveyal.datatools.manager.models.transform.FeedTransformZipTarget;
import com.conveyal.datatools.manager.models.transform.FeedTransformation;
import com.conveyal.datatools.manager.models.transform.ZipTransformation;

/**
 * This job will apply a {@link ZipTransformation} or {@link DbTransformation} to a GTFS zip file or database namespace,
 * respectively, and generate the required FeedTransformTarget object from those inputs, which is passed into the
 * {@link FeedTransformation#doTransform} method.
 */
public class ArbitraryTransformJob extends MonitorableJob {

    private final FeedTransformTarget target;
    private final FeedTransformation transformation;

    /**
     * Constructor to initialize a feed transform job that applies to the target version's zip GTFS file.
     */
    public ArbitraryTransformJob(Auth0UserProfile owner, FeedTransformZipTarget zipTarget, ZipTransformation transformation) {
        super(owner, "Transform " + zipTarget.gtfsFile.getAbsolutePath(), JobType.ARBITRARY_FEED_TRANSFORM);
        this.target = zipTarget;
        this.transformation = transformation;
    }

    /**
     * Constructor to initialize a feed transform job that applies to the target database namespace.
     */
    public ArbitraryTransformJob(Auth0UserProfile owner, FeedTransformDbTarget dbTarget, DbTransformation transformation) {
        super(owner, "Transform " + dbTarget.snapshotId, JobType.ARBITRARY_FEED_TRANSFORM);
        this.target = dbTarget;
        this.transformation = transformation;
    }

    @Override
    public void jobLogic() {
        // First validate the target and skip transformation if it is invalid.
        target.validate(status);
        if (status.error) return;
        // If target is valid, perform transformation.
        transformation.doTransform(target, status);
    }
}
