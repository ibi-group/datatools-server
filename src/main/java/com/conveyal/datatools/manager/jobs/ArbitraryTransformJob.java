package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.transform.DbTransformation;
import com.conveyal.datatools.manager.models.transform.FeedTransformTarget;
import com.conveyal.datatools.manager.models.transform.FeedTransformation;
import com.conveyal.datatools.manager.models.transform.ZipTransformation;

import java.io.File;

/**
 * This job will apply a {@link ZipTransformation} or {@link DbTransformation} to a GTFS zip file or database namespace,
 * respectively, and generate the required FeedTransformTarget object from those inputs, which is passed into the
 * {@link FeedTransformation#transform} method.
 */
public class ArbitraryTransformJob extends MonitorableJob {

    private final FeedTransformTarget target;
    private final FeedTransformation transformation;

    /**
     * Constructor to initialize a feed transform job that applies to the target version's zip GTFS file.
     */
    public ArbitraryTransformJob(Auth0UserProfile owner, File targetFile, ZipTransformation transformation) {
        super(owner, "Transform " + targetFile.getAbsolutePath(), JobType.ARBITRARY_FEED_TRANSFORM);
        this.target = new FeedTransformTarget(targetFile);
        this.transformation = transformation;
    }

    /**
     * Constructor to initialize a feed transform job that applies to the target database namespace.
     */
    public ArbitraryTransformJob(Auth0UserProfile owner, String targetSnapshotId, DbTransformation transformation) {
        super(owner, "Transform " + targetSnapshotId, JobType.ARBITRARY_FEED_TRANSFORM);
        this.target = new FeedTransformTarget(targetSnapshotId);
        this.transformation = transformation;
    }

    @Override
    public void jobLogic() {
        transformation.transform(target, status);
    }
}
