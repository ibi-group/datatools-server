package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.FeedVersionJob;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs the load GTFS into SQL task for a given feed version. If feed version is not new (and using S3 for storage),
 * the load step will skip uploading the feed to S3 to avoid overwriting the existing files there (this shouldn't be
 * harmful, but it is a waste of time/bandwidth and will overwrite the timestamp on the file which could cause confusion).
 */
public class LoadFeedJob extends FeedVersionJob {
    public static final Logger LOG = LoggerFactory.getLogger(LoadFeedJob.class);

    private FeedVersion feedVersion;
    private final boolean isNewVersion;

    public LoadFeedJob(FeedVersion version, Auth0UserProfile owner, boolean isNewVersion) {
        super(owner, "Loading GTFS", JobType.LOAD_FEED);
        feedVersion = version;
        this.isNewVersion = isNewVersion;
        status.update("Waiting to load feed...", 0);
    }

    /**
     * Getter that allows a client to know the ID of the feed version that will be created as soon as the upload is
     * initiated; however, we will not store the FeedVersion in the mongo application database until the upload and
     * processing is completed. This prevents clients from manipulating GTFS data before it is entirely imported.
     */
    @JsonProperty
    public String getFeedVersionId () {
        return feedVersion.id;
    }

    @JsonProperty
    public String getFeedSourceId () {
        return feedVersion.feedSourceId;
    }

    @Override
    public void jobLogic () {
        LOG.info("Running LoadFeedJob for {}", feedVersion.id);
        feedVersion.load(status, isNewVersion);
    }

    @Override
    public void jobFinished () {
        if (!status.error) {
            status.completeSuccessfully("Load stage complete!");
        }
    }

}
