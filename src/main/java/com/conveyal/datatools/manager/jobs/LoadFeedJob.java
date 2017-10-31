package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by demory on 6/16/16.
 */
public class LoadFeedJob extends MonitorableJob {
    public static final Logger LOG = LoggerFactory.getLogger(LoadFeedJob.class);

    private FeedVersion feedVersion;

    public LoadFeedJob(FeedVersion version, String owner) {
        super(owner, "Loading GTFS", JobType.LOAD_FEED);
        feedVersion = version;
        status.update(false, "Waiting to load feed...", 0);
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

    @Override
    public void jobLogic () {
        LOG.info("Running LoadFeedJob for {}", feedVersion.id);
        feedVersion.load(status);
    }

    @Override
    public void jobFinished () {
        if (!status.error) {
            status.update(false, "Load stage complete!", 100, true);
        }
    }

}
