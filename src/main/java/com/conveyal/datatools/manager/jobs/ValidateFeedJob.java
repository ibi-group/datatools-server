package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.FeedVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by demory on 6/16/16.
 */
public class ValidateFeedJob extends MonitorableJob {
    public static final Logger LOG = LoggerFactory.getLogger(ValidateFeedJob.class);

    public FeedVersion feedVersion;

    public ValidateFeedJob(FeedVersion version, String owner) {
        super(owner, "Validating Feed for " + version.parentFeedSource().name, JobType.VALIDATE_FEED);
        feedVersion = version;
        status.update(false, "Waiting to begin validation...", 0);
    }

    @Override
    public void jobLogic () {
        LOG.info("Running ValidateFeedJob for {}", feedVersion.id);
        feedVersion.validate(status);
    }

    @Override
    public void jobFinished () {
        if (!status.error) {
            status.update(false, "Validation finished!", 100, true);
        }
    }

}
