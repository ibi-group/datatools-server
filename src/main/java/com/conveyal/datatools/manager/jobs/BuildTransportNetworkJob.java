package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.r5.transit.TransportNetwork;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by landon on 4/30/16.
 */
public class BuildTransportNetworkJob extends MonitorableJob {

    private static final Logger LOG = LoggerFactory.getLogger(BuildTransportNetworkJob.class);
    public FeedVersion feedVersion;

    public BuildTransportNetworkJob (FeedVersion feedVersion, String owner) {
        super(owner, "Building Transport Network for " + feedVersion.parentFeedSource().name, JobType.BUILD_TRANSPORT_NETWORK);
        this.feedVersion = feedVersion;
        status.message = "Waiting to begin job...";
    }

    @Override
    public void jobLogic() {
        TransportNetwork transportNetwork = null;
        LOG.info("Building transport network");
        try {
            if (feedVersion.validationResult != null && feedVersion.validationResult.fatalException == null) {
                // Build network if validation result is OK.
                feedVersion.buildTransportNetwork(status);
            }
            else {
                // If there were validation problems, don't bother building network because it will likely have a bad
                // result.
                String message = "Transport network skipped because of bad validation.";
                LOG.error(message);
                status.update(true, message, 100, true);
                return;
            }
        } catch (Exception e) {
            String message = "Transport network build failed!";
            LOG.error(message, e);
            status.update(true, message, 100, true);
            status.exceptionType = e.getMessage();
            status.exceptionDetails = ExceptionUtils.getStackTrace(e);
            return;
        }

        if (!status.error) {
            // If no error was reported in FeedVersion::buildTransportNetwork, update status.
            status.update(false, "Transport network built successfully!", 100, true);
        }
    }

}
