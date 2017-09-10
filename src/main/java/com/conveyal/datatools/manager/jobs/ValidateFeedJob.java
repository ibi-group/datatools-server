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
    public Status status;

    public ValidateFeedJob(FeedVersion version, String owner) {
        super(owner, "Validating Feed for " + version.parentFeedSource().name, JobType.VALIDATE_FEED);
        feedVersion = version;
        status = new Status();
        status.message = "Waiting to begin validation...";
        status.percentComplete = 0;
    }

    @Override
    public Status getStatus() {
        synchronized (status) {
            return status.clone();
        }
    }

    @Override
    public void run() {
        LOG.info("Running ValidateFeedJob for {}", feedVersion.id);
        feedVersion.storeUser(owner);
        feedVersion.validate(eventBus);
        feedVersion.save();
        if (!status.error)
        synchronized (status) {
            if (!status.error) {
                status.message = "Validation complete!";
                status.percentComplete = 100;
                status.completed = true;
            }
        }
        jobFinished();
    }

    @Override
    public void handleStatusEvent(Map statusMap) {
        synchronized (status) {
            status.message = (String) statusMap.get("message");
            status.percentComplete = (double) statusMap.get("percentComplete");
            status.error = (boolean) statusMap.get("error");
        }
    }

//    public void handleGTFSValidationEvent(GTFSValidationEvent gtfsValidationEvent) {
//
//    }
}
