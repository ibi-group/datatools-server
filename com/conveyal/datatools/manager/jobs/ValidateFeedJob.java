package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.FeedVersion;

/**
 * Created by demory on 6/16/16.
 */
public class ValidateFeedJob extends MonitorableJob {

    private FeedVersion feedVersion;
    private Status status;

    public ValidateFeedJob(FeedVersion version, String owner) {
        super(owner, "Validating Feed for " + version.getFeedSource().name, JobType.VALIDATE_FEED);
        feedVersion = version;
        status = new Status();
    }

    @Override
    public Status getStatus() {
        synchronized (status) {
            return status.clone();
        }
    }

    public String getFeedVersionId() {
        return feedVersion.id;
    }

    @Override
    public void run() {
        System.out.println("running validate job!!");
        feedVersion.validate();
        feedVersion.save();
        jobFinished();
    }
}
