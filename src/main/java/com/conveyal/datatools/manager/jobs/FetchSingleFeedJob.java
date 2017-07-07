package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import spark.HaltException;

import java.util.Map;

public class FetchSingleFeedJob extends MonitorableJob {

    private FeedSource feedSource;
    public FeedVersion result;
    private Status status;

    public FetchSingleFeedJob (FeedSource feedSource, String owner) {
        super(owner, "Fetching feed for " + feedSource.name, JobType.FETCH_SINGLE_FEED);
        this.feedSource = feedSource;
        this.result = null;
        this.status = new Status();
        status.message = "Fetching...";
        status.percentComplete = 0.0;
        status.uploading = true;
    }

    @Override
    public void run() {
        // TODO: fetch automatically vs. manually vs. in-house
        try {
            result = feedSource.fetch(eventBus, owner);
            jobFinished();
        } catch (Exception e) {
            jobFinished();
            // throw any halts that may have prevented this job from finishing
            throw e;
        }
        if (result != null) {
            // should be run here (rather than threaded) so that the chain of jobs
            // continues in the same thread as this (original FetchSingleFeedJob instance)
            new ProcessSingleFeedJob(result, this.owner).run();
        }
    }

    @Override
    public Status getStatus() {
        synchronized (status) {
            return status.clone();
        }
    }

    @Override
    public void handleStatusEvent(Map statusMap) {
        synchronized (status) {
            status.message = (String) statusMap.get("message");
            status.percentComplete = (double) statusMap.get("percentComplete");
            status.error = (boolean) statusMap.get("error");
        }
    }
}
