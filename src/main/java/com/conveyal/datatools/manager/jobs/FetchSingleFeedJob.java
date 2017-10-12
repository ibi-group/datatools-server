package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.Persistence;

import java.util.Map;

public class FetchSingleFeedJob extends MonitorableJob {

    private FeedSource feedSource;
    public FeedVersion result;
    private boolean continueThread;

    /**
     * Fetch a single feed source by URL
     * @param feedSource feed source to be fetched
     * @param owner user who owns job
     * @param continueThread indicates whether downstream jobs should continue in same thread
     */
    public FetchSingleFeedJob (FeedSource feedSource, String owner, boolean continueThread) {
        super(owner, "Fetching feed for " + feedSource.name, JobType.FETCH_SINGLE_FEED);
        this.feedSource = feedSource;
        this.result = null;
        this.continueThread = continueThread;
        status.message = "Fetching...";
        status.percentComplete = 0.0;
        status.uploading = true;
    }

    @Override
    public void jobLogic () {
        // TODO: fetch automatically vs. manually vs. in-house
        result = feedSource.fetch(status, owner);

        // FIXME: null result should indicate that a fetch was not needed (GTFS has not been modified)
        // True failures will throw exceptions.
        if (result != null) {
            Persistence.feedVersions.create(result);
            // in some cases (FetchProjectFeeds) job should be run here (rather than threaded)
            // so that the chain of jobs continues in the same thread as this (original
            // FetchSingleFeedJob instance)
            if (continueThread) {
                new ProcessSingleFeedJob(result, this.owner).run();
            } else {
                ProcessSingleFeedJob job = new ProcessSingleFeedJob(result, this.owner);
                DataManager.heavyExecutor.execute(job);
            }
        }
    }

}
