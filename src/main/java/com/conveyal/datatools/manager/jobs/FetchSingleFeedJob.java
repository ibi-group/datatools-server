package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.Persistence;

import java.util.Map;

public class FetchSingleFeedJob extends MonitorableJob {

    private FeedSource feedSource;
    private FeedVersion result;

    /**
     * Fetch a single feed source by URL
     * @param feedSource feed source to be fetched
     * @param owner user who owns job
     */
    public FetchSingleFeedJob (FeedSource feedSource, String owner) {
        super(owner, "Fetching feed for " + feedSource.name, JobType.FETCH_SINGLE_FEED);
        this.feedSource = feedSource;
        this.result = null;
        status.message = "Fetching...";
        status.percentComplete = 0.0;
        status.uploading = true;
    }

    @Override
    public void jobLogic () {
        // TODO: fetch automatically vs. manually vs. in-house
        result = feedSource.fetch(status, owner);

        // Null result indicates that a fetch was not needed (GTFS has not been modified)
        // True failures will throw exceptions.
        if (result != null) {
            // FetchSingleFeedJob could be run in a lightExecutor because it is a fairly quick/lightweight task.
            // However, ProcessSingleFeedJob often follows a fetch and it requires significant time to complete,
            // so FetchSingleFeedJob ought to be run in the heavyExecutor.
            ProcessSingleFeedJob processSingleFeedJob = new ProcessSingleFeedJob(result, this.owner);
            addNextJob(processSingleFeedJob);
        }
    }

}
