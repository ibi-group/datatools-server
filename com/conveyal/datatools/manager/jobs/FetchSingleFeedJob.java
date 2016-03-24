package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;

public class FetchSingleFeedJob implements Runnable {

    private FeedSource feedSource;
    public FeedVersion result;

    public FetchSingleFeedJob (FeedSource feedSource) {
        this.feedSource = feedSource;
        this.result = null;
    }

    @Override
    public void run() {
        // TODO: fetch automatically vs. manually vs. in-house
        result = feedSource.fetch();
    }

}
