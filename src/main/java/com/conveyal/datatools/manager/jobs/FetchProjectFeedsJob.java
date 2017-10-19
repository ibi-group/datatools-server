package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by landon on 3/25/16.
 */
public class FetchProjectFeedsJob extends MonitorableJob {
    public static final Logger LOG = LoggerFactory.getLogger(FetchProjectFeedsJob.class);
    // FIXME store only the project ID  not the entire project object - it might change
    private Project proj;
    public Map<String, FeedVersion> result;

    public FetchProjectFeedsJob (Project proj, String owner) {
        super(owner, "Fetching feeds for " + proj.name + " project.", JobType.FETCH_PROJECT_FEEDS);
        this.proj = proj;
    }

    @Override
    public void jobLogic() {
        LOG.info("Fetch job running for {} project at {}", proj.name, ZonedDateTime.now(ZoneId.of("America/New_York")));
        result = new HashMap<>();

        for(FeedSource feedSource : proj.retrieveProjectFeedSources()) {
            // skip feed if not fetched automatically
            if (!FeedSource.FeedRetrievalMethod.FETCHED_AUTOMATICALLY.equals(feedSource.retrievalMethod)) {
                continue;
            }
            FetchSingleFeedJob fetchSingleFeedJob = new FetchSingleFeedJob(feedSource, owner);
            DataManager.heavyExecutor.execute(fetchSingleFeedJob);
        }
    }

}