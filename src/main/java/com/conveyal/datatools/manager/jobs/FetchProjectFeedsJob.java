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
    private Project proj;
    public Map<String, FeedVersion> result;
    private Status status;

    public FetchProjectFeedsJob (Project proj, String owner) {
        super(owner, "Fetching feeds for " + proj.name + " project.", JobType.FETCH_PROJECT_FEEDS);
        this.proj = proj;
        this.status = new Status();
    }

    @Override
    public void run() {
        LOG.info("Fetch job running for {} project at {}", proj.name, ZonedDateTime.now(ZoneId.of("America/New_York")));
        result = new HashMap<>();

        for(FeedSource feedSource : proj.getProjectFeedSources()) {

            if (!FeedSource.FeedRetrievalMethod.FETCHED_AUTOMATICALLY.equals(feedSource.retrievalMethod))
                continue;
//            LOG.info();
            FetchSingleFeedJob fetchSingleFeedJob = new FetchSingleFeedJob(feedSource, owner);
            DataManager.heavyExecutor.execute(fetchSingleFeedJob);
        }
        jobFinished();
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