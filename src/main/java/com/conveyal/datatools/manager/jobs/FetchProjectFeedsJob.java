package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedRetrievalMethod;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.JobUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collection;

/**
 * Created by landon on 3/25/16.
 */
public class FetchProjectFeedsJob extends MonitorableJob {
    public static final Logger LOG = LoggerFactory.getLogger(FetchProjectFeedsJob.class);
    public String projectId;

    public FetchProjectFeedsJob (Project project, Auth0UserProfile owner) {
        super(owner, "Fetching feeds for " + project.name + " project.", JobType.FETCH_PROJECT_FEEDS);
        this.projectId = project.id;
    }

    @Override
    public void jobLogic() {
        Project project = Persistence.projects.getById(projectId);
        if (project == null) {
            // FIXME: Since this is no longer running in a scheduled context, perhaps this should be removed?
            LOG.error("Fetch feeds job failed because project {} does not exist in database.", projectId);
            return;
        }
        LOG.info("Fetch job running for {} project at {}", project.name, ZonedDateTime.now(ZoneId.of("America/New_York")));
        Collection<FeedSource> projectFeeds = project.retrieveProjectFeedSources();
        for(FeedSource feedSource : projectFeeds) {
            // skip feed if not fetched automatically
            if (!FeedRetrievalMethod.FETCHED_AUTOMATICALLY.equals(feedSource.retrievalMethod)) {
                continue;
            }
            // warn if a feed is setup to be fetched automatically, but doesn't have a url defined
            if (feedSource.url == null) {
                LOG.warn(
                    "Feed '{}' ({}) is set to be fetched automatically, but lacks a url!",
                    feedSource.name,
                    feedSource.id
                );
                continue;
            }
            // No need to track overall status on this FetchProjectFeedsJob. All "child" jobs execute in threadpool,
            // so we don't know their status.
            FetchSingleFeedJob fetchSingleFeedJob = new FetchSingleFeedJob(feedSource, owner, true);
            // Run this in a heavy executor with continueThread = true, so that fetch/process jobs for each
            // feed source execute in order (i.e., fetch feed source A, then process; next, fetch feed source b, then
            // process).
            JobUtils.heavyExecutor.execute(fetchSingleFeedJob);
        }
    }

}