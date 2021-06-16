package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.FeedVersionJob;
import com.conveyal.datatools.common.utils.Scheduler;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.JobUtils;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FetchSingleFeedJob extends FeedVersionJob {
    public static final Logger LOG = LoggerFactory.getLogger(FetchSingleFeedJob.class);
    private FeedSource feedSource;
    private FeedVersion result;
    private final boolean continueThread;

    /**
     * Fetch a single feed source by URL
     * @param feedSource feed source to be fetched
     * @param owner user who owns job
     */
    public FetchSingleFeedJob (FeedSource feedSource, Auth0UserProfile owner, boolean continueThread) {
        super(owner, "Fetching feed for " + feedSource.name, JobType.FETCH_SINGLE_FEED);
        this.feedSource = feedSource;
        this.result = null;
        this.continueThread = continueThread;
        status.message = "Fetching...";
        status.percentComplete = 0.0;
        status.uploading = true;
    }

    @JsonProperty
    public String getFeedSourceId() {
        return feedSource != null ? feedSource.id : null;
    }

    /**
     * Getter that allows a client to know the ID of the feed version that will be created as soon as the upload is
     * initiated; however, we will not store the FeedVersion in the mongo application database until the upload and
     * processing is completed. This prevents clients from manipulating GTFS data before it is entirely imported.
     */
    @JsonProperty
    public String getFeedVersionId () {
        // Feed version result is null unless (and until) fetch is successful.
        return result != null ? result.id : null;
    }

    @Override
    public void jobLogic () {
        String feedSourceId = getFeedSourceId();
        FeedSource feedSource = Persistence.feedSources.getById(feedSourceId);
        if (feedSource == null) {
            LOG.error("Fetch feed job failed because feed source {} does not exist in database. Clearing all jobs for feed source.", feedSourceId);
            Scheduler.removeAllFeedSourceJobs(feedSourceId, true);
            return;
        }
        // TODO: fetch automatically vs. manually vs. in-house
        result = feedSource.fetch(status);

        // Null result indicates that a fetch was not needed (GTFS has not been modified)
        // True failures will throw exceptions.
        if (result != null) {
            // FetchSingleFeedJob should typically be run in a lightExecutor because it is a fairly lightweight task.
            // ProcessSingleFeedJob often follows a fetch and requires significant time to complete,
            // so FetchSingleFeedJob ought to be run in the heavyExecutor. Technically, the "fetch" completes
            // quickly and the "processing" happens over time. So, we run the processing in a separate thread in order
            // to match this user and system expectation.
            //
            // The exception (continueThread = true) is provided for FetchProjectFeedsJob, when we want the feeds to
            // fetch and then process in sequence.
            ProcessSingleFeedJob processSingleFeedJob = new ProcessSingleFeedJob(result, this.owner, true);
            if (continueThread) {
                addNextJob(processSingleFeedJob);
            } else {
                JobUtils.heavyExecutor.execute(processSingleFeedJob);
            }
        }
    }

}
