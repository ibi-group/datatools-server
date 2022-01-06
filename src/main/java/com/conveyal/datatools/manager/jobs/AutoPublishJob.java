package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.controllers.api.FeedVersionController;
import com.conveyal.datatools.manager.gtfsplus.GtfsPlusValidation;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Auto publish the latest feed versions of a feed source if:
 * - there are no blocking validation errors, and.
 * - the feed source is not locked/already being published by another instance of {@link AutoPublishJob}.
 * This class assumes that feed attributes such as autoPublish and retrievalMethod
 * have been checked.
 */
public class AutoPublishJob extends MonitorableJobWithResourceLock<FeedSource> {
    public static final Logger LOG = LoggerFactory.getLogger(AutoPublishJob.class);

    /**
     * Auto-publish latest feed from specific feed source.
     */
    public AutoPublishJob(FeedSource feedSource, Auth0UserProfile owner) {
        super(
            owner,
            "Auto-Publish Feed",
            JobType.AUTO_PUBLISH_FEED_VERSION,
            feedSource,
            feedSource.name
        );
    }

    @Override
    protected void innerJobLogic() throws Exception {
        FeedSource feedSource = super.resource;
        LOG.info("Auto-publish task running for feed source {}", feedSource.name);

        // Retrieve the latest feed version associated with the feed source of the current
        // feed version set for the deployment.
        FeedVersion latestFeedVersion = feedSource.retrieveLatest();

        // Validate and check for blocking issues in the feed version to deploy.
        if (latestFeedVersion.hasBlockingIssuesForPublishing()) {
            status.fail("Could not publish this feed version because it contains blocking errors.");
        } else {
            try {
                GtfsPlusValidation gtfsPlusValidation = GtfsPlusValidation.validate(latestFeedVersion.id);
                if (!gtfsPlusValidation.issues.isEmpty()) {
                    status.fail("Could not publish this feed version because it contains GTFS+ blocking errors.");
                }
            } catch(Exception e) {
                status.fail("Could not read GTFS+ zip file", e);
            }
        }

        // If validation successful, just execute the feed updating process.
        if (!status.error) {
            FeedVersionController.publishToExternalResource(latestFeedVersion);
            LOG.info("Auto-published feed source {} to external resource.", feedSource.id);
        }
    }
}
