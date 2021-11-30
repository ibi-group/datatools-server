package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.controllers.api.FeedVersionController;
import com.conveyal.datatools.manager.gtfsplus.GtfsPlusValidation;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Auto publish the latest feed versions of a feed source if there are no blocking validation errors.
 * The following other conditions are checked:
 *
 * 1) {@link Project#pinnedDeploymentId} is not null.
 * 2) The project is not locked/already being deployed by another instance of {@link AutoPublishJob}.
 * 3) The deployment is not null, has feed versions and has been previously deployed.
 * 4) The deployment does not conflict with an already active deployment.
 * 5) There are no related feed versions with critical errors or feed fetches in progress.
 *
 * If there are related feed fetches in progress auto deploy is skipped but the deployment's feed versions are still
 * advanced to the latest versions.
 */
public class AutoPublishJob extends MonitorableJob {
    public static final Logger LOG = LoggerFactory.getLogger(AutoPublishJob.class);

    /**
     * Feed source to be considered for auto-publishing.
     */
    private final FeedSource feedSource;

    /**
     * A set of projects which have been locked by a instance of {@link AutoPublishJob} to prevent repeat
     * auto-publishing.
     */
    private static final Set<String> lockedFeedSources = Collections.synchronizedSet(new HashSet<>());

    /**
     * Auto-publish latest feed from specific feed source.
     */
    public AutoPublishJob(FeedSource feedSource, Auth0UserProfile owner) {
        super(owner, "Auto-Publish Feed", JobType.AUTO_PUBLISH_FEED_VERSION);
        this.feedSource = feedSource;
    }

    @Override
    public void jobLogic() {
        // Define if project and feed source are candidates for auto publish.
        if (
            lockedFeedSources.contains(feedSource.id)
        ) {
            String message = String.format(
                "Feed source %s skipped for auto publishing (another publishing job is in progress)",
                feedSource.name
            );
            LOG.info(message);
            status.fail(message);
            return;
        }

        try {
            synchronized (lockedFeedSources) {
                if (!lockedFeedSources.contains(feedSource.id)) {
                    lockedFeedSources.add(feedSource.id);
                    LOG.info("Auto-publish lock added for feed source id: {}", feedSource.id);
                } else {
                    LOG.warn("Unable to acquire lock for feed source {}", feedSource.name);
                    status.fail(String.format("Feed source %s is locked for auto-publishing.", feedSource.name));
                    return;
                }
            }
            LOG.info("Auto-publish task running for feed source {}", feedSource.name);

            // Retrieve the latest feed version associated with the feed source of the current
            // feed version set for the deployment.
            FeedVersion latestFeedVersion = feedSource.retrieveLatest();

            // Validate and check for blocking issues in the feed version to deploy.
            if (latestFeedVersion.hasBlockingIssuesForPublishing()) {
                status.fail("Could not publish this feed version because it contains blocking errors.");
            }

            try {
                GtfsPlusValidation gtfsPlusValidation = GtfsPlusValidation.validate(latestFeedVersion.id);
                if (!gtfsPlusValidation.issues.isEmpty()) {
                    status.fail("Could not publish this feed version because it contains GTFS+ blocking errors.");
                }
            } catch(Exception e) {
                status.fail("Could not read GTFS+ zip file", e);
            }


            // If validation successful, just execute the feed updating process.
            // FIXME: move method to another class.
            FeedVersionController.publishToExternalResource(latestFeedVersion);
        } catch (Exception e) {
            status.fail(
                String.format("Could not auto-publish feed source %s!", feedSource.name),
                e
            );
        } finally {
            lockedFeedSources.remove(feedSource.id);
            LOG.info("Auto deploy lock removed for project id: {}", feedSource.id);
        }
    }
}
