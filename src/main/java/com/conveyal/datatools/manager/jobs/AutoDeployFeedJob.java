package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.controllers.api.DeploymentController;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * Auto deploy new feed version to OTP server if {@link Project#autoDeploy} is enabled and other conditions are met
 * (e.g., feed version has no critical errors, active deployment is not in progress, etc.). This job must run after
 * {@link ValidateFeedJob} as it has a dependency on the outcome of {@link FeedVersion#hasCriticalErrors}.
 */
public class AutoDeployFeedJob extends MonitorableJob {
    public static final Logger LOG = LoggerFactory.getLogger(ValidateFeedJob.class);

    private final FeedVersion feedVersion;
    private final FeedSource feedSource;

    AutoDeployFeedJob(FeedVersion version, Auth0UserProfile owner, FeedSource source) {
        super(owner, "Auto Deploy Feed", JobType.AUTO_DEPLOY_FEED_VERSION);
        feedVersion = version;
        feedSource = source;
    }

    @Override
    public void jobLogic () {
        Project project = feedSource.retrieveProject();
        Deployment deployment = Persistence.deployments.getById(project.pinnedDeploymentId);
        if (DataManager.isModuleEnabled("deployment") &&
            feedSource.deployable &&
            project.pinnedDeploymentId != null &&
            project.autoDeploy &&
            deployment != null &&
            !feedVersion.hasCriticalErrors()) {

            if (deployment.feedVersionIds == null) {
                // FIXME: is it possible that no previous versions have been deployed?
                deployment.feedVersionIds = new ArrayList<>();
            }
            // Remove previously defined version for this feed source.
            for (FeedVersion versionToReplace : deployment.retrieveFullFeedVersions()) {
                if (versionToReplace.feedSourceId.equals(feedSource.id)) {
                    deployment.feedVersionIds.remove(versionToReplace.id);
                }
            }
            // Add new version ID TODO: Should we not do this if the feed source was not already applied?
            deployment.feedVersionIds.add(feedVersion.id);
            Persistence.deployments.replace(deployment.id, deployment);
            // Send deployment (with new feed version) to most recently used server.
            OtpServer server;
            if (deployment.latest() != null) {
                String latestServerId = deployment.latest().serverId;
                server = Persistence.servers.getById(latestServerId);
                if (server == null) {
                    status.fail(String.format("Server with id %s no longer exists. Skipping deployment.", latestServerId));
                    return;
                }
            } else {
                // FIXME: Should we deploy some other server if deployment has not previously been deployed?
                status.fail(String.format("Deployment %s has never been deployed. Skipping auto-deploy.", deployment.id));
                return;
            }
            // Finally, queue up the new deploy job.
            if (DeploymentController.queueDeployJob(new DeployJob(deployment, owner, server))) {
                status.completeSuccessfully(String.format("New deploy job initiated for %s", server.name));
            } else {
                status.fail(String.format("Could not auto-deploy to %s due to conflicting active deployment.", server.name));
            }
        } else {
            status.fail("Required conditions for auto deploy not met. Skipping auto-deploy");
        }
    }
}
