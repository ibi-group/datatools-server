package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.controllers.api.DeploymentController;
import com.conveyal.datatools.manager.models.*;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * Auto deploy new feed version to OTP server if {@link Project#autoDeploy} is enabled and other conditions are met
 * (e.g., feed version has no critical errors, active deployment is not in progress, etc.). This job must run after
 * {@Link ValidateFeedJob} as it has a dependency on the outcome of {@Link FeedVersion#hasCriticalErrors}.
 */
public class AutoDeployFeedJob extends MonitorableJob {
    public static final Logger LOG = LoggerFactory.getLogger(ValidateFeedJob.class);

    private FeedVersion feedVersion;
    private final FeedSource feedSource;

    AutoDeployFeedJob(FeedVersion version, Auth0UserProfile owner, FeedSource source) {
        super(owner, "Auto Deploy Feed", JobType.AUTO_DEPLOY_FEED_VERSION);
        feedVersion = version;
        feedSource = source;
    }

    @Override
    public void jobLogic () {
        if (!DataManager.isModuleEnabled("deployment")) {
            // Do not attempt to deploy if module is disabled.
            return;
        }
        Project project = feedSource.retrieveProject();
        if (feedSource.deployable && project.pinnedDeploymentId != null && project.autoDeploy) {
            // TODO: Get deployment, update feed version for feed source, and kick off deployment to server that
            //  deployment is currently pointed at.
            Deployment deployment = Persistence.deployments.getById(project.pinnedDeploymentId);
            if (deployment == null) {
                LOG.error("Pinned deployment no longer exists.");
                // FIXME: Remove pinned deployment?
            } else {
                // If the deployment is already in progress, do not attempt auto-deploy.
                // FIXME: Should this be placed in the DeploymentController#queueDeployJob body? Perhaps, since things
                //  could get jumbled when updating the deployment object in MongoDB if we're trying to simultaneously
                //  deploy to two servers at once.
                DeployJob activeJob = DeploymentController.checkDeploymentInProgress(deployment.id);
                if (activeJob != null) {
                    LOG.warn("Skipping deployment (deployment by {} in progress already)", activeJob.retrieveEmail());
                    return;
                }
                // As long as the feed does not have any critical errors, move forward with auto-deployment.
                if (!feedVersion.hasCriticalErrors()) {
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
                            LOG.error("Server with id {} no longer exists. Skipping deployment.", latestServerId);
                            return;
                        }
                    } else {
                        // FIXME: Should we deploy some other server if deployment has not previously been deployed?
                        LOG.error("Deployment {} has never been deployed. Skipping auto-deploy.", deployment.id);
                        return;
                    }
                    // Finally, queue up the new deploy job.
                    // FIXME: Assumption being that if this returns true the feed version has been deployed.
                    feedVersion.autoDeployed = DeploymentController.queueDeployJob(new DeployJob(deployment, owner, server));
                    if (feedVersion.autoDeployed) LOG.info("New deploy job initiated for {}", server.name);
                    else LOG.warn("Could not auto-deploy to {} due to conflicting active deployment.", server.name);
                }
            }
        }
    }
}
