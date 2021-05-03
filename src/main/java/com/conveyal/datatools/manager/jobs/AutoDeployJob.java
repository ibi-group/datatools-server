package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.controllers.api.DeploymentController;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Auto deploy the latest feed versions associated with a deployment to an OTP server if these conditions are met:
 *
 * 1) {@link Project#pinnedDeploymentId} is not null.
 * 2) The project is not locked/already being deployed by another instance of {@link AutoDeployJob}.
 * 3) The deployment is not null, has feed versions and has been previously deployed.
 * 4) The deployment does not conflict with an already active deployment.
 * 5) There are no related feed versions with critical errors or feed fetches in progress.
 *
 * If there are related feed fetches in progress auto deploy is skipped but the deployment's feed versions are still
 * advanced to the latest versions.
 */
public class AutoDeployJob extends MonitorableJob {
    public static final Logger LOG = LoggerFactory.getLogger(AutoDeployJob.class);

    /**
     * Project to be considered for auto deployment.
     */
    private final Project project;

    /**
     * A set of projects which have been locked by a instance of {@link AutoDeployJob} to prevent repeat
     * deployments.
     */
    private static final Set<String> lockedProjects = Collections.synchronizedSet(new HashSet<>());

    /**
     * Auto deploy specific project.
     */
    public AutoDeployJob(Project project, Auth0UserProfile owner) {
        super(owner, "Auto Deploy Feed", JobType.AUTO_DEPLOY_FEED_VERSION);
        this.project = project;
    }

    @Override
    public void jobLogic() {
        Deployment deployment = Persistence.deployments.getById(project.pinnedDeploymentId);
        // Define if project and deployment are candidates for auto deploy.
        if (project.pinnedDeploymentId == null ||
            deployment == null ||
            deployment.feedVersionIds.isEmpty() ||
            lockedProjects.contains(project.id)) {
            LOG.info("Project {} skipped for auto deployment as required criteria not met.", project.name);
            return;
        }

        if (deployment.latest() == null) {
            String message = String.format(
                "Deployment `%s` has never been deployed. Skipping auto-deploy for project `%s`.",
                deployment.name,
                project.name
            );
            LOG.warn(message);
            NotifyUsersForSubscriptionJob.createNotification(
                "deployment-updated",
                project.id,
                message
            );
            return;
        }

        try {
            synchronized (lockedProjects) {
                if (!lockedProjects.contains(project.id)) {
                    lockedProjects.add(project.id);
                    LOG.info("Auto deploy lock added for project id: {}", project.id);
                } else {
                    LOG.warn("Unable to acquire lock for project {}", project.name);
                    return;
                }
            }
            LOG.info("Auto deploy task running for project {}", project.name);

            // Get the most recently used server.
            String latestServerId = deployment.latest().serverId;
            OtpServer server = Persistence.servers.getById(latestServerId);
            if (server == null) {
                LOG.warn(
                    "Server with id {} no longer exists. Skipping deployment for project {}.",
                    latestServerId,
                    project.name
                );
                return;
            }

            // Analyze and update feed versions in deployment.
            Collection<String> updatedFeedVersionIds = new LinkedList<>();
            List<FeedVersion> latestVersionsWithCriticalErrors = new LinkedList<>();
            List<Deployment.SummarizedFeedVersion> previousFeedVersions = deployment.retrieveFeedVersions();
            boolean shouldWaitForNewFeedVersions = false;

            // Production ready feed versions.
            List<Deployment.SummarizedFeedVersion> pinnedFeedVersions = deployment.retrievePinnedFeedVersions();
            Set<String> pinnedFeedSourceIds = new HashSet<>(
                pinnedFeedVersions
                    .stream()
                    .map(pinnedFeedVersion -> pinnedFeedVersion.feedSource.id)
                    .collect(Collectors.toList())
            );

            // Iterate through each feed version for deployment.
            for (
                Deployment.SummarizedFeedVersion currentDeploymentFeedVersion : previousFeedVersions
            ) {
                // Retrieve the latest feed version associated with the feed source of the current
                // feed version set for the deployment.
                FeedVersion latestFeedVersion = currentDeploymentFeedVersion.feedSource.retrieveLatest();
                // Make sure the latest feed version is not going to supersede a pinned feed version.
                if (pinnedFeedSourceIds.contains(latestFeedVersion.feedSourceId)) {
                    continue;
                }

                // Update to the latest feed version.
                updatedFeedVersionIds.add(latestFeedVersion.id);

                // Throttle this auto-deployment if needed. For projects that haven't yet been auto-deployed, don't
                // wait and go ahead with the auto-deployment. But if the project has been auto-deployed before and
                // if the latest feed version was created before the last time the project was auto-deployed and
                // there are currently-active jobs that could result in an updated feed version being created, then
                // this auto deployment should be throttled.
                if (
                    project.lastAutoDeploy != null &&
                        latestFeedVersion.dateCreated.before(project.lastAutoDeploy) &&
                        currentDeploymentFeedVersion.feedSource.hasJobsInProgress()
                ) {
                    // Another job exists that should result in the creation of a new feed version which should then
                    // trigger an additional AutoDeploy job.
                    LOG.warn(
                        "Feed source {} contains an active job that should result in the creation of a new feed version. Auto deployment will be skipped until that version has fully processed.",
                        currentDeploymentFeedVersion.feedSource.name
                    );
                    shouldWaitForNewFeedVersions = true;
                }

                // Make sure the latest feed version has no critical errors.
                if (latestFeedVersion.hasCriticalErrors()) {
                    latestVersionsWithCriticalErrors.add(latestFeedVersion);
                }
            }

            // Skip auto-deployment for this project if Data Tools should wait for a job that should create a new
            // feed version to complete.
            if (shouldWaitForNewFeedVersions) {
                return;
            }

            // Skip auto-deployment for this project if any of the feed versions contained critical errors.
            if (latestVersionsWithCriticalErrors.size() > 0) {
                StringBuilder errorMessageBuilder = new StringBuilder(
                    String.format("Auto deployment skipped for project %s! %s feed(s) contain critical errors:",
                        project.name,
                        latestVersionsWithCriticalErrors.size())
                );
                for (FeedVersion version : latestVersionsWithCriticalErrors) {
                    errorMessageBuilder.append(
                        String.format(
                            "%s (version %s), ",
                            version.parentFeedSource().name,
                            version.id
                        )
                    );
                }
                String message = errorMessageBuilder.toString();
                LOG.warn(message);
                NotifyUsersForSubscriptionJob.createNotification(
                    "deployment-updated",
                    project.id,
                    message
                );
                return;
            }

            // Add all pinned feed versions to the list of feed versions to be deployed so that they aren't lost as part
            // of this update.
            for (Deployment.SummarizedFeedVersion pinnedFeedVersion : pinnedFeedVersions) {
                updatedFeedVersionIds.add(pinnedFeedVersion.id);
            }

            // Check if the updated feed versions have any difference between the previous ones. If not, and if not
            // doing a regularly scheduled update with street data, then don't bother starting a deploy job.
            // TODO: add logic for street data update
            Set<String> previousFeedVersionIds = new HashSet<>(
                previousFeedVersions.stream().map(feedVersion -> feedVersion.id).collect(Collectors.toList())
            );
            if (
                !updatedFeedVersionIds.stream()
                    .anyMatch(feedVersionId -> !previousFeedVersionIds.contains(feedVersionId))
            ) {
                status.completeSuccessfully("No updated feed versions to deploy.");
                return;
            }

            // Update the deployment's feed version IDs with the latest (and pinned) feed versions.
            deployment.feedVersionIds = updatedFeedVersionIds;
            Persistence.deployments.replace(deployment.id, deployment);

            // Queue up the deploy job.
            if (DeploymentController.queueDeployJob(new DeployJob(deployment, owner, server))) {
                LOG.info("Last auto deploy date updated for project {}.", project.name);
                project.lastAutoDeploy = new Date();
                Persistence.projects.replace(project.id, project);
            } else {
                LOG.warn("Could not auto-deploy to {} due to conflicting active deployment for project {}.",
                    server.name,
                    project.name);
            }
            status.completeSuccessfully("Auto deploy complete.");
        } catch (Exception e) {
            status.fail(
                String.format("Could not auto-deploy project %s!", project.name),
                e
            );
        } finally {
            lockedProjects.remove(project.id);
            LOG.info("Auto deploy lock removed for project id: {}", project.id);
        }
    }
}
