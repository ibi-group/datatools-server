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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Auto deploy new feed version to OTP server if {@link Project#autoDeploy} is enabled and other conditions are met
 * (e.g., feed version has no critical errors, active deployment is not in progress, there are no other feed fetches in
 * progress, etc.). This job must run after {@link ValidateFeedJob} as it has a dependency on the outcome of
 * {@link FeedVersion#hasCriticalErrors}.
 */
public class AutoDeployJob extends MonitorableJob {
    public static final Logger LOG = LoggerFactory.getLogger(AutoDeployJob.class);

    /**
     * Projects to be considered for auto deployment.
     */
    private final List<Project> projects;

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
        projects = Arrays.asList(project);
    }

    /**
     * Auto deploy all projects.
     */
    public AutoDeployJob(Auth0UserProfile owner) {
        super(owner, "Auto Deploy Feed", JobType.AUTO_DEPLOY_FEED_VERSION);
        projects = Persistence.projects.getAll();
    }

    @Override
    public void jobLogic() {
        for (Project project : projects) {
            Deployment deployment = Persistence.deployments.getById(project.pinnedDeploymentId);
            // Define if project and deployment are candidates for auto deploy.
            if (!project.autoDeploy ||
                project.pinnedDeploymentId == null ||
                deployment == null ||
                deployment.feedVersionIds.isEmpty() ||
                lockedProjects.contains(project.id)) {
                LOG.info("Project {} skipped for auto deployment as required criteria not met.", project.name);
                continue;
            }
            try {
                synchronized (lockedProjects) {
                    if (!lockedProjects.contains(project.id)) {
                        lockedProjects.add(project.id);
                    } else {
                        LOG.warn("Unable to acquire lock for project {}", project.name);
                        continue;
                    }
                }
                LOG.info("Auto deploy task running for project {}", project.name);

                if (deployment.latest() == null) {
                    String message = String.format(
                        "Deployment `%s` has never been deployed. Skipping auto-deploy for project `%s`.",
                        deployment.name,
                        project.name
                    );
                    LOG.warn(message);
                    NotifyUsersForSubscriptionJob.createNotification(
                        "project-updated",
                        project.id,
                        message
                    );
                    continue;
                }

                // Get the most recently used server.
                String latestServerId = deployment.latest().serverId;
                OtpServer server = Persistence.servers.getById(latestServerId);
                if (server == null) {
                    LOG.debug(
                        "Server with id {} no longer exists. Skipping deployment for project {}.",
                        latestServerId,
                        project.name
                    );
                    continue;
                }

                // analyze and update feed versions in deployment
                Collection<String> updatedFeedVersionIds = new LinkedList<>();
                List<FeedVersion> latestVersionsWithCriticalErrors = new LinkedList<>();
                boolean shouldWaitForNewFeedVersions = false;
                
                // iterate through each feed version for deployment
                for (
                    Deployment.SummarizedFeedVersion currentDeploymentFeedVersion : deployment.retrieveFeedVersions()
                ) {
                    // retrieve and update to the latest feed version associated with the feed source of the current 
                    // feed version set for the deployment
                    FeedVersion latestFeedVersion = currentDeploymentFeedVersion.feedSource.retrieveLatest();
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
                        LOG.debug(
                            "Feed source {} contains an active job that should result in the creation of a new feed version. Auto deployment will be skipped until that version has fully processed.",
                            currentDeploymentFeedVersion.feedSource.name
                        );
                        shouldWaitForNewFeedVersions = true;
                    }

                    // make sure the latest feed version has no critical errors
                    if (latestFeedVersion.hasCriticalErrors()) {
                        latestVersionsWithCriticalErrors.add(latestFeedVersion);
                    }
                }

                // always update the deployment's feed version IDs with the latest feed versions
                deployment.feedVersionIds = updatedFeedVersionIds;
                Persistence.deployments.replace(deployment.id, deployment);

                // skip auto-deployment for this project if Data Tools should wait for a job that should create a new
                // feed version to complete
                if (shouldWaitForNewFeedVersions) {
                    continue;
                }

                // skip auto-deployment for this project if any of the feed versions contained critical errors
                if (latestVersionsWithCriticalErrors.size() > 0) {
                    StringBuilder errorMessageBuilder = new StringBuilder(
                        String.format("Auto deployment skipped for project %s!", project.name)
                    );
                    for (FeedVersion version : latestVersionsWithCriticalErrors) {
                        errorMessageBuilder.append(
                            String.format(
                                " Feed version %s in feed source %s contains critical errors!",
                                version.name,
                                version.parentFeedSource().name
                            )
                        );
                    }
                    String message = errorMessageBuilder.toString();
                    LOG.warn(message);
                    NotifyUsersForSubscriptionJob.createNotification(
                        "project-updated",
                        project.id,
                        message
                    );
                    continue;
                }

                // Queue up the deploy job.
                if (DeploymentController.queueDeployJob(new DeployJob(deployment, owner, server))) {
                    LOG.debug("Last auto deploy date updated for project {}.", project.name);
                    project.lastAutoDeploy = new Date();
                    Persistence.projects.replace(project.id, project);
                } else {
                    LOG.debug("Could not auto-deploy to {} due to conflicting active deployment for project {}.",
                        server.name,
                        project.name);
                }
            } finally {
                lockedProjects.remove(project.id);
                LOG.debug("Auto deploy lock removed for project id: {}", project.id);
            }
        }
        status.completeSuccessfully("Auto deploy complete.");
    }
}
