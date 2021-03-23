package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.controllers.api.DeploymentController;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Auto deploy new feed version to OTP server if {@link Project#autoDeploy} is enabled and other conditions are met
 * (e.g., feed version has no critical errors, active deployment is not in progress, there are no other feed fetches in
 * progress, etc.). This job must run after {@link ValidateFeedJob} as it has a dependency on the outcome of
 * {@link FeedVersion#hasCriticalErrors}.
 */
public class AutoDeployFeedJob extends MonitorableJob {
    public static final Logger LOG = LoggerFactory.getLogger(ValidateFeedJob.class);

    /**
     * Projects to be considered for auto deployment
     */
    private List<Project> projects = new ArrayList<>();

    /**
     * A set of projects which have been locked by a instance of {@link AutoDeployFeedJob} to prevent repeat
     * deployments.
     */
    private final Set<String> lockedProjects = Sets.newConcurrentHashSet();

    /**
     * Auto deploy specific project.
     */
    public AutoDeployFeedJob(Project project, Auth0UserProfile owner) {
        super(owner, "Auto Deploy Feed", JobType.AUTO_DEPLOY_FEED_VERSION);
        projects.add(project);
    }

    /**
     * Auto deploy all projects.
     */
    public AutoDeployFeedJob(Auth0UserProfile owner) {
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
                deployment.feedVersionIds == null ||
                lockedProjects.contains(project.id)) {
                LOG.debug("Project {} skipped for auto deployment as required criteria not met.", project.id);
                continue;
            }

            try {
                LOG.debug("Auto deploy Lock applied to project id: {}", project.id);
                lockedProjects.add(project.id);

                boolean hasFeedFetchesInProgress = false;
                boolean hasFeedVersionWithCriticalErrors = false;
                List<FeedVersion> feedVersions = deployment.retrieveFullFeedVersions();
                for (FeedVersion feedVersion : feedVersions) {
                    if ((project.lastAutoDeploy == null ||
                        feedVersion.dateCreated.after(project.lastAutoDeploy)) &&
                        feedVersion.hasCriticalErrors()) {
                        hasFeedVersionWithCriticalErrors = true;
                        FeedSource feedSource = Persistence.feedSources.getById(feedVersion.feedSourceId);
                        String message = String.format(
                            "Auto deployment skipped for project `%s`! Feed version `%s` in feed source `%s` contains critical errors!",
                            project.name,
                            feedVersion.name,
                            feedSource.name
                        );
                        LOG.warn(message);
                        NotifyUsersForSubscriptionJob.createNotification(
                            "project-updated",
                            project.id,
                            message
                        );
                        break;
                    } else if ((project.lastAutoDeploy == null ||
                        feedVersion.dateCreated.before(project.lastAutoDeploy)) &&
                        deployment.hasFeedFetchesInProgress()) {
                        hasFeedFetchesInProgress = true;
                        LOG.debug("Project {} contains feed version {} which has active fetches in progress. Skipping auto deployment.",
                            project.id,
                            feedVersion.id);
                        break;
                    }
                }

                if (!hasFeedFetchesInProgress && !hasFeedVersionWithCriticalErrors) {
                    // Send deployment (with new feed version) to most recently used server.
                    if (deployment.latest() != null) {
                        String latestServerId = deployment.latest().serverId;
                        OtpServer server = Persistence.servers.getById(latestServerId);
                        if (server != null) {
                            // If there are no other fetches in progress, queue up the deploy job.
                            if (DeploymentController.queueDeployJob(new DeployJob(deployment, owner, server))) {
                                LOG.debug("Last auto deploy date updated for project id {}.", project.id);
                                project.lastAutoDeploy = new Date();
                                Persistence.projects.replace(project.id, project);
                            } else {
                                LOG.debug("Could not auto-deploy to {} due to conflicting active deployment for project id {}.",
                                    server.name,
                                    project.id);
                            }
                        } else {
                            LOG.debug("Server with id {} no longer exists. Skipping deployment for project id {}.",
                                latestServerId,
                                project.id);
                        }
                    } else {
                        // FIXME: Should we deploy some other server if deployment has not previously been deployed?
                        LOG.debug("Deployment {} has never been deployed. Skipping auto-deploy for project id {}.",
                            deployment.id,
                            project.id);
                    }
                }
            } finally {
                lockedProjects.remove(project.id);
                LOG.debug("Auto deploy lock removed for project id: {}", project.id);
            }
        }
        status.completeSuccessfully("Auto deploy complete.");
    }
}
