package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
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
    private List<Project> projects = new ArrayList<>();

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
        projects.add(project);
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
                lockedProjects.contains(project.id)) {
                LOG.info("Project {} skipped for auto deployment as required criteria not met.", project.name);
                continue;
            }
            try {
                synchronized (lockedProjects) {
                    if (!lockedProjects.contains(project.id)) {
                        lockedProjects.add(project.id);
                    } else {
                        continue;
                    }
                }
                LOG.info("Auto deploy task running for project {}", project.name);
                if (!canAutoDeployProject(project, deployment)) {
                    continue;
                }

                List<String> feedVersionIdsForDeployment = getFeedVersionsForDeployment(project, deployment);
                if (feedVersionIdsForDeployment.isEmpty()) {
                    LOG.info("Project {} skipped for auto deployment as no newer feed versions are available!",
                        project.name);
                    continue;
                }
                if (deployment.latest() == null) {
                    String message = String.format("Deployment `%s` has never been deployed. Skipping auto-deploy for project `%s`.",
                        deployment.name,
                        project.name);
                    LOG.warn(message);
                    NotifyUsersForSubscriptionJob.createNotification(
                        "project-updated",
                        project.id,
                        message
                    );
                    continue;
                }
                // Send deployment (with new feed version) to most recently used server.
                String latestServerId = deployment.latest().serverId;
                OtpServer server = Persistence.servers.getById(latestServerId);
                if (server != null) {
                    // Advance all feed versions in the deployment.
                    deployment.feedVersionIds = new ArrayList<>();
                    deployment.feedVersionIds.addAll(feedVersionIdsForDeployment);
                    Persistence.deployments.replace(deployment.id, deployment);
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
                } else {
                    LOG.debug("Server with id {} no longer exists. Skipping deployment for project {}.",
                        latestServerId,
                        project.name);
                }
            } finally {
                lockedProjects.remove(project.id);
                LOG.debug("Auto deploy lock removed for project id: {}", project.id);
            }
        }
        status.completeSuccessfully("Auto deploy complete.");
    }

    /**
     * Retrieve feed versions from all feed sources which have no critical errors and were created after the last
     * auto deploy.
     */
    private List<String> getFeedVersionsForDeployment(Project project, Deployment deployment) {
        List<String> feedVersionIds = new ArrayList<>();
        Collection<FeedSource> feedSources = getDeploymentFeedSources(deployment);
        for (FeedSource feedSource : feedSources) {
            for (FeedVersion feedVersion : feedSource.retrieveFeedVersions()) {
                if ((project.lastAutoDeploy == null ||
                    feedVersion.dateCreated.after(project.lastAutoDeploy)) &&
                    !feedVersion.hasCriticalErrors()) {
                    // Feed version has no critical errors, advance the feed version in the deployment.
                    feedVersionIds.add(feedVersion.id);
                }
            }
        }
        return feedVersionIds;
    }

    /**
     * Get all feed sources associated with a deployments current feed versions.
     */
    private List<FeedSource> getDeploymentFeedSources(Deployment deployment) {
        List<FeedSource> feedSources = new ArrayList<>();
        for (Deployment.SummarizedFeedVersion feedVersion : deployment.retrieveFeedVersions()) {
            feedSources.add(feedVersion.feedSource);
        }
        return feedSources;
    }

    /**
     * Check each feed version associated with a deployment. If a single feed version has critical errors or the
     * deployment still has feed fetches in progress, skip auto deployment for project.
     */
    private boolean canAutoDeployProject(Project project, Deployment deployment) {
        Collection<FeedSource> feedSources = getDeploymentFeedSources(deployment);
        if (feedSources.isEmpty()) {
            String message = String.format(
                "Auto deployment skipped for project %s. Deployment %s has no associated feed sources!",
                project.name,
                deployment.name
            );
            LOG.warn(message);
            return false;
        }
        for (FeedSource feedSource : feedSources) {
            for (FeedVersion feedVersion : feedSource.retrieveFeedVersions()) {
                if ((project.lastAutoDeploy == null ||
                    feedVersion.dateCreated.after(project.lastAutoDeploy)) &&
                    feedVersion.hasCriticalErrors()) {
                    String message = String.format(
                        "Auto deployment skipped for project %s! Feed version %s in feed source %s contains critical errors!",
                        project.name,
                        feedVersion.name,
                        feedVersion.parentFeedSource().name
                    );
                    LOG.warn(message);
                    NotifyUsersForSubscriptionJob.createNotification(
                        "project-updated",
                        project.id,
                        message
                    );
                    return false;
                } else if ((project.lastAutoDeploy == null ||
                    feedVersion.dateCreated.before(project.lastAutoDeploy)) &&
                    deployment.hasFeedFetchesInProgress()) {
                    LOG.debug("Project {} contains feed version {} which has active fetches in progress. Skipping auto deployment.",
                        project.name,
                        feedVersion.name);
                    return false;
                }
            }
        }
        return true;
    }
}
