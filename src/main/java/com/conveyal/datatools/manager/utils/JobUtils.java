package com.conveyal.datatools.manager.utils;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.DeployJob;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.google.common.collect.Sets;
import com.mongodb.client.FindIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class JobUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JobUtils.class);

    // Heavy executor should contain long-lived CPU-intensive tasks (e.g., feed loading/validation)
    public static Executor heavyExecutor = Executors.newFixedThreadPool(4);

    // light executor is for tasks for things that should finish quickly (e.g., email notifications)
    public static Executor lightExecutor = Executors.newSingleThreadExecutor();

    /**
     * Stores jobs underway by user ID. NOTE: any set created and stored here must be created with
     * {@link Sets#newConcurrentHashSet()} or similar thread-safe Set.
     */
    public static Map<String, Set<MonitorableJob>> userJobsMap = new ConcurrentHashMap<>();

    private static final Map<String, DeployJob> deploymentJobsByServer = new HashMap<>();

    public static Set<MonitorableJob> getAllJobs() {
        return userJobsMap.values().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    /** Shorthand method for getting a single job by job ID. */
    public static MonitorableJob getJobByJobId(String jobId) {
        for (MonitorableJob job : getAllJobs()) {
            if (job.jobId.equals(jobId)) return job;
        }
        return null;
    }

    /**
     * Gets a job by user ID and job ID.
     * @param clearCompleted if true, remove requested job if it has completed or errored
     */
    public static MonitorableJob getJobById(String userId, String jobId, boolean clearCompleted) {
        // Get jobs set directly from userJobsMap because we may remove an element from it below.
        Set<MonitorableJob> userJobs = userJobsMap.get(userId);
        if (userJobs == null) {
            return null;
        }
        for (MonitorableJob job : userJobs) {
            if (job.jobId.equals(jobId)) {
                if (clearCompleted && (job.status.completed || job.status.error)) {
                    // remove job if completed or errored
                    userJobs.remove(job);
                }
                return job;
            }
        }
        // if job is not found (because it doesn't exist or was completed).
        return null;
    }

    /**
     * Convenience wrapper method to retrieve all jobs for {@link Auth0UserProfile}.
     */
    public static Set<MonitorableJob> getJobsForUser(Auth0UserProfile user) {
        if (user == null) {
            LOG.warn("Null user passed to getJobsForUser!");
            return Sets.newConcurrentHashSet();
        }
        return getJobsByUserId(user.getUser_id(), false);
    }

    /**
     * Get set of active jobs by user ID. If there are no active jobs, return a new set.
     *
     * NOTE: this should be a concurrent hash set so that it is threadsafe.
     *
     * @param clearCompleted if true, remove all completed and errored jobs for this user.
     */
    public static Set<MonitorableJob> getJobsByUserId(String userId, boolean clearCompleted) {
        Set<MonitorableJob> allJobsForUser = userJobsMap.get(userId);
        if (allJobsForUser == null) {
            return Sets.newConcurrentHashSet();
        }
        if (clearCompleted) {
            // Any staged jobs will still have their status updated, so they need to be retrieved again with any status
            // updates. All completed or errored jobs are in their final state and will not be updated any longer, so we
            // remove them once the client has seen them.
            userJobsMap.put(userId, filterStagedAndActiveJobs(allJobsForUser));
        }
        return allJobsForUser;
    }

    /**
     * Filter all jobs based on the active state. A job is only active once running. Note: this excludes "staged" jobs,
     * i.e., those which have not yet started.
     */
    public static Set<MonitorableJob> getAllActiveJobs() {
        return filterJobs(getAllJobs(), job -> job.active);
    }

    /**
     * Filter the input set of jobs with the provided filter.
     */
    private static Set<MonitorableJob> filterJobs(Set<MonitorableJob> jobs, Predicate<MonitorableJob> filter) {
        // Note: this must be a thread-safe set in case it is placed into the DataManager#userJobsMap.
        Set<MonitorableJob> filteredJobs = Sets.newConcurrentHashSet();
        jobs.stream()
            .filter(filter)
            .forEach(filteredJobs::add);
        return filteredJobs;
    }

    /**
     * Filter jobs based on status. Once a job has been created the status values are available for review. A staged job
     * may or may not be active.
     */
    public static Set<MonitorableJob> filterStagedAndActiveJobs(Set<MonitorableJob> jobs) {
        return filterJobs(jobs, job -> !job.status.completed && !job.status.error);
    }

    /**
     * Creates and queues a new {@link DeployJob} if there are no conflicting jobs assigned to the specified server.
     *
     * @param deployment The deployment to associate the new DeployJob with
     * @param owner The owner to associate the new DeployJob with
     * @param server The server to associate the new DeployJob with
     * @return returns the DeployJob if the job was successfully queued, otherwise this returns null
     */
    public static DeployJob queueDeployJob(Deployment deployment, Auth0UserProfile owner, OtpServer server) {
        // Check that we can deploy to the specified target. (Any deploy job for the target that is presently active will
        // cause a halt.)
        if (deploymentJobsByServer.containsKey(server.id)) {
            // There is a deploy job for the server. Check if it is active.
            DeployJob conflictingDeployJob = deploymentJobsByServer.get(server.id);
            if (conflictingDeployJob != null && !conflictingDeployJob.status.completed) {
                // Another deploy job is actively being deployed to the server target.
                LOG.error("New deploy job will not be queued due to active deploy job in progress.");
                return null;
            }
        }

        // For any previous deployments sent to the server/router combination, set deployedTo to null because
        // this new one will overwrite it. NOTE: deployedTo for the current deployment will only be updated after the
        // successful completion of the deploy job.
        FindIterable<Deployment> deploymentsWithSameTarget = Deployment.retrieveDeploymentForServerAndRouterId(
            server.id,
            deployment.routerId
        );
        for (Deployment oldDeployment : deploymentsWithSameTarget) {
            LOG.info("Setting deployment target to null for id={}", oldDeployment.id);
            Persistence.deployments.updateField(oldDeployment.id, "deployedTo", null);
        }
        // Finally, add deploy job to the heavy executor.
        DeployJob deployJob = new DeployJob(deployment, owner, server);
        heavyExecutor.execute(deployJob);
        deploymentJobsByServer.put(server.id, deployJob);
        return deployJob;
    }
}
