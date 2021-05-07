package com.conveyal.datatools.manager.utils;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class JobUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JobUtils.class);

    /**
     * Stores jobs underway by user ID. NOTE: any set created and stored here must be created with
     * {@link Sets#newConcurrentHashSet()} or similar thread-safe Set.
     */
    public static Map<String, Set<MonitorableJob>> userJobsMap = new ConcurrentHashMap<>();

    // Heavy executor should contain long-lived CPU-intensive tasks (e.g., feed loading/validation)
    public static Executor heavyExecutor = Executors.newFixedThreadPool(4);

    // light executor is for tasks for things that should finish quickly (e.g., email notifications)
    public static Executor lightExecutor = Executors.newSingleThreadExecutor();

    public static Set<MonitorableJob> getAllJobs() {
        return userJobsMap.values().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    /** Shorthand method for getting a single job by job ID. */
    public static MonitorableJob getJobByJobId(String jobId) {
        for (MonitorableJob job : getAllJobs()) if (job.jobId.equals(jobId)) return job;
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
            // Any active jobs will still have their status updated, so they need to be retrieved again with any status
            // updates. All completed or errored jobs are in their final state and will not be updated any longer, so we
            // remove them once the client has seen them.
            Set<MonitorableJob> jobsStillActive = filterActiveJobs(allJobsForUser);

            userJobsMap.put(userId, jobsStillActive);
        }
        return allJobsForUser;
    }

    public static Set<MonitorableJob> filterActiveJobs(Set<MonitorableJob> jobs) {
        // Note: this must be a thread-safe set in case it is placed into the DataManager#userJobsMap.
        Set<MonitorableJob> jobsStillActive = Sets.newConcurrentHashSet();
        jobs.stream().filter(job -> job.active).forEach(jobsStillActive::add);
        return jobsStillActive;
    }
}
