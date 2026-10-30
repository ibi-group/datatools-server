package com.conveyal.datatools.common.status;

import com.conveyal.datatools.manager.utils.JobUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;

/**
 * A context for managing cancellation of jobs. This is used to ensure that cancellation requests are
 * propagated to all sub-jobs and that rollback is performed in the correct order.
 */
public class CancellationContext {
    private static final Logger LOG = LoggerFactory.getLogger(CancellationContext.class);
    final MonitorableJob rootJob;
    final List<MonitorableJob> startedJobs = new ArrayList<>();
    final List<Runnable> afterCurrentCancellationWindowTasks = new ArrayList<>();
    volatile boolean requestCancellation;
    volatile boolean acceptingCancellations = true;

    CancellationContext(MonitorableJob rootJob) {
        this.rootJob = rootJob;
    }

    /**
     * Request cancellation of the job. If the job is still accepting cancellation requests, this method will mark the
     * job as having a cancellation requested and return true. If the job is no longer accepting cancellation requests,
     * it will return false.
     */
    synchronized boolean requestCancellation() {
        if (!acceptingCancellations) {
            return false;
        }
        requestCancellation = true;
        rootJob.status.cancellationRequested = true;
        return true;
    }

    /**
     * Check if a cancellation has been requested. If a cancellation has been requested, this method will throw a
     * CancellationException. This is used to ensure that jobs can be canceled in a controlled manner, allowing for
     * proper rollback and cleanup of resources.
     */
    void checkForCancellation() {
        if (requestCancellation) {
            throw new CancellationException("Job cancellation requested.");
        }
    }

    /**
     * Close the window for accepting cancellation requests. This method should be called when the job is about to
     * complete or when it is no longer safe to accept cancellation requests. After this method is called, any further
     * cancellation requests will be ignored, and the job will proceed to completion or rollback as appropriate.
     */
    synchronized void closeCancellationWindow() {
        checkForCancellation();
        acceptingCancellations = false;
    }

    /**
     * Stop accepting cancellation requests and return whether a cancellation has been requested. This method is used to
     * finalize the cancellation state of the job, allowing it to proceed to completion or rollback as appropriate.
     * If a cancellation has been requested, the job will be rolled back; otherwise, it will proceed to completion.
     */
    synchronized boolean stopAcceptingCancellations() {
        acceptingCancellations = false;
        return requestCancellation;
    }

    /**
     * Commit the job, executing any tasks that were registered to run after this. This method should be called when
     * the job has completed successfully and is ready to finalize its state. After this method is called, any
     * registered tasks will be executed, and the list of tasks will be cleared.
     */
    void commitJob() {
        closeCancellationWindow();
        try {
            for (Runnable task : afterCurrentCancellationWindowTasks) {
                task.run();
            }
        } finally {
            afterCurrentCancellationWindowTasks.clear();
        }
    }

    /**
     * Rollback all started jobs in reverse order, marking them as canceled and recording any exceptions that occurred
     * during rollback. This method should be called when a cancellation requestCancellation has been made and the job is being
     * rolled back.
     */
    void rollback() {
        stopAcceptingCancellations();
        Exception failure = null;
        for (int i = startedJobs.size() - 1; i >= 0; i--) {
            try {
                startedJobs.get(i).rollbackCancellation();
            } catch (Exception e) {
                LOG.error("Cancellation rollback failed for job {}", startedJobs.get(i).jobId, e);
                if (failure == null) {
                    failure = e;
                } else if (failure != e) {
                    failure.addSuppressed(e);
                }
            }
        }
        Set<MonitorableJob> marked = new HashSet<>();
        markCancelled(rootJob, failure, marked);
        for (MonitorableJob job : startedJobs) {
            markCancelled(job, failure, marked);
        }
    }

    /**
     * Recursively mark the given job and its sub-jobs as canceled, recording any failure that occurred during rollback.
     * This method is used to ensure that all jobs in the hierarchy are marked as canceled and that any exceptions are
     * properly recorded.
     */
    void markCancelled(MonitorableJob job, Exception failure, Set<MonitorableJob> marked) {
        if (!marked.add(job)) return;
        for (MonitorableJob child : job.subJobs) {
            markCancelled(child, failure, marked);
        }
        job.status.cancellationRequested = true;
        job.status.cancelled = failure == null;
        job.status.fail(failure == null ? "Job cancelled." : "Cancellation rollback failed.", failure);
    }

    @FunctionalInterface
    public interface CleanupAction {
        void run() throws Exception;
    }

    /**
     * Attempt every cleanup, and report failures rather than leaving cancellation looking successful.
     */
    public static void rollbackResources(CleanupAction... actions) throws Exception {
        Exception failure = null;
        for (CleanupAction action : actions) {
            try {
                action.run();
            } catch (Exception e) {
                if (failure == null) {
                    failure = e;
                } else if (failure != e) {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

}
