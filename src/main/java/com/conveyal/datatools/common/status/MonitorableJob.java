package com.conveyal.datatools.common.status;

import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.metrics.MetricsService;
import com.conveyal.datatools.manager.utils.JobUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;

/**
 * Created by landon on 6/13/16.
 */
public abstract class MonitorableJob implements Runnable, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MonitorableJob.class);
    private static final ThreadLocal<CancellationContext> CURRENT_CANCELLATION = new ThreadLocal<>();
    private transient volatile CancellationContext cancellationContext;
    private boolean postProcessing;
    protected final Auth0UserProfile owner;

    // Public fields will be serialized over HTTP API and visible to the web client
    public final JobType type;
    public File file;

    /**
     * Whether the job is currently running. This is needed since some jobs can be recurring jobs that won't run until
     * their scheduled time and when they finish they could run again.
     */
    public volatile boolean active = false;

    // The two fields below are public because they are used by the UI through the /jobs endpoint.
    public String parentJobId;
    public JobType parentJobType;
    // Status is not final to allow some jobs to have extra status fields.
    public Status status = new Status();
    // Name is not final in case it needs to be amended during job processing.
    public String name;
    public final String jobId = UUID.randomUUID().toString();

    /**
     * Additional jobs that will be run after the main logic of this job has completed.
     * This job is not considered entirely completed until its sub-jobs have all completed.
     */
    @JsonIgnore
    @BsonIgnore
    public List<MonitorableJob> subJobs = new ArrayList<>();

    public enum JobType {
        AUTO_DEPLOY_FEED_VERSION,
        UNKNOWN_TYPE,
        ARBITRARY_FEED_TRANSFORM,
        BUILD_TRANSPORT_NETWORK,
        CREATE_FEEDVERSION_FROM_SNAPSHOT,
        // **** Legacy snapshot jobs
        PROCESS_SNAPSHOT_MERGE,
        PROCESS_SNAPSHOT_EXPORT,
        // ****
        LOAD_FEED,
        VALIDATE_FEED,
        DEPLOY_TO_OTP,
        EXPORT_GIS,
        EXPORT_DEPLOYMENT_GIS,
        FETCH_PROJECT_FEEDS,
        FETCH_SINGLE_FEED,
        MAKE_PROJECT_PUBLIC,
        PROCESS_FEED,
        SYSTEM_JOB,
        CREATE_SNAPSHOT,
        EXPORT_SNAPSHOT_TO_GTFS,
        CONVERT_EDITOR_MAPDB_TO_SQL,
        VALIDATE_ALL_FEEDS,
        MONITOR_SERVER_STATUS,
        MERGE_FEED_VERSIONS,
        RECREATE_BUILD_IMAGE,
        UPDATE_PELIAS,
        AUTO_PUBLISH_FEED_VERSION
    }

    public MonitorableJob(Auth0UserProfile owner, String name, JobType type) {
        // Prevent the creation of a job if the user is null.
        if (owner == null) {
            throw new IllegalArgumentException("MonitorableJob must be registered with a non-null user/owner.");
        }
        this.owner = owner;
        this.name = name;
        status.name = name;
        this.type = type;
        registerJob();
    }

    public MonitorableJob(Auth0UserProfile owner) {
        this(owner, "Unnamed Job", JobType.UNKNOWN_TYPE);
    }

    /** Constructor for a usually unmonitored system job (but still something we want to conform to our model). */
    public MonitorableJob () {
        this(Auth0UserProfile.createSystemUser(), "System job", JobType.SYSTEM_JOB);
    }

    /**
     * This method should never be called directly or overridden.
     * It is a standard start-up stage for all monitorable jobs.
     */
    private void registerJob() {
        // Get all active jobs and add the latest active job. Note: Removal of job from user's set of jobs is handled
        // in the StatusController when a user requests their active jobs and the job has finished/errored.
        Set<MonitorableJob> userJobs = JobUtils.getJobsForUser(this.owner);
        userJobs.add(this);
        JobUtils.userJobsMap.put(retrieveUserId(), userJobs);
        JobUtils.removeJobsOlderThanAWeek();
    }

    @JsonProperty("owner")
    public String retrieveUserId() {
        return this.owner.getUser_id();
    }

    @JsonProperty("email")
    public String retrieveEmail() {
        return this.owner.getEmail();
    }

    @JsonIgnore @BsonIgnore
    public List<MonitorableJob> getSubJobs() {
        return subJobs;
    }

    public File retrieveFile () {
        return file;
    }

    /**
     * This method must be overridden by subclasses to perform the core steps of the job.
     */
    public abstract void jobLogic() throws Exception;

    /**
     * This method may be overridden in the event that you want to perform a special final step after this job and
     * all sub-jobs have completed.
     */
    public void jobFinished () {
        // Do nothing by default. Note: job is only removed from active jobs set only when a user requests the latest jobs
        // via the StatusController HTTP endpoint.
    }

    @JsonIgnore @BsonIgnore
    public boolean supportsCancellation() {
        return false;
    }

    public synchronized void enableCancellation() {
        if (!supportsCancellation() || cancellationContext != null) {
            throw new IllegalStateException("Job cannot be submitted for cancellation.");
        }
        cancellationContext = new CancellationContext(this);
    }

    public boolean requestCancellation() {
        CancellationContext context = cancellationContext;
        if (context == null || !context.requestCancellation()) return false;
        status.cancellationRequested = true;
        return true;
    }

    public void failSubmission(Exception failure) {
        if (cancellationContext != null) cancellationContext.stopAcceptingCancellations();
        status.fail("Job could not be submitted.", failure);
    }

    protected void checkCancellation() {
        if (cancellationContext != null) cancellationContext.checkForCancellation();
    }

    /**
     * Release only resources created by this job. Called in reverse execution order, after work has stopped.
     */
    protected void rollbackCancellation() throws Exception {
    }

    /**
     * Keep publication and notifications outside the reversible part of a job chain.
     */
    protected void afterCancellationWindow(Runnable task) {
        afterCurrentCancellationWindow(task);
    }

    /**
     * Run the given task after the current cancellation window has closed. If there is no current cancellation
     * window, the task will be run immediately. Otherwise, it will be queued to run after the current cancellation
     * window has closed.
     */
    public static void afterCurrentCancellationWindow(Runnable task) {
        CancellationContext context = CURRENT_CANCELLATION.get();
        if (context == null || !context.acceptingCancellations) {
            task.run();
        } else {
            context.afterCurrentCancellationWindowTasks.add(task);
        }
    }

    /**
     * Add a sub-job that will be run after the main logic of this job has completed. This sub-job will be run outside
     * the cancellation window, so it should not create any resources that need to be rolled back if the job is
     * canceled.
     */
    protected void addPostProcessingJob(MonitorableJob job) {
        job.postProcessing = cancellationContext != null;
        addNextJob(job);
    }

    /**
     * Update the current cancellation context for this job. If this job is the root of a cancellation context, it will
     * be set as the current context. If this job is a sub-job of a cancellation context, it will inherit the context
     * from its root job.
     */
    private boolean updateCancellationContext(CancellationContext previousContext) {
        if (cancellationContext == null && previousContext != null && previousContext.acceptingCancellations) {
            cancellationContext = previousContext;
        }
        if (cancellationContext != null) {
            CURRENT_CANCELLATION.set(cancellationContext);
            cancellationContext.startedJobs.add(this);
        }
        return cancellationContext != null && cancellationContext.rootJob == this;
    }

    /**
     * This implements Runnable.  All monitorable jobs should use this exact sequence of steps. Don't override this method;
     * override jobLogic and jobFinished method(s).
     */
    public void run () {
        CancellationContext previousContext = CURRENT_CANCELLATION.get();
        boolean isRootJob = updateCancellationContext(previousContext);
        active = true;
        boolean parentJobErrored = false;
        boolean subTaskErrored = false;
        String cancelMessage = "";
        try {
            checkCancellation();
            // First execute the core logic of the specific MonitorableJob subclass
            jobLogic();
            checkCancellation();
            if (status.error) {
                parentJobErrored = true;
                cancelMessage = String.format("Task cancelled due to error in %s task", getClass().getSimpleName());
            }
            // Immediately run any sub-jobs in sequence in the current thread.
            // This hogs the current thread pool thread but makes execution order predictable.
            int subJobNumber = 1;
            int subJobsTotal = subJobs.size() + 1;

            for (MonitorableJob subJob : subJobs) {
                checkCancellation();
                String subJobName = subJob.getClass().getSimpleName();
                if (!parentJobErrored && !subTaskErrored) {
                    // Calculate completion based on number of sub jobs remaining.
                    double percentComplete = subJobNumber * 100D / subJobsTotal;
                    // Run sub-task if no error has errored during parent job or previous sub-task execution.
                    status.update(String.format("Waiting on %s...", subJobName), percentComplete);
                    if (subJob.postProcessing) {
                        // If this is a post-processing job, we are outside the cancellation window and can commit the job.
                        cancellationContext.commitJob();
                    }
                    subJob.run();
                    checkCancellation();
                    // Record if there has been an error in the execution of the sub-task. (Note: this will not
                    // incorrectly overwrite a 'true' value with 'false' because the sub-task is only run if
                    // jobHasErrored is false.
                    if (subJob.status.error) {
                        subTaskErrored = true;
                        cancelMessage = String.format("Task cancelled due to error in %s task", subJobName);
                    }
                } else {
                    // Cancel (fail) next sub-task and continue.
                    subJob.cancel(cancelMessage);
                }
                subJobNumber += 1;
            }
            // Complete the job (as success if no errors encountered, as failure otherwise).
            if (!isRootJob) {
                if (!parentJobErrored && !subTaskErrored) {
                    status.completeSuccessfully("Job complete!");
                } else {
                    status.complete(true);
                }
            } else {
                cancellationContext.commitJob();
                CURRENT_CANCELLATION.remove();
                if (parentJobErrored || subTaskErrored) status.complete(true);
            }
            // Run final steps of job pending completion or error. Note: any tasks that depend on job success should
            // checkForCancellation job status in jobFinished to determine if final step should be executed (e.g., storing feed
            // version in MongoDB).
            // TODO: should we add separate hooks depending on state of job/sub-tasks (e.g., success, catch, finally)
            jobFinished();
            if (isRootJob && !status.error) {
                status.completeSuccessfully("Job complete!");
            }

            // We retain finished or errored jobs on the server until they are fetched via the API, which implies they
            // could be displayed by the client.
        } catch (CancellationException e) {
            if (!isRootJob) {
                // If this is a sub-job, we want to propagate the cancellation exception to the parent job so that it
                // can handle it appropriately.
                throw e;
            } else {
                handleCancellationRollback(e);
            }
        } catch (Exception e) {
            if (!hasCancellationBeenRequested(e, isRootJob)) {
                status.fail("Job failed due to unhandled exception!", e);
            }
        } finally {
            finalizeJobCancellation(previousContext, isRootJob);
            LOG.info("{} (jobId={}) {} in {} ms", type, jobId, status.error ? "errored" : "completed", status.duration);
            active = false;
            MetricsService.recordJobOutcome(type, status.error, status.duration);
        }
    }

    /**
     * Handle the rollback of a job when a cancellation has been requested.
     */
    private void handleCancellationRollback(Exception e) {
        if (cancellationContext == null) {
            status.fail("Job failed due to unhandled exception!", e);
        } else {
            cancellationContext.rollback();
        }
    }

    /**
     * Check if a cancellation has been requested for this job or any of its parent jobs.
     */
    private boolean hasCancellationBeenRequested(Exception e, boolean isRootJob) {
        // Libraries may report an error while cancellation is pending. Still perform the requested cleanup.
        boolean cancellationRequested = isRootJob
            ? cancellationContext.stopAcceptingCancellations()
            : cancellationContext != null && cancellationContext.requestCancellation;
        if (cancellationRequested) {
            LOG.warn("Job {} failed while cancellation was pending.", jobId, e);
            if (!isRootJob) {
                throw new CancellationException("Job cancellation requested.");
            }
            cancellationContext.rollback();
        }
        return cancellationRequested;
    }

    /**
     * Finalize the job cancellation by stopping the acceptance of cancellations for the root job and restoring the
     * previous cancellation context for sub-jobs.
     */
    private void finalizeJobCancellation(CancellationContext previousContext, boolean isRootJob) {
        if (isRootJob) {
            cancellationContext.stopAcceptingCancellations();
        }
        if (previousContext == null) {
            CURRENT_CANCELLATION.remove();
        } else {
            CURRENT_CANCELLATION.set(previousContext);
        }
    }

    /**
     * An alternative method to run(), this method updates job status with error and should contain any other
     * clean up steps needed to complete job in an errored state (generally due to failure in a previous task in
     * the chain).
     */
    private void cancel(String message) {
        // Updating the job status with error is all we need to do in order to move the job into completion. Once the
        // user fetches the errored job, it will be automatically removed from the system.
        status.fail(message);
        // FIXME: Do we need to run any clean up here?
    }

    /**
     * Enqueues a sub-job to be run when the main logic of this job has finished.
     */
    public void addNextJob(MonitorableJob ...jobs) {
        for (MonitorableJob job : jobs) {
            job.parentJobId = this.jobId;
            job.parentJobType = this.type;
            subJobs.add(job);
        }
    }

    /** Convenience wrapper for a {@link List} of jobs. */
    public void addNextJob(List<MonitorableJob> jobs) {
        for (MonitorableJob job : jobs) addNextJob(job);
    }

    /**
     * Represents the current status of this job.
     */
    public static class Status {
        /** What message (defined in messages.<lang>) should be displayed to the user? */
        public String message;

        /** Detailed exception method to display to user (to help with support requests) */
        public String exceptionType;
        public String exceptionDetails;

        /** Is this deployment completed (successfully or unsuccessfully) */
        public volatile boolean completed = false;

        /** What was the error (null if no error)? */
        public volatile boolean error = false;

        public volatile boolean cancellationRequested;
        public volatile boolean cancelled;

        /** Is the item currently being uploaded to the server? */
        public boolean uploading;

        // What is the job/task/file called
        public String name;

        /** How much of task is complete? */
        public double percentComplete;

        public long startTime = System.currentTimeMillis();
        public long duration;

        // When was the job initialized?
        public String initialized = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);

        // When was the job last modified?
        public String modified = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);

        // Name of file/item once completed
        public String completedName;

        /**
         * Update status message and percent complete. This method should be used while job is still in progress.
         */
        public void update (String message, double percentComplete) {
            LOG.info("Job updated `{}`: `{}`\n{}", name, message, getCallingMethodTrace());
            this.message = message;
            this.percentComplete = percentComplete;
        }

        /**
         * Gets stack trace from method calling {@link #update(String, double)} or {@link #fail(String)} for logging
         * purposes.
         */
        private String getCallingMethodTrace() {
            StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
            // Get trace from method calling update or fail. To trace this back:
            // 0. this thread
            // 1. this method
            // 2. Status#update or Status#fail
            // 3. line where update/fail is called in server job
            return stackTrace.length >= 3 ? stackTrace[3].toString() : "WARNING: Stack trace not found.";
        }

        /**
         * Shorthand method to update status object on successful job completion.
         */
        public void completeSuccessfully(String message) {
            // Do not overwrite the message (and other fields), if the job has already been completed.
            if (!this.completed) this.complete(false, message);
        }

        /**
         * Set job status to completed with error and message information.
         */
        private void complete(boolean isError, String message) {
            this.error = isError;
            // Skip message update if the job message is null or the message has already been defined.
            if (message != null) this.message = message;
            this.percentComplete = 100;
            this.completed = true;
            this.duration = System.currentTimeMillis() - this.startTime;
        }

        /**
         * Shorthand method to complete job without overriding current message.
         */
        private void complete(boolean isError) {
            complete(isError, null);
        }

        /**
         * Fail job status with message and exception.
         */
        public void fail (String message, Exception e) {
            if (e != null) {
                this.exceptionDetails = ExceptionUtils.getStackTrace(e);
                this.exceptionType = e.getMessage();
                // If exception is null, overloaded fail method was called and message already logged with trace.
                String logMessage = String.format("Job `%s` failed with message: `%s`", name, message);
                LOG.warn(logMessage, e);
            }
            this.complete(true, message);
        }

        /**
         * Fail job status with message.
         */
        public void fail (String message) {
            // Log error with stack trace from calling method in job.
            LOG.error("Job failed with message {}\n{}", message, getCallingMethodTrace());
            fail(message, null);
        }
    }
}
