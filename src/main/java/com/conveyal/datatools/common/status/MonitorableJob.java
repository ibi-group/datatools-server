package com.conveyal.datatools.common.status;

import com.conveyal.datatools.manager.auth.Auth0UserProfile;
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

/**
 * Created by landon on 6/13/16.
 */
public abstract class MonitorableJob implements Runnable, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MonitorableJob.class);
    protected final Auth0UserProfile owner;

    // Public fields will be serialized over HTTP API and visible to the web client
    public final JobType type;
    public File file;

    /**
     * Whether the job is currently running. This is needed since some jobs can be recurring jobs that won't run until
     * their scheduled time and when they finish they could run again.
     */
    public boolean active = false;

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

    /**
     * This implements Runnable.  All monitorable jobs should use this exact sequence of steps. Don't override this method;
     * override jobLogic and jobFinished method(s).
     */
    public void run () {
        active = true;
        boolean parentJobErrored = false;
        boolean subTaskErrored = false;
        String cancelMessage = "";
        try {
            // First execute the core logic of the specific MonitorableJob subclass
            jobLogic();
            if (status.error) {
                parentJobErrored = true;
                cancelMessage = String.format("Task cancelled due to error in %s task", getClass().getSimpleName());
            }
            // Immediately run any sub-jobs in sequence in the current thread.
            // This hogs the current thread pool thread but makes execution order predictable.
            int subJobNumber = 1;
            int subJobsTotal = subJobs.size() + 1;

            for (MonitorableJob subJob : subJobs) {
                String subJobName = subJob.getClass().getSimpleName();
                if (!parentJobErrored && !subTaskErrored) {
                    // Calculate completion based on number of sub jobs remaining.
                    double percentComplete = subJobNumber * 100D / subJobsTotal;
                    // Run sub-task if no error has errored during parent job or previous sub-task execution.
                    status.update(String.format("Waiting on %s...", subJobName), percentComplete);
                    subJob.run();
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
            // FIXME: should jobFinished be run if task or any sub-task fails?
            if (subTaskErrored) {
                // Cancel parent job completion if an error was encountered in task/sub-task. No need to cancel sub-task
                // because the error presumably already occurred and has a better error message.
                cancel(cancelMessage);
            }
            // Complete the job (as success if no errors encountered, as failure otherwise).
            if (!parentJobErrored && !subTaskErrored) status.completeSuccessfully("Job complete!");
            else status.complete(true);
            // Run final steps of job pending completion or error. Note: any tasks that depend on job success should
            // check job status in jobFinished to determine if final step should be executed (e.g., storing feed
            // version in MongoDB).
            // TODO: should we add separate hooks depending on state of job/sub-tasks (e.g., success, catch, finally)
            jobFinished();

            // We retain finished or errored jobs on the server until they are fetched via the API, which implies they
            // could be displayed by the client.
        } catch (Exception e) {
            status.fail("Job failed due to unhandled exception!", e);
        } finally {
            LOG.info("{} (jobId={}) {} in {} ms", type, jobId, status.error ? "errored" : "completed", status.duration);
            active = false;
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
        public boolean completed = false;

        /** What was the error (null if no error)? */
        public boolean error = false;

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
