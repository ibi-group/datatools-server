package com.conveyal.datatools.common.status;

import com.conveyal.datatools.manager.DataManager;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by landon on 6/13/16.
 */
public abstract class MonitorableJob implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(MonitorableJob.class);
    protected final String owner;

    // Public fields will be serialized over HTTP API and visible to the web client
    public final String name;
    public final JobType type;
    public String parentJobId;
    public JobType parentJobType;
    // Not final to allow some jobs to have extra status fields.
    public Status status = new Status();
    public final String jobId = UUID.randomUUID().toString();

    /**
     * Additional jobs that will be run after the main logic of this job has completed.
     * This job is not considered entirely completed until its sub-jobs have all completed.
     */
    protected List<MonitorableJob> subJobs = new ArrayList<>();

    public enum JobType {
        UNKNOWN_TYPE,
        BUILD_TRANSPORT_NETWORK,
        CREATE_FEEDVERSION_FROM_SNAPSHOT,
        PROCESS_SNAPSHOT,
        LOAD_FEED,
        VALIDATE_FEED,
        DEPLOY_TO_OTP,
        FETCH_PROJECT_FEEDS,
        FETCH_SINGLE_FEED,
        MAKE_PROJECT_PUBLIC,
        PROCESS_FEED,
        MERGE_PROJECT_FEEDS
    }

    public MonitorableJob(String owner, String name, JobType type) {
        this.owner = owner;
        this.name = name;
        this.type = type;
        registerJob();
    }

    public MonitorableJob(String owner) {
        this(owner, "Unnamed Job", JobType.UNKNOWN_TYPE);
    }

    /**
     * This method should never be called directly or overridden.
     * It is a standard start-up stage for all monitorable jobs.
     */
    private void registerJob() {
        ConcurrentHashSet<MonitorableJob> userJobs = DataManager.userJobsMap.get(this.owner);
        if (userJobs == null) {
            userJobs = new ConcurrentHashSet<>();
        }
        userJobs.add(this);

        DataManager.userJobsMap.put(this.owner, userJobs);
    }

    /**
     * This method should never be called directly or overridden. It is a standard clean up stage for all
     * monitorable jobs.
     */
    private void unRegisterJob () {
        // remove this job from the user-job map
        ConcurrentHashSet<MonitorableJob> userJobs = DataManager.userJobsMap.get(this.owner);
        if (userJobs != null) userJobs.remove(this);
    }

    /**
     * This method must be overridden by subclasses to perform the core steps of the job.
     */
    public abstract void jobLogic();

    /**
     * This method may be overridden in the event that you want to perform a special final step after this job and
     * all sub-jobs have completed.
     */
    public void jobFinished () {
        // do nothing by default.
    }

    /**
     * This implements Runnable.  All monitorable jobs should use this exact sequence of steps. Don't override this method;
     * override jobLogic and jobFinished method(s).
     */
    public void run () {
        boolean parentJobErrored = false;
        boolean subTaskErrored = false;
        String cancelMessage = "";
        long startTimeNanos = System.nanoTime();
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
                if (!parentJobErrored && !subTaskErrored) {
                    // Run sub-task if no error has errored during parent job or previous sub-task execution.
                    // FIXME this will overwrite a message if message is set somewhere else.
                    // FIXME If a subtask fails, cancel the parent task and cancel or remove subsequent sub-tasks.
//                status.message = String.format("Finished %d/%d sub-tasks", subJobNumber, subJobsTotal);
                    status.percentComplete = subJobNumber * 100D / subJobsTotal;
                    status.error = false; // FIXME: remove this error=false assignment
                    subJob.run();

                    // Record if there has been an error in the execution of the sub-task. (Note: this will not
                    // incorrectly overwrite a 'true' value with 'false' because the sub-task is only run if
                    // jobHasErrored is false.
                    if (subJob.status.error) {
                        subTaskErrored = true;
                        cancelMessage = String.format("Task cancelled due to error in %s task", subJob.getClass().getSimpleName());
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

            // Run final steps of job pending completion or error. Note: any tasks that depend on job success should
            // check job status to determine if final step should be executed (e.g., storing feed version in MongoDB).
            // TODO: should we add separate hooks depending on state of job/sub-tasks (e.g., success, catch, finally)
            jobFinished();

            status.completed = true;

            // We retain finished or errored jobs on the server until they are fetched via the API, which implies they
            // could be displayed by the client.
        } catch (Exception ex) {
            // Set job status to failed
            // Note that when an exception occurs during job execution we do not call unRegisterJob,
            // so the job continues to exist in the failed state and the user can see it.
            status.update(true, ex.getMessage(), 100, true);
        }
        status.startTime = TimeUnit.NANOSECONDS.toMillis(startTimeNanos);
        status.duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNanos);
        LOG.info("{} {} {} in {} ms", type, jobId, status.error ? "errored" : "completed", status.duration);
    }

    /**
     * An alternative method to run(), this method updates job status with error and should contain any other
     * clean up steps needed to complete job in an errored state (generally due to failure in a previous task in
     * the chain).
     */
    private void cancel(String message) {
        // Updating the job status with error is all we need to do in order to move the job into completion. Once the
        // user fetches the errored job, it will be automatically removed from the system.
        status.update(true, message, 100);
        status.completed = true;
        // FIXME: Do we need to run any clean up here?
    }

    /**
     * Enqueues a sub-job to be run when the main logic of this job has finished.
     */
    public void addNextJob(MonitorableJob job) {
        job.parentJobId = this.jobId;
        job.parentJobType = this.type;
        subJobs.add(job);
    }

    /**
     * Represents the current status of this job.
     */
    public static class Status implements Cloneable {

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

        public long startTime;
        public long duration;

        // When was the job initialized?
        public String initialized = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);

        // When was the job last modified?
        public String modified = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);

        // Name of file/item once completed
        public String completedName;

        public void update (boolean isError, String message, double percentComplete) {
            this.error = isError;
            this.message = message;
            this.percentComplete = percentComplete;
        }

        public void update (boolean isError, String message, double percentComplete, boolean isComplete) {
            this.error = isError;
            this.message = message;
            this.percentComplete = percentComplete;
            this.completed = isComplete;
        }

    }
}
