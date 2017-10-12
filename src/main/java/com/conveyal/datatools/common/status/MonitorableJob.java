package com.conveyal.datatools.common.status;

import com.conveyal.datatools.manager.DataManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Created by landon on 6/13/16.
 */
public abstract class MonitorableJob implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(MonitorableJob.class);
    protected final String owner;

    // Public fields will be serialized over HTTP API and visible to the web client
    public final String name;
    public final JobType type;
    // Not final to allow some jobs to have extra status fields.
    public Status status = new Status();
    public final String jobId = UUID.randomUUID().toString();

    /**
     * Additional jobs that will be run after the main logic of this job has completed.
     * This job is not considered entirely completed until its sub-jobs have all completed.
     */
    protected List<Runnable> subJobs = new ArrayList<>();

    public enum JobType {
        UNKNOWN_TYPE,
        BUILD_TRANSPORT_NETWORK,
        CREATE_FEEDVERSION_FROM_SNAPSHOT,
        PROCESS_SNAPSHOT,
        VALIDATE_FEED,
        FETCH_PROJECT_FEEDS,
        FETCH_SINGLE_FEED,
        MAKE_PROJECT_PUBLIC,
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
     * This method should never be called directly or overriden.
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
     * This method should never be called directly or overriden. It is a standard clean up stage for all
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
        try {
            // First execute the core logic of the specific MonitorableJob subclass
            jobLogic();

            // Immediately run any sub-jobs in sequence in the current thread.
            // This hogs the current thread pool thread but makes execution order predictable.
            int subJobNumber = 1;
            int subJobsTotal = subJobs.size() + 1;
            for (Runnable subJob : subJobs) {
                status.message = String.format("Sub-job %d/%d", subJobNumber, subJobsTotal);
                status.percentComplete = subJobNumber * 100D / subJobsTotal;
                status.error = false;
                subJob.run();
                subJobNumber += 1;
            }
            jobFinished();
            // TODO Mark the status in the event bus as "finished". Currently we always immediately delete the job so this is never seen.
            unRegisterJob();
        } catch (Exception ex) {
            // Set job status to failed
            // Note that when an exception occurs we do not call unRegisterJob,
            // so the job continues to exist in the failed state and the user can see it.
            // TODO somehow delete it later
        }
    }

    /**
     * Enqueues a sub-job to be run when the main logic of this job has finished.
     */
    public void addNextJob(Runnable job) {
        subJobs.add(job);
    }

    /**
     * Represents the current status of this job.
     */
    public static class Status implements Cloneable {

        /** What message (defined in messages.<lang>) should be displayed to the user? */
        public String message;

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

    }
}
