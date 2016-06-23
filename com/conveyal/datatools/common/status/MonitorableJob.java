package com.conveyal.datatools.common.status;

import com.conveyal.datatools.manager.DataManager;
import jdk.nashorn.internal.scripts.JO;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Created by landon on 6/13/16.
 */
public abstract class MonitorableJob implements Runnable {

    protected String owner;
    protected String name;
    protected JobType type;

    public String jobId = UUID.randomUUID().toString();

    protected List<MonitorableJob> nextJobs = new ArrayList<>();

    public enum JobType { UNKNOWN_TYPE, BUILD_TRANSPORT_NETWORK, VALIDATE_FEED, PROCESS_SNAPSHOT }

    public MonitorableJob(String owner, String name, JobType type) {
        this.owner = owner;
        this.name = name;
        this.type = type;
        storeJob();
    }

    public MonitorableJob(String owner) {
        this(owner, "Unnamed Job", JobType.UNKNOWN_TYPE);
    }

    public String getName() {
        return name;
    }

    public JobType getType() {
        return type;
    }

    public abstract Status getStatus();

    protected void storeJob() {
        Set<MonitorableJob> userJobs = DataManager.userJobsMap.get(this.owner);
        if (userJobs == null) {
            userJobs = new HashSet<>();
        }
        userJobs.add(this);

        DataManager.userJobsMap.put(this.owner, userJobs);
    }

    protected void jobFinished() {
        // kick off any next jobs
        for(MonitorableJob job : nextJobs) {
            new Thread(job).start();
        }

        // remove this job from the user-job map
        Set<MonitorableJob> userJobs = DataManager.userJobsMap.get(this.owner);
        if (userJobs != null) userJobs.remove(this);
    }

    public void addNextJob(MonitorableJob job) {
        nextJobs.add(job);
    }

    /**
     * Represents the current status of this job.
     */
    public static class Status implements Cloneable {
        /** What message (defined in messages.<lang>) should be displayed to the user? */
        public String message;

        /** Is this deployment completed (successfully or unsuccessfully) */
        public boolean completed;

        /** What was the error (null if no error)? */
        public boolean error;

        /** Is the item currently being uploaded to the server? */
        public boolean uploading;

        // What is the job/task/file called
        public String name;

        /** How much of task is complete? */
        public double percentComplete;

        // When was the job initialized?
        public String initialized;

        // When was the job last modified?
        public String modified;

        // Name of file/item once completed
        public String completedName;

        public Status() {
            this.error = false;
            this.completed = false;
            this.initialized = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);
            this.modified= LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);
            this.percentComplete = 0;
        }

        public Status clone () {
            Status ret = new Status();
            ret.message = message;
            ret.completed = completed;
            ret.error = error;
            ret.uploading = uploading;
            ret.name = name;
            ret.percentComplete = percentComplete;
            ret.initialized = initialized;
            ret.modified = modified;
            ret.completedName = completedName;
            return ret;
        }
    }
}
