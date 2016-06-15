package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.editor.utils.Auth0UserProfile;
import com.conveyal.datatools.manager.DataManager;
import org.apache.http.auth.AUTH;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by landon on 6/13/16.
 */
public interface MonitorableJob extends Runnable {

    Status getStatus();

    default void storeJob() {
        Set<MonitorableJob> userJobs = DataManager.userJobsMap.get(this.getStatus().owner);
        if (userJobs == null) {
            userJobs = new HashSet<>();
        }
        userJobs.add(this);

        DataManager.userJobsMap.put(this.getStatus().owner, userJobs);
    }

    /**
     * Represents the current status of this job.
     */
    public static class Status implements Cloneable {
        /** What error message (defined in messages.<lang>) should be displayed to the user? */
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

        // Who initialized the job?
        public String owner;

        // Name of file/item once completed
        public String completedName;

        public Status(String owner) {
            this.error = false;
            this.owner = owner;
            this.completed = false;
            this.initialized = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);
            this.modified= LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);
            this.percentComplete = 0.0;
        }

        public Status clone () {
            Status ret = new Status(owner);
            ret.message = message;
            ret.completed = completed;
            ret.error = error;
            ret.uploading = uploading;
            ret.name = name;
            ret.percentComplete = percentComplete;
            ret.initialized = initialized;
            ret.modified = modified;
            ret.owner = owner;
            ret.completedName = completedName;
            return ret;
        }
    }
}
