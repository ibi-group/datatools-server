package com.conveyal.datatools.common.status;

/**
 * Created by landon on 8/5/16.
 */
public class StatusEvent {

//    MonitorableJob.Status status;
    public String message;
    public double percentComplete;
    public Boolean error;

    public StatusEvent (String message, double percentComplete, Boolean error) {
//        this.status = status;
        this.message = message;
        this.percentComplete = percentComplete;
        this.error = error;
    }
}
