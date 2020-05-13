package com.conveyal.datatools.manager.jobs;

public class OtpRunnerStatus {
    public boolean error;
    public boolean graphBuilt;
    public boolean graphUploaded;
    public boolean serverStarted;
    public String message;
    public int numFilesDownloaded;
    public double pctProgress;
    public int totalFilesToDownload;
}
