package com.conveyal.datatools.manager.jobs;

/**
 * A mapping of the fields and values that otp-runner writes to a status file. See otp-runner documentation for more
 * info: https://github.com/ibi-group/otp-runner#statusjson
 */
public class OtpRunnerStatus {
    public boolean error;
    public boolean graphBuilt;
    public boolean graphUploaded;
    public boolean serverStarted;
    public String message;
    public String nonce;
    public int numFilesDownloaded;
    public double pctProgress;
    public int totalFilesToDownload;
}
