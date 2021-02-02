package com.conveyal.datatools.manager.jobs;

import java.util.List;

/**
 * A mapping of the possible values of an otp-runner manifest. For further documentation please see otp-runner docs at
 * https://github.com/ibi-group/otp-runner#manifestjson-values
 */
public class OtpRunnerManifest {
    public String baseFolder;
    public List<String> baseFolderDownloads;
    public String buildConfigJSON;
    public boolean buildGraph;
    public String buildLogFile;
    public String graphObjUrl;
    public List<String> gtfsDownloads;
    public String jarFile;
    public String jarUrl;
    public String nonce;
    public String otpRunnerLogFile;
    public String otpVersion;
    public boolean prefixLogUploadsWithInstanceId;
    public String routerConfigJSON;
    public String routerName;
    public boolean runServer;
    public String s3UploadPath;
    public String serverLogFile;
    public int serverStartupTimeoutSeconds;
    public String statusFileLocation;
    public boolean uploadGraphBuildLogs;
    public boolean uploadGraphBuildReport;
    public boolean uploadGraph;
    public boolean uploadOtpRunnerLogs;
    public boolean uploadServerStartupLogs;
}
