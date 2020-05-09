package com.conveyal.datatools.manager.jobs;

import java.util.List;

public class OtpRunnerManifest {
    public String buildConfigJSON;
    public boolean buildGraph;
    public String buildLogFile;
    public String graphObjUrl;
    public String graphsFolder;
    public List<String> gtfsAndOsmUrls;
    public String jarFile;
    public String jarUrl;
    public String otpRunnerLogFile;
    public String routerConfigJSON;
    public String routerName;
    public boolean runServer;
    public String s3UploadBucket;
    public String serverLogFile;
    public int serverStartupTimeoutSeconds;
    public String statusFileLocation;
    public boolean uploadGraphBuildLogs;
    public boolean uploadGraphBuildReport;
    public boolean uploadGraph;
    public boolean uploadOtpRunnerLogs;
    public boolean uploadServerStartupLogs;
}
