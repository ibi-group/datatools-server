package com.conveyal.datatools.manager.jobs;

import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceNetworkInterfaceSpecification;
import com.amazonaws.services.ec2.model.InstanceStateChange;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancing;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.waiters.Waiter;
import com.amazonaws.waiters.WaiterParameters;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.common.utils.aws.CheckedAWSException;
import com.conveyal.datatools.common.utils.aws.EC2Utils;
import com.conveyal.datatools.common.utils.aws.EC2ValidationResult;
import com.conveyal.datatools.common.utils.aws.S3Utils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.OtpRunnerManifest.OtpRunnerBaseFolderDownload;
import com.conveyal.datatools.manager.models.CustomFile;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.EC2Info;
import com.conveyal.datatools.manager.models.EC2InstanceSummary;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.JobUtils;
import com.conveyal.datatools.manager.utils.StringUtils;
import com.conveyal.datatools.manager.utils.TimeTracker;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.binary.Base64;
import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.conveyal.datatools.manager.models.Deployment.DEFAULT_OTP_VERSION;

/**
 * Deploy the given deployment to the OTP servers specified by targets.
 * @author mattwigway
 *
 */
public class DeployJob extends MonitorableJob {

    private static final Logger LOG = LoggerFactory.getLogger(DeployJob.class);
    public static final String bundlePrefix = "bundles";
    // Indicates whether EC2 instances should be EBS optimized.
    private static final boolean EBS_OPTIMIZED = "true".equals(DataManager.getConfigPropertyAsText("modules.deployment.ec2.ebs_optimized"));
    // Indicates the node.js version installed by nvm to set the PATH variable to point to
    private static final String NODE_VERSION = "v12.16.3";
    public static final String OTP_RUNNER_BRANCH = DataManager.getConfigPropertyAsText(
        "modules.deployment.ec2.otp_runner_branch",
        "master"
    );
    public static final String OTP_RUNNER_STATUS_FILE = "status.json";
    private static final String OTP_RUNNER_LOG_FILE = "/var/log/otp-runner.log";
    // Note: using a cloudfront URL for these download repo URLs will greatly increase download/deploy speed.
    private static final String OTP_REPO_URL = DataManager.getConfigPropertyAsText(
        "modules.deployment.otp_download_url",
        "https://opentripplanner-builds.s3.amazonaws.com"
    );
    private static final String BUILD_CONFIG_FILENAME = "build-config.json";
    private static final String ROUTER_CONFIG_FILENAME = "router-config.json";

    /**
     * Deployment to EC2 servers assumes that nginx is setup with a directory publicly exposed to the internet where
     * status files can be created. This is that directory.
     */
    private static final String EC2_WEB_DIR = "/usr/share/nginx/client";

    /**
     * S3 bucket to upload deployment to. If not null, uses {@link OtpServer#s3Bucket}. Otherwise, defaults to 
     * {@link S3Utils#DEFAULT_BUCKET}
     * */
    private final String s3Bucket;
    private final int targetCount;
    private final DeployType deployType;
    private final String customRegion;
    // a nonce that is used with otp-runner to verify that status files produced by otp-runner are from this deployment
    private final String nonce = UUID.randomUUID().toString();
    // whether the routerConfig was already uploaded (only applies in ec2 deployments)
    private boolean routerConfigUploaded = false;

    private int tasksCompleted = 0;
    private int totalTasks;

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");

    /** The deployment to deploy */
    private Deployment deployment;

    /** The OTP server to deploy to (also contains S3 information). */
    private final OtpServer otpServer;

    /** Temporary file that contains the deployment data */
    private File deploymentTempFile;

    /** This hides the status field on the parent class, providing additional fields. */
    public DeployStatus status;

    private int serverCounter = 0;
    private String dateString = DATE_FORMAT.format(new Date());
    private String jobRelativePath;

    /** If true, don't upload stuff to S3. (Used only during testing) */
    private final boolean dryRun;

    @JsonIgnore @BsonIgnore
    public boolean isOtp2() {
        return Deployment.TripPlannerVersion.OTP_2.equals(deployment.tripPlannerVersion);
    }

    @JsonProperty
    public String getDeploymentId () {
        return deployment.id;
    }

    @JsonProperty
    public String getProjectId () {
        return deployment.projectId;
    }

    /** Increment the completed servers count (for use during ELB deployment) and update the job status. */
    public void incrementCompletedServers() {
        status.numServersCompleted++;
        int totalServers = otpServer.ec2Info.instanceCount;
        if (totalServers < 1) totalServers = 1;
        int numRemaining = totalServers - status.numServersCompleted;
        double newStatus = status.percentComplete + (100 - status.percentComplete) * numRemaining / totalServers;
        status.update(String.format("Completed %d servers. %d remaining...", status.numServersCompleted, numRemaining), newStatus);
    }

    public String getNonce() {
        return nonce;
    }

    @JsonProperty
    public String getServerId () {
        return otpServer.id;
    }

    public Deployment getDeployment() {
        return deployment;
    }

    public OtpServer getOtpServer() {
        return otpServer;
    }

    /**
     * Primary constructor for kicking off a deployment of transit/OSM/config data to OpenTripPlanner.
     *
     * FIXME: It appears that DeployType#Replace is the only type used in the non-test code. Should the others be
     *  removed?
     * @param deployment the deployment (set of feed versions and configurations) to deploy
     * @param owner the requesting user
     * @param otpServer the server/ELB target for the deployment
     */
    public DeployJob(Deployment deployment, Auth0UserProfile owner, OtpServer otpServer) {
        this(deployment, owner, otpServer, null, DeployType.REPLACE);
    }

    public DeployJob(
        Deployment deployment,
        Auth0UserProfile owner,
        OtpServer otpServer,
        String bundlePath,
        DeployType deployType
    ) {
        this(
            "Deploying " + deployment.name,
            deployment,
            owner,
            otpServer,
            bundlePath,
            deployType,
            false
        );
    }

    public DeployJob(
        String jobName,
        Deployment deployment,
        Auth0UserProfile owner,
        OtpServer otpServer,
        String bundlePath,
        DeployType deployType,
        boolean dryRun
    ) {

        // TODO add new job type or get rid of enum in favor of just using class names
        super(owner, jobName, JobType.DEPLOY_TO_OTP);
        this.dryRun = dryRun;
        this.deployment = deployment;
        this.otpServer = otpServer;
        this.s3Bucket = otpServer.s3Bucket != null ? otpServer.s3Bucket : S3Utils.DEFAULT_BUCKET;
        // Use a special subclass of status here that has additional fields
        this.status = new DeployStatus();
        this.status.name = jobName;
        this.targetCount = otpServer.internalUrl != null ? otpServer.internalUrl.size() : 0;
        this.totalTasks = 1 + targetCount;
        status.message = "Initializing...";
        status.built = false;
        status.numServersCompleted = 0;
        status.totalServers = otpServer.internalUrl == null ? 0 : otpServer.internalUrl.size();
        this.deployType = deployType;
        if (bundlePath == null) {
            // Use standard path for bundle.
            this.jobRelativePath = String.join("/", bundlePrefix, deployment.projectId, deployment.id, this.jobId);
        } else {
            // Override job relative path so that bundle can be downloaded directly. Note: this is currently only used
            // for testing (DeployJobTest), but the uses may be expanded in order to perhaps add a server to an existing
            // deployment using either a specified bundle or Graph.obj.
            this.jobRelativePath = bundlePath;
        }
        this.customRegion = otpServer.ec2Info != null && otpServer.ec2Info.region != null
            ? otpServer.ec2Info.region
            : null;
    }

    public void jobLogic () {
        if (otpServer.ec2Info != null) totalTasks++;
        // If needed, dump the GTFS feeds and OSM to a zip file and optionally upload to S3. Since ec2 deployments use
        // otp-runner to automatically download all files needed for the bundle, skip this step if ec2 deployment is
        // enabled and there are internal urls to deploy the graph over wire.
        if (
            deployType.equals(DeployType.REPLACE) &&
                (otpServer.ec2Info == null ||
                    (otpServer.internalUrl != null && otpServer.internalUrl.size() > 0)
                )
        ) {
            if (otpServer.s3Bucket != null) totalTasks++;
            try {
                deploymentTempFile = File.createTempFile("deployment", ".zip");
            } catch (IOException e) {
                status.fail("Could not create temp file for deployment", e);
                return;
            }

            LOG.info("Created deployment bundle file: " + deploymentTempFile.getAbsolutePath());

            // Dump the deployment bundle to the temp file.
            try {
                status.message = "Creating transit bundle (GTFS and OSM)";
                // Only download OSM extract if an OSM extract does not exist at a public URL and not skipping extract.
                boolean includeOsm = deployment.osmExtractUrl == null && !deployment.skipOsmExtract;
                // TODO: At this stage, perform a HEAD request on OSM extract URL to verify that it exists before
                //  continuing with deployment. The same probably goes for the specified OTP jar file.
                this.deployment.dump(deploymentTempFile, true, includeOsm, true);
                tasksCompleted++;
            } catch (Exception e) {
                status.fail("Error dumping deployment", e);
                return;
            }

            status.percentComplete = 100.0 * (double) tasksCompleted / totalTasks;
            LOG.info("Deployment pctComplete = {}", status.percentComplete);
            status.built = true;

            // Upload to S3, if specifically required by the OTPServer or needed for servers in the target group to fetch.
            if (otpServer.s3Bucket != null || otpServer.ec2Info != null) {
                if (!DataManager.useS3) {
                    status.fail("Cannot upload deployment to S3. Application not configured for s3 storage.");
                    return;
                }
                try {
                    uploadBundleToS3();
                } catch (Exception e) {
                    status.fail(String.format("Error uploading/copying deployment bundle to s3://%s", s3Bucket), e);
                }
            }
        }

        // Handle spinning up new EC2 servers for the load balancer's target group.
        if (otpServer.ec2Info != null) {
            if ("true".equals(DataManager.getConfigPropertyAsText("modules.deployment.ec2.enabled"))) {
                replaceEC2Servers();
                tasksCompleted++;
            } else {
                status.fail("Cannot complete deployment. EC2 deployment disabled in server configuration.");
                return;
            }
        }
        else if ("true".equals(System.getenv("RUN_E2E"))) {
            // If running E2E tests, fire up an otp-runner graph build on the same machine.
            try {
                // Generate a basic otp-runner manifest
                OtpRunnerManifest manifest = new OtpRunnerManifest();
                // add common settings
                manifest.baseFolder = String.format("/tmp/%s/graphs", getTripPlannerString());
                manifest.baseFolderDownloads = new ArrayList<>();
                manifest.jarFile = getJarFileOnInstance();
                manifest.nonce = this.nonce;
                manifest.otpRunnerLogFile = OTP_RUNNER_LOG_FILE;
                manifest.otpVersion = isOtp2()
                    ? "2.x"
                    : "1.x";
                manifest.prefixLogUploadsWithInstanceId = true;
                manifest.statusFileLocation = String.format("%s/%s", "/var/log", OTP_RUNNER_STATUS_FILE);
                manifest.uploadOtpRunnerLogs = false;
                manifest.buildGraph = true;
                try {
                    if (deployment.feedVersionIds.size() > 0) {
                        // add OSM data
                        URL osmDownloadUrl = deployment.getUrlForOsmExtract();
                        if (osmDownloadUrl != null) {
                            addUriAsBaseFolderDownload(manifest, osmDownloadUrl.toString());
                        }

                        // add GTFS data
                        for (String feedVersionId : deployment.feedVersionIds) {
                            CustomFile gtfsFile = new CustomFile();
                            // OTP 2.x must have the string `gtfs` somewhere inside the filename, so prepend the filename
                            // with the string `gtfs-`.
                            gtfsFile.filename = String.format("gtfs-%s", feedVersionId);
                            gtfsFile.uri = S3Utils.getS3FeedUri(feedVersionId);
                            addCustomFileAsBaseFolderDownload(manifest, gtfsFile);
                        }
                    }
                } catch (MalformedURLException e) {
                    status.fail("Failed to create base folder download URLs!", e);
                    return;
                }
                // The graph stays on this machine for e2e tests.
                manifest.uploadGraph = false;
                manifest.uploadGraphBuildLogs = false;
                manifest.uploadGraphBuildReport = false;
                // A new OTP instance should not be started. In E2E environments,
                // there is already an OTP instance running in the background,
                // and the test emulates updating the router graph in that OTP instance.
                manifest.runServer = false;

                // Write manifest to temp file
                // (CI directories are managed separately).
                String otpRunnerManifestFile = String.format("/tmp/%s/otp-runner-manifest.json", getTripPlannerString());
                File otpManifestFile = new File(otpRunnerManifestFile);
                otpManifestFile.createNewFile();
                LOG.info("E2E otp-runner empty manifest file created.");

                try (
                    FileWriter fw =  new FileWriter(otpManifestFile)
                ) {
                    ObjectMapper mapper = new ObjectMapper();
                    mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
                    fw.write(mapper.writeValueAsString(manifest));
                    LOG.info("E2E otp-runner manifest file written.");
                } catch (JsonProcessingException e) {
                    status.fail("Failed to create E2E manifest for otp-runner!", e);
                    return;
                }

                // Run otp-runner with the manifest produced earlier.
                Process p = Runtime.getRuntime().exec(String.format("otp-runner %s", otpRunnerManifestFile));
                p.waitFor();
                LOG.info("otp-runner exit code: {}", p.exitValue());
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        // If there are no OTP targets (i.e. we're only deploying to S3), we're done.
        if(otpServer.internalUrl != null) {
            // If we come to this point, there are internal URLs we need to deploy to (i.e., build graph over the wire).
            boolean sendOverWireSuccessful = buildGraphOverWire();
            if (!sendOverWireSuccessful) return;
            // Set baseUrl after success.
            status.baseUrl = otpServer.publicUrl;
        }

        status.completed = true;
    }

    /**
     * Obtains an EC2 client from the AWS Utils client manager that is applicable to this deploy job's AWS
     * configuration. It is important to obtain a client this way so that the client is assured to be valid in the event
     * that a client is obtained that has a session that eventually expires.
     */
    @JsonIgnore
    public AmazonEC2 getEC2ClientForDeployJob() throws CheckedAWSException {
        return EC2Utils.getEC2Client(otpServer.role, customRegion);
    }

    /**
     * Obtains an ELB client from the AWS Utils client manager that is applicable to this deploy job's AWS
     * configuration. It is important to obtain a client this way so that the client is assured to be valid in the event
     * that a client is obtained that has a session that eventually expires.
     */
    @JsonIgnore
    public AmazonElasticLoadBalancing getELBClientForDeployJob() throws CheckedAWSException {
        return EC2Utils.getELBClient(otpServer.role, customRegion);
    }

    /**
     * Obtains an S3 client from the AWS Utils client manager that is applicable to this deploy job's AWS
     * configuration. It is important to obtain a client this way so that the client is assured to be valid in the event
     * that a client is obtained that has a session that eventually expires.
     */
    @JsonIgnore
    public AmazonS3 getS3ClientForDeployJob() throws CheckedAWSException {
        return S3Utils.getS3Client(otpServer.role, customRegion);
    }

    /**
     * Upload to S3 the transit data bundle zip that contains GTFS zip files, OSM data, and config files.
     */
    private void uploadBundleToS3() throws InterruptedException, IOException, CheckedAWSException {
        AmazonS3URI uri = new AmazonS3URI(getS3BundleURI());
        String bucket = uri.getBucket();
        status.message = "Uploading bundle to " + getS3BundleURI();
        status.uploadingS3 = true;
        LOG.info("Uploading deployment {} to {}", deployment.name, uri.toString());
        // Use Transfer Manager so we can monitor S3 bundle upload progress.
        TransferManager transferManager = TransferManagerBuilder
            .standard()
            .withS3Client(getS3ClientForDeployJob())
            .build();
        final Upload uploadBundle = transferManager.upload(bucket, uri.getKey(), deploymentTempFile);
        uploadBundle.addProgressListener(
            (ProgressListener) progressEvent -> status.percentUploaded = uploadBundle.getProgress().getPercentTransferred()
        );
        uploadBundle.waitForCompletion();
        // Check if router config exists and upload as separate file using transfer manager. Note: this is because we
        // need the router-config separately from the bundle for EC2 instances that download the graph only.
        byte[] routerConfigAsBytes = deployment.generateRouterConfig();
        if (routerConfigAsBytes != null) {
            LOG.info("Uploading router-config.json to s3 bucket");
            // Write router config to temp file.
            File routerConfigFile = File.createTempFile("router-config", ".json");
            FileOutputStream out = new FileOutputStream(routerConfigFile);
            out.write(routerConfigAsBytes);
            out.close();
            // Upload router config.
            transferManager
                .upload(bucket, getS3FolderURI().getKey() + "/" + ROUTER_CONFIG_FILENAME, routerConfigFile)
                .waitForCompletion();
            // Delete temp file.
            routerConfigFile.delete();
        }
        // Shutdown the Transfer Manager, but don't shut down the underlying S3 client.
        // The default behavior for shutdownNow shut's down the underlying s3 client
        // which will cause any following s3 operations to fail.
        transferManager.shutdownNow(false);

        // copy to [name]-latest.zip
        String copyKey = getLatestS3BundleKey();
        CopyObjectRequest copyObjRequest = new CopyObjectRequest(bucket, uri.getKey(), uri.getBucket(), copyKey);
        getS3ClientForDeployJob().copyObject(copyObjRequest);
        LOG.info("Copied to s3://{}/{}", bucket, copyKey);
        LOG.info("Uploaded to {}", getS3BundleURI());
        status.update("Upload to S3 complete.", status.percentComplete + 10);
        status.uploadingS3 = false;
    }

    /**
     * Builds the OTP graph over wire, i.e., send the data over an HTTP POST request to boot/replace the existing graph
     * using the OTP Routers#buildGraphOverWire endpoint.
     */
    private boolean buildGraphOverWire() {
        // Send the deployment file over the wire to each OTP server.
        for (String rawUrl : otpServer.internalUrl) {
            status.message = "Deploying to " + rawUrl;
            status.uploading = true;
            LOG.info(status.message);

            URL url;
            try {
                url = new URL(rawUrl + "/routers/" + getRouterId());
            } catch (MalformedURLException e) {
                status.fail(String.format("Malformed deployment URL %s", rawUrl), e);
                return false;
            }

            // grab them synchronously, so that we only take down one OTP server at a time
            HttpURLConnection conn;
            try {
                conn = (HttpURLConnection) url.openConnection();
            } catch (IOException e) {
                status.fail(String.format("Unable to open URL of OTP server %s", url), e);
                return false;
            }

            conn.addRequestProperty("Content-Type", "application/zip");
            conn.setDoOutput(true);
            // graph build can take a long time but not more than an hour, I should think
            conn.setConnectTimeout(60 * 60 * 1000);
            conn.setFixedLengthStreamingMode(deploymentTempFile.length());

            // this makes it a post request so that we can upload our file
            WritableByteChannel post;
            try {
                post = Channels.newChannel(conn.getOutputStream());
            } catch (IOException e) {
                status.fail(String.format("Could not open channel to OTP server %s", url), e);
                return false;
            }

            // retrieveById the input file
            FileChannel input;
            try {
                input = new FileInputStream(deploymentTempFile).getChannel();
            } catch (FileNotFoundException e) {
                status.fail("Internal error: could not read dumped deployment!", e);
                return false;
            }

            try {
                conn.connect();
            } catch (IOException e) {
                status.fail(String.format("Unable to open connection to OTP server %s", url), e);
                return false;
            }

            // copy
            try {
                input.transferTo(0, Long.MAX_VALUE, post);
            } catch (IOException e) {
                status.fail(String.format("Unable to transfer deployment to server %s", url), e);
                return false;
            }

            try {
                post.close();
            } catch (IOException e) {
                status.fail(String.format("Error finishing connection to server %s", url), e);
                return false;
            }

            try {
                input.close();
            } catch (IOException e) {
                // do nothing
                LOG.warn("Could not close input stream for deployment file.");
            }

            status.uploading = false;

            // wait for the server to build the graph
            // TODO: timeouts?
            try {
                int code = conn.getResponseCode();
                if (code != HttpURLConnection.HTTP_CREATED) {
                    // Get input/error stream from connection response.
                    InputStream stream = code < HttpURLConnection.HTTP_BAD_REQUEST
                        ? conn.getInputStream()
                        : conn.getErrorStream();
                    String response;
                    try (Scanner scanner = new Scanner(stream)) {
                        scanner.useDelimiter("\\Z");
                        response = scanner.next();
                    }
                    status.fail(String.format("Got response code %d from server due to %s", code, response));
                    // Skip deploying to any other servers.
                    // There is no reason to take out the rest of the servers, it's going to have the same result.
                    return false;
                }
            } catch (IOException e) {
                status.fail(String.format("Could not finish request to server %s", url), e);
            }

            status.numServersCompleted++;
            tasksCompleted++;
            status.percentComplete = 100.0 * (double) tasksCompleted / totalTasks;
        }
        return true;
    }

    private String getS3BundleURI() {
        return joinToS3FolderUri("bundle.zip");
    }

    private String  getLatestS3BundleKey() {
        String name = StringUtils.getCleanName(deployment.parentProject().name.toLowerCase());
        return String.format("%s/%s/%s-latest.zip", bundlePrefix, deployment.projectId, name);
    }

    @Override
    public void jobFinished () {
        // Delete temp file containing OTP deployment (OSM extract and GTFS files) so that the server's disk storage
        // does not fill up.
        if (deployType.equals(DeployType.REPLACE) && deploymentTempFile != null) {
            boolean deleted = deploymentTempFile.delete();
            if (!deleted) {
                LOG.error("Deployment {} not deleted! Disk space in danger of filling up.", deployment.id);
            }
        }
        String message;
        // FIXME: For some reason status duration is not getting set properly in MonitorableJob.
        status.duration = System.currentTimeMillis() - status.startTime;
        // persist value on most recently fetched deployment as there could have been changes to the deployment
        // before this point
        Deployment latestDeployment = Persistence.deployments.getById(deployment.id);
        if (!status.error) {
            // Update status with successful completion state only if no error was encountered.
            status.completeSuccessfully("Deployment complete!");
            // Store the target server in the deployedTo field and set last deployed time.
            LOG.info("Updating deployment target and deploy time.");
            latestDeployment.deployedTo = otpServer.id;
            long durationMinutes = TimeUnit.MILLISECONDS.toMinutes(status.duration);
            message = String.format("%s successfully deployed %s to %s in %s minutes.", owner.getEmail(), deployment.name, otpServer.publicUrl, durationMinutes);
        } else {
            message = String.format("WARNING: Deployment %s failed to deploy to %s. Error: %s", deployment.name, otpServer.publicUrl, status.message);
        }
        // Unconditionally add deploy summary. If the job fails, we should still record the summary.
        latestDeployment.deployJobSummaries.add(0, new DeploySummary(this));
        Persistence.deployments.replace(deployment.id, latestDeployment);

        // Send notification to those subscribed to updates for the deployment.
        NotifyUsersForSubscriptionJob.createNotification("deployment-updated", deployment.id, message);
        startAnotherAutoDeploymentIfNeeded();
    }

    /**
     * Checks if there is a need to start a new auto-deploy job. A new FetchSingleFeedJob could be started after the
     * AutoDeployJob has made sure no fetching jobs exist and the DeployJob has started. Therefore this new feed version
     * wouldn't result in a new DeployJob getting kicked off since there was already one running.
     */
    private void startAnotherAutoDeploymentIfNeeded() {
        Project project = deployment.parentProject();
        Set<String> pinnedFeedVersionIds = new HashSet<>(deployment.pinnedfeedVersionIds);

        // don't auto-deploy if an error occurred with this deployment
        boolean shouldStartAnotherAutoDeployment = !status.error &&
            // make sure deployment is enabled for data tools. Not sure how we'd get this far if it weren't...
            DataManager.isModuleEnabled("deployment") &&
            // make sure auto-deployment is enabled for the project
            project.autoDeployTypes.size() > 0 &&
            // check if there are any non-pinned feed versions newer than existing ones
            deployment.feedVersionIds.stream()
                .anyMatch(feedVersionId -> {
                    // don't check pinned feed versions for a more recent feed version from the feed source
                    if (pinnedFeedVersionIds.contains(feedVersionId)) {
                        return false;
                    }

                    // get the latest feed version (it could be null)
                    FeedVersion latest = Persistence.feedVersions.getById(feedVersionId)
                        .parentFeedSource()
                        .retrieveLatest();
                    // return true if the latest feed version was created after the last time an auto-deploy job
                    // completed for the project. This means there is at least one new feed version that hasn't
                    // yet been auto deployed that must have been created while this deploy job ran.
                    return latest != null && latest.dateCreated.after(project.lastAutoDeploy);
                });

        if (shouldStartAnotherAutoDeployment) {
            // newer feed versions exist! Start a new auto-deploy job.
            JobUtils.heavyExecutor.execute(new AutoDeployJob(deployment.parentProject(), owner));
        } else {
            LOG.info("No need to start another auto-deployment");
        }
    }

    /**
     * Start up EC2 instances as trip planning servers running on the provided ELB. If the graph build and run config are
     * different, this method will start a separate graph building instance (generally a short-lived, high-powered EC2
     * instance) that terminates after graph build completion and is replaced with long-lived, smaller instances.
     * Otherwise, it will simply use the graph build instance as a long-lived instance. After monitoring the server
     * statuses and verifying that they are running, the previous EC2 instances assigned to the ELB are removed and
     * terminated.
     */
    private void replaceEC2Servers() {
        // Before starting any instances, validate the EC2 configuration to save time down the road (e.g., if a config
        // issue were encountered after a long graph build).
        status.message = "Validating AWS config";
        try {
            EC2ValidationResult ec2ValidationResult = otpServer.validateEC2Config();
            if (!ec2ValidationResult.isValid()) {
                status.fail(ec2ValidationResult.getMessage(), ec2ValidationResult.getException());
                return;
            }
            S3Utils.verifyS3WritePermissions(S3Utils.getS3Client(otpServer), otpServer.s3Bucket);
        } catch (Exception e) {
            status.fail("An error occurred while validating the AWS configuration", e);
            return;
        }
        try {
            // Track any previous instances running for the server we're deploying to in order to de-register and
            // terminate them later.
            List<EC2InstanceSummary> previousInstances = null;
            try {
                previousInstances = otpServer.retrieveEC2InstanceSummaries();
            } catch (CheckedAWSException e) {
                status.fail("Failed to retrieve previously running EC2 instances!", e);
                return;
            }
            // Track new instances that should be added to target group once the deploy job is completed.
            List<Instance> newInstancesForTargetGroup = new ArrayList<>();
            // Initialize recreate build image job and executor in case they're needed below.
            ExecutorService recreateBuildImageExecutor = null;
            RecreateBuildImageJob recreateBuildImageJob = null;
            // First start graph-building instance and wait for graph to successfully build.
            if (!deployType.equals(DeployType.USE_PREBUILT_GRAPH)) {
                status.message = "Starting up graph building EC2 instance";
                List<Instance> graphBuildingInstances = startEC2Instances(1, false);
                // Exit if an error was encountered.
                if (status.error || graphBuildingInstances.size() == 0) {
                    terminateInstances(graphBuildingInstances);
                    return;
                }
                status.message = "Waiting for graph build to complete...";
                MonitorServerStatusJob monitorGraphBuildServer = new MonitorServerStatusJob(
                    owner,
                    this,
                    graphBuildingInstances.get(0),
                    false
                );
                // Run job synchronously because nothing else can be accomplished while the graph build is underway.
                monitorGraphBuildServer.run();

                if (monitorGraphBuildServer.status.error) {
                    // If an error occurred while monitoring the initial server, fail this job and instruct user to inspect
                    // build logs.
                    status.fail("Error encountered while building graph. Inspect build logs.");
                    terminateInstances(graphBuildingInstances);
                    return;
                }

                status.update("Graph build is complete!", 40);
                // If only building graph, terminate the graph building instance and then mark the job as finished. We
                // do not want to proceed with the rest of the job which would shut down existing servers running for
                // the deployment.
                if (deployment.buildGraphOnly) {
                    if (terminateInstances(graphBuildingInstances)) {
                        status.update("Graph build is complete!", 100);
                    }
                    return;
                }

                // Check if a new image of the instance with the completed graph build should be created.
                if (otpServer.ec2Info.recreateBuildImage) {
                    // Create a graph build image in a new thread, so that the remaining servers can be booted
                    // up simultaneously.
                    recreateBuildImageJob = new RecreateBuildImageJob(
                        this,
                        owner,
                        graphBuildingInstances
                    );
                    recreateBuildImageExecutor = Executors.newSingleThreadExecutor();
                    recreateBuildImageExecutor.execute(recreateBuildImageJob);
                }
                // Check whether the graph build instance type or AMI ID is different from the non-graph building type.
                // If so, update the number of servers remaining to the total amount of servers that should be started
                // and, if no recreate build image job is in progress, terminate the graph building instance.
                if (otpServer.ec2Info.hasSeparateGraphBuildConfig()) {
                    // different instance type and/or ami exists for graph building, so update the number of instances
                    // to start up to be the full amount of instances.
                    status.numServersRemaining = Math.max(otpServer.ec2Info.instanceCount, 1);
                    if (!otpServer.ec2Info.recreateBuildImage) {
                        // the build image should not be recreated, so immediately terminate the graph building
                        // instance. If image recreation is enabled, the graph building instance is terminated at the
                        // end of the RecreateBuildImageJob.
                        if (!terminateInstances(graphBuildingInstances)) {
                            // failed to terminate graph building instance
                            return;
                        }
                    }
                } else {
                    // The graph build configuration is identical to the run config, so keep the graph building instance
                    // on and add it to the list of running instances.
                    newInstancesForTargetGroup.addAll(graphBuildingInstances);
                    status.numServersRemaining = otpServer.ec2Info.instanceCount <= 0
                        ? 0
                        : otpServer.ec2Info.instanceCount - 1;
                }
            }
            status.update("Starting server instances", 50);
            // Spin up remaining servers which will download the graph from S3.
            List<MonitorServerStatusJob> remainingServerMonitorJobs = new ArrayList<>();
            List<Instance> remainingInstances = new ArrayList<>();
            if (status.numServersRemaining > 0) {
                // Spin up remaining EC2 instances.
                status.message = String.format("Spinning up remaining %d instance(s).", status.numServersRemaining);
                remainingInstances.addAll(startEC2Instances(status.numServersRemaining, true));
                if (remainingInstances.size() == 0 || status.error) {
                    terminateInstances(remainingInstances);
                    return;
                }
                status.message = String.format(
                    "Waiting for %d remaining instance(s) to start OTP server.",
                    status.numServersRemaining
                );
                // Create new thread pool to monitor server setup so that the servers are monitored in parallel.
                ExecutorService service = Executors.newFixedThreadPool(status.numServersRemaining);
                for (Instance instance : remainingInstances) {
                    // Note: new instances are added
                    MonitorServerStatusJob monitorServerStatusJob = new MonitorServerStatusJob(
                        owner,
                        this,
                        instance,
                        true
                    );
                    remainingServerMonitorJobs.add(monitorServerStatusJob);
                    service.submit(monitorServerStatusJob);
                }
                // Shutdown thread pool once the jobs are completed and wait for its termination. Once terminated, we can
                // consider the servers up and running (or they have failed to initialize properly).
                service.shutdown();
                service.awaitTermination(4, TimeUnit.HOURS);
            }
            // Check if any of the monitor jobs encountered any errors and terminate the job's associated instance.
            int numFailedInstances = 0;
            for (MonitorServerStatusJob job : remainingServerMonitorJobs) {
                if (job.status.error) {
                    numFailedInstances++;
                    String id = job.getInstanceId();
                    LOG.warn("Error encountered while monitoring server {}. Terminating.", id);
                    remainingInstances.removeIf(instance -> instance.getInstanceId().equals(id));
                    try {
                        // terminate instance without failing overall deploy job. That happens later.
                        EC2Utils.terminateInstances(getEC2ClientForDeployJob(), id);
                    } catch (Exception e){
                        job.status.message = String.format(
                            "%s During job cleanup, the instance was not properly terminated!",
                            job.status.message
                        );
                    }
                }
            }
            // Add all servers that did not encounter issues to list for registration with ELB.
            newInstancesForTargetGroup.addAll(remainingInstances);
            // Fail deploy job if no instances are running at this point (i.e., graph builder instance has shut down
            // and the graph loading instance(s) failed to load graph successfully).
            if (newInstancesForTargetGroup.size() == 0) {
                status.fail("Job failed because no running instances remain.");
            } else {
                // Deregister and terminate previous EC2 servers running that were associated with this server.
                if (deployType.equals(DeployType.REPLACE)) {
                    List<String> previousInstanceIds = previousInstances.stream().filter(instance -> "running".equals(instance.state.getName()))
                        .map(instance -> instance.instanceId).collect(Collectors.toList());
                    // If there were previous instances assigned to the server, deregister/terminate them (now that the new
                    // instances are up and running).
                    if (previousInstanceIds.size() > 0) {
                        boolean previousInstancesTerminated = EC2Utils.deRegisterAndTerminateInstances(
                            otpServer.role,
                            otpServer.ec2Info.targetGroupArn,
                            customRegion,
                            previousInstanceIds
                        );
                        // If there was a problem during de-registration/termination, notify via status message.
                        if (!previousInstancesTerminated) {
                            failJobWithAppendedMessage(String.format(
                                "Server setup is complete! (WARNING: Could not terminate previous EC2 instances: %s",
                                previousInstanceIds
                            ));
                        }
                    }
                }
                if (numFailedInstances > 0) {
                    failJobWithAppendedMessage(String.format(
                        "%d instances failed to properly start.",
                        numFailedInstances
                    ));
                }
            }
            // Wait for a recreate graph build job to complete if one was started. This must always occur even if the
            // job has already been failed in order to shutdown the recreateBuildImageExecutor.
            if (recreateBuildImageExecutor != null) {
                // store previous message in case of a previous failure.
                String previousMessage = status.message;
                status.update("Waiting for recreate graph building image job to complete", 95);
                while (!recreateBuildImageJob.status.completed) {
                    try {
                        // wait 1 second
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        recreateBuildImageJob.status.fail("An error occurred with the parent DeployJob", e);
                        status.message = previousMessage;
                        failJobWithAppendedMessage(
                            "An error occurred while waiting for the graph build image to be recreated",
                            e
                        );
                        break;
                    }
                }
                recreateBuildImageExecutor.shutdown();
            }
            if (!status.error) {
                // Job is complete.
                status.completeSuccessfully("Server setup is complete!");
            }
        } catch (Exception e) {
            LOG.error("Could not deploy to EC2 server", e);
            status.fail("Could not deploy to EC2 server", e);
        }
    }

    /**
     * Start the specified number of EC2 instances based on the {@link OtpServer#ec2Info}.
     * @param count number of EC2 instances to start
     * @return a list of the instances is returned once the public IP addresses have been assigned
     */
    private List<Instance> startEC2Instances(int count, boolean graphAlreadyBuilt) {
        // Create user data to instruct the ec2 instance to do stuff at startup.
        createAndUploadManifestAndConfigs(graphAlreadyBuilt);
        String userData = constructUserData(graphAlreadyBuilt);
        // Failure was encountered while constructing user data.
        if (userData == null) {
            // Fail job if it is not already failed.
            if (!status.error) status.fail("Error constructing EC2 user data.");
            return Collections.EMPTY_LIST;
        }
        // The subnet ID should only change if starting up a server in some other AWS account. This is not
        // likely to be a requirement.
        // Define network interface so that a public IP can be associated with server.
        InstanceNetworkInterfaceSpecification interfaceSpecification = new InstanceNetworkInterfaceSpecification()
                .withSubnetId(otpServer.ec2Info.subnetId)
                .withAssociatePublicIpAddress(true)
                .withGroups(otpServer.ec2Info.securityGroupId)
                .withDeviceIndex(0);
        // Pick proper ami depending on whether graph is being built and what is defined.
        String amiId = otpServer.ec2Info.getAmiId(graphAlreadyBuilt);
        // Verify that AMI is correctly defined.
        boolean amiIdValid;
        Exception amiCheckException = null;
        try {
            amiIdValid = amiId != null && EC2Utils.amiExists(getEC2ClientForDeployJob(), amiId);
        } catch (Exception e) {
            amiIdValid = false;
            amiCheckException = e;
        }

        if (!amiIdValid) {
            status.fail(
                String.format(
                    "AMI ID (%s) is missing or bad. Check the deployment settings or the default value in the app config at %s",
                    amiId,
                    EC2Utils.AMI_CONFIG_PATH
                ),
                amiCheckException
            );
            return Collections.EMPTY_LIST;
        }

        // Pick proper instance type depending on whether graph is being built and what is defined.
        String instanceType = otpServer.ec2Info.getInstanceType(graphAlreadyBuilt);
        // Verify that instance type is correctly defined.
        try {
            InstanceType.fromValue(instanceType);
        } catch (IllegalArgumentException e) {
            status.fail(String.format(
                "Instance type (%s) is bad. Check the deployment settings. The default value is %s",
                instanceType,
                EC2Utils.DEFAULT_INSTANCE_TYPE
            ), e);
            return Collections.EMPTY_LIST;
        }
        status.message = String.format("Starting up %d new instance(s) to run OTP", count);

        RunInstancesRequest runInstancesRequest = new RunInstancesRequest()
                .withNetworkInterfaces(interfaceSpecification)
                .withInstanceType(instanceType)
                // TODO: Optimize for EBS to support large systems like NYSDOT.
                //  This may incur additional costs and may need to be replaced with some other setting
                //  if it is proves too expensive. However, it may be the easiest way to resolve the
                //  issue where downloading a graph larger than 3GB slows to a halt.
                .withEbsOptimized(EBS_OPTIMIZED)
                .withMinCount(count)
                .withMaxCount(count)
                .withIamInstanceProfile(new IamInstanceProfileSpecification().withArn(otpServer.ec2Info.iamInstanceProfileArn))
                .withImageId(amiId)
                .withKeyName(otpServer.ec2Info.keyName)
                // This will have the instance terminate when it is shut down.
                .withInstanceInitiatedShutdownBehavior("terminate")
                .withUserData(Base64.encodeBase64String(userData.getBytes()));
        List<Instance> instances;
        try {
            // attempt to start the instances. Sometimes, AWS does not have enough availability of the desired instance
            // type and can throw an error at this point.
            instances = getEC2ClientForDeployJob().runInstances(runInstancesRequest).getReservation().getInstances();
        } catch (Exception e) {
            status.fail(String.format("DeployJob failed due to a problem with AWS: %s", e.getMessage()), e);
            return Collections.EMPTY_LIST;
        }

        status.message = "Waiting for instance(s) to start";
        List<String> instanceIds = EC2Utils.getIds(instances);
        Set<String> instanceIpAddresses = new HashSet<>();
        // Wait so that create tags request does not fail because instances not found.
        try {
            Waiter<DescribeInstanceStatusRequest> waiter = getEC2ClientForDeployJob().waiters().instanceStatusOk();
            long beginWaiting = System.currentTimeMillis();
            waiter.run(new WaiterParameters<>(new DescribeInstanceStatusRequest().withInstanceIds(instanceIds)));
            LOG.info("Instance status is OK after {} ms", (System.currentTimeMillis() - beginWaiting));
        } catch (Exception e) {
            status.fail("Waiter for instance status check failed. You may need to terminate the failed instances.", e);
            return Collections.EMPTY_LIST;
        }
        for (Instance instance : instances) {
            // Note: The public IP addresses will likely be null at this point because they take a few seconds to
            // initialize.
            String serverName = String.format("%s %s (%s) %d %s", deployment.tripPlannerVersion, deployment.name, dateString, serverCounter++, graphAlreadyBuilt ? "clone" : "builder");
            LOG.info("Creating tags for new EC2 instance {}", serverName);
            try {
                CreateTagsRequest createTagsRequest = new CreateTagsRequest()
                        .withTags(new Tag("Name", serverName))
                        .withTags(new Tag("projectId", deployment.projectId))
                        .withTags(new Tag("deploymentId", deployment.id))
                        .withTags(new Tag("jobId", this.jobId))
                        .withTags(new Tag("serverId", otpServer.id))
                        .withTags(new Tag("routerId", getRouterId()))
                        .withTags(new Tag("user", retrieveEmail()))
                        .withResources(instance.getInstanceId());

                String tagKey = DataManager.getConfigPropertyAsText("modules.deployment.ec2.tag_key");
                String tagValue = DataManager.getConfigPropertyAsText("modules.deployment.ec2.tag_value");

                Tag customTag = new Tag();
                customTag.setKey(tagKey);
                customTag.setValue(tagValue);

                createTagsRequest = createTagsRequest.withTags(customTag);
                getEC2ClientForDeployJob().createTags(createTagsRequest);
            } catch (Exception e) {
                status.fail("Failed to create tags for instances.", e);
                return instances;
            }
        }
        // Wait up to 10 minutes for IP addresses to be available.
        TimeTracker ipCheckTracker = new TimeTracker(10, TimeUnit.MINUTES);
        // Store the instances with updated IP addresses here.
        List<Instance> updatedInstances = new ArrayList<>();
        Filter instanceIdFilter = new Filter("instance-id", instanceIds);
        // While all of the IPs have not been established, keep checking the EC2 instances, waiting a few seconds between
        // each check.
        String ipCheckMessage = "Checking that public IP address(es) have initialized for EC2 instance(s).";
        status.message = ipCheckMessage;
        while (instanceIpAddresses.size() < instances.size()) {
            LOG.info(ipCheckMessage);
            // Check that all of the instances have public IPs.
            List<Instance> instancesWithIps;
            try {
                instancesWithIps = EC2Utils.fetchEC2Instances(
                    getEC2ClientForDeployJob(),
                    instanceIdFilter
                );
            } catch (Exception e) {
                status.fail(
                    "Failed while waiting for public IP addresses to be assigned to new instance(s)!",
                    e
                );
                return updatedInstances;
            }
            for (Instance instance : instancesWithIps) {
                String publicIp = instance.getPublicIpAddress();
                // If IP has been found, store the updated instance and IP.
                if (publicIp != null) {
                    instanceIpAddresses.add(publicIp);
                    updatedInstances.add(instance);
                }
            }

            try {
                int sleepTimeMillis = 10000;
                LOG.info("Waiting {} seconds to perform another public IP address check...", sleepTimeMillis / 1000);
                Thread.sleep(sleepTimeMillis);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (ipCheckTracker.hasTimedOut()) {
                status.fail("Job timed out due to public IP assignment taking longer than ten minutes!");
                return updatedInstances;
            }
        }
        LOG.info("Public IP addresses have all been assigned. {}", String.join(",", instanceIpAddresses));
        return updatedInstances;
    }

    /**
     * Attempts to terminate the provided instances. If the instances failed to terminate properly, the deploy job is
     * failed. Returns true if the instances terminated successfully. Returns false if instance termination encountered
     * an error and adds to the status message as needed.
     */
    private boolean terminateInstances(List<Instance> instances) {
        TerminateInstancesResult terminateInstancesResult;
        try {
            terminateInstancesResult = EC2Utils.terminateInstances(getEC2ClientForDeployJob(), instances);
        } catch (Exception e) {
            failJobWithAppendedMessage(
                "During job cleanup, an instance was not properly terminated!",
                e
            );
            return false;
        }

        // verify that all instances have terminated
        boolean allInstancesTerminatedProperly = true;
        for (InstanceStateChange terminatingInstance : terminateInstancesResult.getTerminatingInstances()) {
            // instance state code == 32 means the instance is preparing to be terminated.
            // instance state code == 48 means it has been terminated.
            int instanceStateCode = terminatingInstance.getCurrentState().getCode();
            if (instanceStateCode != 32 && instanceStateCode != 48) {
                failJobWithAppendedMessage(
                    String.format("Instance %s failed to properly terminate!", terminatingInstance.getInstanceId())
                );
                allInstancesTerminatedProperly = false;
            }
        }
        return allInstancesTerminatedProperly;
    }

    /**
     * Helper for ${@link DeployJob#failJobWithAppendedMessage(String, Exception)} that doesn't take an exception
     * argument.
     */
    private void failJobWithAppendedMessage(String appendedMessage) {
        failJobWithAppendedMessage(appendedMessage, null);
    }

    /**
     * If the status already has been marked as having errored out, the given message will be appended to the current
     * message, but the given Exception is not added to the status Exception. Otherwise, the status message is set to the
     * given message contents and the job is marked as failed with the given Exception.
     */
    private void failJobWithAppendedMessage(String appendedMessage, Exception e) {
        if (status.error) {
            status.message = String.format("%s %s", status.message, appendedMessage);
        } else {
            status.fail(appendedMessage, e);
        }
    }

    /**
     * @return the router ID for this deployment (defaults to "default")
     */
    private String getRouterId() {
        return deployment.routerId == null ? "default" : deployment.routerId;
    }

    /**
     * Construct the otp-runner manifest and then upload it to AWS S3. Also upload the build or server config to S3 as
     * needed.
     *
     * @param graphAlreadyBuilt whether or not the graph has already been built
     */
    public OtpRunnerManifest createAndUploadManifestAndConfigs(boolean graphAlreadyBuilt) {
        String jarName = getJarName();
        String s3JarUri = getS3JarUri(jarName);
        if (!s3JarUriIsValid(s3JarUri)) {
            return null;
        }
        // create otp-runner config file
        OtpRunnerManifest manifest = new OtpRunnerManifest();
        // add common settings
        manifest.baseFolder = String.format("/var/%s/graphs", getTripPlannerString());
        manifest.baseFolderDownloads = new ArrayList<>();
        manifest.graphObjUri = getS3GraphUri();
        manifest.jarFile = getJarFileOnInstance();
        manifest.jarUri = s3JarUri;
        manifest.nonce = this.nonce;
        // This must be added here because logging starts immediately before defaults are set while validating the
        // manifest
        manifest.otpRunnerLogFile = OTP_RUNNER_LOG_FILE;
        manifest.otpVersion = isOtp2()
            ? "2.x"
            : "1.x";
        manifest.prefixLogUploadsWithInstanceId = true;
        manifest.serverStartupTimeoutSeconds = 3300;
        manifest.s3UploadPath = getS3FolderURI().toString();
        manifest.statusFileLocation = String.format("%s/%s", EC2_WEB_DIR, OTP_RUNNER_STATUS_FILE);
        manifest.uploadOtpRunnerLogs = true;
        // add settings applicable to current instance. Two different manifest files are generated when deploying with
        // different instance types for graph building vs server running
        if (!graphAlreadyBuilt) {
            // settings when graph building needs to happen
            manifest.buildGraph = true;
            try {
                if (deployment.feedVersionIds.size() > 0) {
                    // add OSM data
                    URL osmDownloadUrl = deployment.getUrlForOsmExtract();
                    if (osmDownloadUrl != null) {
                        addUriAsBaseFolderDownload(manifest, osmDownloadUrl.toString());
                    }

                    // add GTFS data
                    for (String feedVersionId : deployment.feedVersionIds) {
                        CustomFile gtfsFile = new CustomFile();
                        // OTP 2.x must have the string `gtfs` somewhere inside the filename, so prepend the filename
                        // with the string `gtfs-`.
                        gtfsFile.filename = String.format("gtfs-%s", feedVersionId);
                        gtfsFile.uri = S3Utils.getS3FeedUri(feedVersionId);
                        addCustomFileAsBaseFolderDownload(manifest, gtfsFile);
                    }
                }
            } catch (MalformedURLException e) {
                status.fail("Failed to create base folder download URLs!", e);
                return null;
            }
            if (
                !addStringContentsAsBaseFolderDownload(
                    manifest,
                    BUILD_CONFIG_FILENAME,
                    deployment.generateBuildConfigAsString()
                )
            ) {
                return null;
            }
            manifest.uploadGraph = true;
            manifest.uploadGraphBuildLogs = true;
            manifest.uploadGraphBuildReport = true;
            boolean buildGraphOnly = deployment.buildGraphOnly || otpServer.ec2Info.hasSeparateGraphBuildConfig();
            if (buildGraphOnly) {
                // This instance should be ran to only build the graph
                manifest.runServer = false;
            } else {
                // This instance will both run a graph and start the OTP server
                if (
                    !addStringContentsAsBaseFolderDownload(
                        manifest,
                        ROUTER_CONFIG_FILENAME,
                        deployment.generateRouterConfigAsString()
                    )
                ) {
                    return null;
                }
                routerConfigUploaded = true;
                manifest.runServer = true;
                manifest.uploadServerStartupLogs = true;
            }

            // add any extra files that should be downloaded based on whether this manifest is for graph building only
            for (CustomFile customFile : deployment.customFiles) {
                if (customFile.useDuringBuild || (!buildGraphOnly && customFile.useDuringServe)) {
                    addCustomFileAsBaseFolderDownload(manifest, customFile);
                }
            }
        } else {
            // This instance will only start the OTP server with a prebuilt graph
            manifest.buildGraph = false;
            addUriAsBaseFolderDownload(manifest, getRouterConfigS3Uri());
            if (!routerConfigUploaded) {
                if (!uploadStringToS3File(ROUTER_CONFIG_FILENAME, deployment.generateRouterConfigAsString())) {
                    return null;
                }
                routerConfigUploaded = true;
            }
            manifest.runServer = true;
            manifest.uploadServerStartupLogs = true;

            // add any extra files that should be downloaded while running the server
            for (CustomFile customFile : deployment.customFiles) {
                if (customFile.useDuringServe) {
                    addCustomFileAsBaseFolderDownload(manifest, customFile);
                }
            }
        }

        // upload otp-runner manifest to s3
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
            if (
                !uploadStringToS3File(
                    getOtpRunnerManifestS3Filename(graphAlreadyBuilt),
                    mapper.writeValueAsString(manifest)
                )
            ) {
                return null;
            }
        } catch (JsonProcessingException e) {
            status.fail("Failed to create manifest for otp-runner!", e);
            return null;
        }
        return manifest;
    }

    /**
     * Adds a custom file as a base folder download. If the custom file has non-null contents, then those contents will
     * be uploaded to AWS S3 and that corresponding AWS S3 URI will be added to the list of base folder downloads. If
     * the custom file has a non-null uri, then it is assumed that the AWS S3 Object already exists. This returns true
     * if uploading to AWS S3 went ok or there was a URI for the custom file.
     */
    private boolean addCustomFileAsBaseFolderDownload(OtpRunnerManifest manifest, CustomFile customFile) {
        OtpRunnerBaseFolderDownload downloadTask = new OtpRunnerBaseFolderDownload();
        downloadTask.name = customFile.filename;

        // check if there is a string as the contents. If this is the case, an upload to s3 will be needed.
        if (customFile.contents != null) {
            // fail the job when a filename is missing if there are contents
            if (org.apache.commons.lang3.StringUtils.isEmpty(customFile.filename)) {
                status.fail("Failed to process a custom file with a missing filename!");
                return false;
            }
            // includes contents, upload them to s3
            return addStringContentsAsBaseFolderDownload(
                manifest,
                customFile.filename,
                customFile.contents
            );
        } else if (customFile.uri != null) {
            // has a URL, add that and return true
            downloadTask.uri = customFile.uri;
            manifest.baseFolderDownloads.add(downloadTask);
            return true;
        } else {
            status.fail(String.format("Failed to upload custom file %s", customFile));
            return false;
        }
    }

    /**
     * Adds a new base folder download task with just the url. Renaming the file to something else is not important
     * or not needed for OTP to properly recognize the file.
     */
    private void addUriAsBaseFolderDownload(OtpRunnerManifest manifest, String uri) {
        OtpRunnerBaseFolderDownload downloadTask = new OtpRunnerBaseFolderDownload();
        downloadTask.uri = uri;
        manifest.baseFolderDownloads.add(downloadTask);
    }

    /**
     * Uploads the given contents to AWS S3 and adds the resulting S3 URL to the otp-runner manifest's
     * baseFolderDownloads field. Returns true if the upload to S3 was successful.
     */
    private boolean addStringContentsAsBaseFolderDownload(OtpRunnerManifest manifest, String filename, String contents) {
        OtpRunnerBaseFolderDownload downloadTask = new OtpRunnerBaseFolderDownload();
        downloadTask.uri = joinToS3FolderUri(filename);
        manifest.baseFolderDownloads.add(downloadTask);
        return uploadStringToS3File(filename, contents);
    }

    /**
     * Construct the user data script (as string) that should be provided to the AMI and executed upon EC2 instance
     * startup.
     */
    public String constructUserData(boolean graphAlreadyBuilt) {
        String otpRunnerManifestS3FilePath = joinToS3FolderUri(getOtpRunnerManifestS3Filename(graphAlreadyBuilt));
        String otpRunnerManifestFileOnInstance = String.format(
            "/var/%s/otp-runner-manifest.json",
            getTripPlannerString()
        );

        List<String> lines = new ArrayList<>();
        lines.add("#!/bin/bash");
        // NOTE: user data output is logged to `/var/log/cloud-init-output.log` automatically with ec2 instances
        // Add some items to the $PATH as the $PATH with user-data scripts differs from the ssh $PATH.
        lines.add("export PATH=\"$PATH:/home/ubuntu/.yarn/bin\"");
        lines.add(String.format("export PATH=\"$PATH:/home/ubuntu/.nvm/versions/node/%s/bin\"", NODE_VERSION));
        // Remove previous files that might have been created during an Image creation
        Arrays.asList(
            String.join("/", EC2_WEB_DIR, OTP_RUNNER_STATUS_FILE),
            otpRunnerManifestFileOnInstance,
            getJarFileOnInstance(),
            OTP_RUNNER_LOG_FILE,
            "/var/log/otp-build.log",
            "/var/log/otp-server.log"
        ).forEach(file -> lines.add(String.format("rm %s || echo '' > /dev/null", file)));

        // download otp-runner manifest
        lines.add(String.format("aws s3 cp %s %s", otpRunnerManifestS3FilePath, otpRunnerManifestFileOnInstance));
        // install otp-runner as a global package thus enabling use of the otp-runner command
        // This will install that latest version of otp-runner from the configured github branch. otp-runner is not yet
        // published as an npm package.
        lines.add(String.format("yarn global add https://github.com/ibi-group/otp-runner.git#%s", OTP_RUNNER_BRANCH));
        // execute otp-runner
        lines.add(String.format("otp-runner %s", otpRunnerManifestFileOnInstance));
        // Return the entire user data script as a single string.
        return String.join("\n", lines);
    }

    /**
     * Return the appropriate otp-runner manifest filename. Two different manifest files are generated when deploying
     * with different instance types for graph building vs server running.
     */
    private String getOtpRunnerManifestS3Filename(boolean graphAlreadyBuilt) {
        return graphAlreadyBuilt ? "otp-runner-server-only-manifest.json" : "otp-runner-graph-build-manifest.json";
    }

    /**
     * Upload a file into the relative path with the given contents
     *
     * @param filename The filename to create in the jobRelativePath S3 directory
     * @param contents The contents to put into the filename
     * @return
     */
    private boolean uploadStringToS3File(String filename, String contents) {
        // if doing this during a dryRun (used only during testing), immediately return true and don't actually upload
        // to S3
        if (dryRun) return true;
        status.message = String.format("uploading %s to S3", filename);
        try {
            getS3ClientForDeployJob().putObject(s3Bucket, String.format("%s/%s", jobRelativePath, filename), contents);
        } catch (Exception e) {
            status.fail(String.format("Failed to upload file %s", filename), e);
            return false;
        }
        return true;
    }

    /**
     * Return the appropriate path to the jar file on the EC2 instance.
     */
    private String getJarFileOnInstance() {
        return String.format("/opt/%s/%s", getTripPlannerString(), getJarName());
    }

    /**
     * Return the appropriate jar name to use for the deployment.
     */
    private String getJarName() {
        String jarName = deployment.otpVersion;
        if (jarName == null) {
            // If there is no version specified, use the default (and persist value).
            jarName = DEFAULT_OTP_VERSION;
            // persist value on most recently fetched deployment as there could have been changes to the deployment
            // before this point
            Deployment latestDeployment = Persistence.deployments.getById(deployment.id);
            latestDeployment.otpVersion = jarName;
            Persistence.deployments.replace(deployment.id, latestDeployment);
        }
        return jarName;
    }

    /**
     * Construct URL for trip planner jar
     */
    private String getS3JarUri(String jarName) {
        String s3JarKey = jarName + ".jar";
        return String.join("/", OTP_REPO_URL, s3JarKey);
    }

    /**
     * Checks if an AWS S3 url is valid by making a HTTP HEAD request and returning true if the request succeeded.
     */
    private boolean s3JarUriIsValid(String s3JarUri) {
        try {
            final URL url = new URL(s3JarUri);
            HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
            httpURLConnection.setRequestMethod("HEAD");
            int responseCode = httpURLConnection.getResponseCode();
            if (responseCode != HttpStatus.OK_200) {
                status.fail(String.format("Requested trip planner jar does not exist at %s", s3JarUri));
                return false;
            }
        } catch (IOException e) {
            status.fail(String.format("Error checking for trip planner jar: %s", s3JarUri));
            return false;
        }
        return true;
    }

    /**
     * For now, OTP is the only supported trip planner that datatools can deploy to. If others are supported in the
     * future, this method should be modified to return the appropriate trip planner string.
     */
    private String getTripPlannerString() {
        return "otp";
    }

    @JsonIgnore
    public String getJobRelativePath() {
        return jobRelativePath;
    }

    @JsonIgnore
    public AmazonS3URI getS3FolderURI() {
        return new AmazonS3URI(String.format("s3://%s/%s", otpServer.s3Bucket, getJobRelativePath()));
    }

    @JsonIgnore
    public String getS3GraphUri() {
        return joinToS3FolderUri(isOtp2() ? "graph.obj" : "Graph.obj");
    }

    /** Join list of paths to S3 URI for job folder to create a fully qualified URI (e.g., s3://bucket/path/to/file). */
    private String joinToS3FolderUri(CharSequence... paths) {
        List<CharSequence> pathList = new ArrayList<>();
        pathList.add(getS3FolderURI().toString());
        pathList.addAll(Arrays.asList(paths));
        return String.join("/", pathList);
    }

    private String getBuildConfigS3Uri() {
        return joinToS3FolderUri(BUILD_CONFIG_FILENAME);
    }

    private String getRouterConfigS3Uri() {
        return joinToS3FolderUri(ROUTER_CONFIG_FILENAME);
    }

    /**
     * Represents the current status of this job.
     */
    public static class DeployStatus extends Status {
        private static final long serialVersionUID = 1L;
        /** Did the manager build the bundle successfully */
        public boolean built;

        /** Is the bundle currently being uploaded to an S3 bucket? */
        public boolean uploadingS3;

        /** How much of the bundle has been uploaded? */
        public double percentUploaded;

        /** To how many servers have we successfully deployed thus far? */
        public int numServersCompleted;

        public int numServersRemaining;

        /** How many servers are we attempting to deploy to? */
        public int totalServers;

        /** Where can the user see the result? */
        public String baseUrl;

    }

    /**
     * Contains details about a specific deployment job in order to preserve and recall this info after the job has
     * completed.
     */
    public static class DeploySummary implements Serializable {
        private static final long serialVersionUID = 1L;
        public DeployStatus status;
        public String serverId;
        public String s3Bucket;
        public String jobId;
        /** URL for build log file from latest deploy job. */
        public String buildArtifactsFolder;
        public String otpVersion;
        public EC2Info ec2Info;
        public String role;
        public long finishTime = System.currentTimeMillis();

        /** Empty constructor for serialization */
        public DeploySummary () { }

        public DeploySummary (DeployJob job) {
            this.serverId = job.otpServer.id;
            this.ec2Info = job.otpServer.ec2Info;
            this.otpVersion = job.deployment.otpVersion;
            this.jobId = job.jobId;
            this.role = job.otpServer.role;
            this.s3Bucket = job.s3Bucket;
            this.status = job.status;
            this.buildArtifactsFolder = job.getS3FolderURI().toString();
        }
    }

    public enum DeployType {
        REPLACE, USE_PRELOADED_BUNDLE, USE_PREBUILT_GRAPH
    }
}
