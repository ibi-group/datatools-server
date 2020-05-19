package com.conveyal.datatools.manager.jobs;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.CreateImageRequest;
import com.amazonaws.services.ec2.model.CreateImageResult;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DeregisterImageRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceNetworkInterfaceSpecification;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.amazonaws.waiters.Waiter;
import com.amazonaws.waiters.WaiterParameters;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.common.utils.AWSUtils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.controllers.api.DeploymentController;
import com.conveyal.datatools.manager.controllers.api.ServerController;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.EC2Info;
import com.conveyal.datatools.manager.models.EC2InstanceSummary;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.StringUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.codec.binary.Base64;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.conveyal.datatools.manager.controllers.api.ServerController.getIds;
import static com.conveyal.datatools.manager.models.Deployment.DEFAULT_OTP_VERSION;
import static com.conveyal.datatools.manager.models.Deployment.DEFAULT_R5_VERSION;
import static com.conveyal.datatools.manager.models.EC2Info.AMI_CONFIG_PATH;
import static com.conveyal.datatools.manager.models.EC2Info.DEFAULT_INSTANCE_TYPE;

/**
 * Deploy the given deployment to the OTP servers specified by targets.
 * @author mattwigway
 *
 */
public class DeployJob extends MonitorableJob {

    private static final Logger LOG = LoggerFactory.getLogger(DeployJob.class);
    private static final String bundlePrefix = "bundles";
    // Indicates whether EC2 instances should be EBS optimized.
    private static final boolean EBS_OPTIMIZED = "true".equals(DataManager.getConfigPropertyAsText("modules.deployment.ec2.ebs_optimized"));
    private static final String OTP_GRAPH_FILENAME = "Graph.obj";
    // Use txt at the end of these filenames so that these can easily be viewed in a web browser.
    public static final String BUNDLE_DOWNLOAD_COMPLETE_FILE = "BUNDLE_DOWNLOAD_COMPLETE.txt";
    public static final String GRAPH_STATUS_FILE = "GRAPH_STATUS.txt";
    private static final long TEN_MINUTES_IN_MILLISECONDS = 10 * 60 * 1000;
    // Note: using a cloudfront URL for these download repo URLs will greatly increase download/deploy speed.
    private static final String R5_REPO_URL = DataManager.hasConfigProperty("modules.deployment.r5_download_url")
        ? DataManager.getConfigPropertyAsText("modules.deployment.r5_download_url")
        : "https://r5-builds.s3.amazonaws.com";
    private static final String OTP_REPO_URL = DataManager.hasConfigProperty("modules.deployment.otp_download_url")
        ? DataManager.getConfigPropertyAsText("modules.deployment.otp_download_url")
        : "https://opentripplanner-builds.s3.amazonaws.com";
    /**
     * S3 bucket to upload deployment to. If not null, uses {@link OtpServer#s3Bucket}. Otherwise, defaults to 
     * {@link DataManager#feedBucket}
     * */
    private final String s3Bucket;
    private final int targetCount;
    private final DeployType deployType;
    private final AWSStaticCredentialsProvider credentials;
    private final String customRegion;

    private int tasksCompleted = 0;
    private int totalTasks;

    private AmazonEC2 ec2;
    private AmazonS3 s3Client;
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

    public DeployJob(Deployment deployment, Auth0UserProfile owner, OtpServer otpServer) {
        this(deployment, owner, otpServer, null, DeployType.REPLACE);
    }

    public DeployJob(Deployment deployment, Auth0UserProfile owner, OtpServer otpServer, String bundlePath, DeployType deployType) {
        this("Deploying " + deployment.name, deployment, owner, otpServer, bundlePath, deployType);
    }

    public DeployJob(String jobName, Deployment deployment, Auth0UserProfile owner, OtpServer otpServer, String bundlePath, DeployType deployType) {
        // TODO add new job type or get rid of enum in favor of just using class names
        super(owner, jobName, JobType.DEPLOY_TO_OTP);
        this.deployment = deployment;
        this.otpServer = otpServer;
        this.s3Bucket = otpServer.s3Bucket != null ? otpServer.s3Bucket : DataManager.feedBucket;
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
        // CONNECT TO EC2/S3
        credentials = AWSUtils.getCredentialsForRole(otpServer.role, this.jobId);
        this.customRegion = otpServer.ec2Info != null && otpServer.ec2Info.region != null
            ? otpServer.ec2Info.region
            : null;
        ec2 = customRegion == null
            ? AWSUtils.getEC2ClientForCredentials(credentials)
            : AWSUtils.getEC2ClientForCredentials(credentials, customRegion);
        s3Client = AWSUtils.getS3ClientForCredentials(credentials, customRegion);
    }

    public void jobLogic () {
        // If not using a preloaded bundle (or pre-built graph), we need to dump the GTFS feeds and OSM to a zip file and optionally upload
        // to S3.
        if (deployType.equals(DeployType.REPLACE)) {
            if (otpServer.s3Bucket != null) totalTasks++;
            if (otpServer.ec2Info != null) totalTasks++;
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
                } catch (AmazonClientException | InterruptedException | IOException e) {
                    status.fail(String.format("Error uploading/copying deployment bundle to s3://%s", s3Bucket), e);
                }

            }
        }

        // Handle spinning up new EC2 servers for the load balancer's target group.
        if (otpServer.ec2Info != null) {
            if ("true".equals(DataManager.getConfigPropertyAsText("modules.deployment.ec2.enabled"))) {
                replaceEC2Servers();
                // If creating a new server, there is no need to deploy to an existing one.
                return;
            } else {
                status.fail("Cannot complete deployment. EC2 deployment disabled in server configuration.");
                return;
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
     * Upload to S3 the transit data bundle zip that contains GTFS zip files, OSM data, and config files.
     */
    private void uploadBundleToS3() throws InterruptedException, AmazonClientException, IOException {
        AmazonS3URI uri = new AmazonS3URI(getS3BundleURI());
        String bucket = uri.getBucket();
        status.message = "Uploading bundle to " + getS3BundleURI();
        status.uploadingS3 = true;
        LOG.info("Uploading deployment {} to {}", deployment.name, uri.toString());
        // Use Transfer Manager so we can monitor S3 bundle upload progress.
        TransferManager transferManager = TransferManagerBuilder.standard().withS3Client(s3Client).build();
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
                .upload(bucket, getS3FolderURI().getKey() + "/router-config.json", routerConfigFile)
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
        s3Client.copyObject(copyObjRequest);
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
        return joinToS3FolderURI("bundle.zip");
    }

    public String getCustomRegion() {
        return customRegion;
    }

    private String  getLatestS3BundleKey() {
        String name = StringUtils.getCleanName(deployment.parentProject().name.toLowerCase());
        return String.format("%s/%s/%s-latest.zip", bundlePrefix, deployment.projectId, name);
    }

    @Override
    public void jobFinished () {
        // Delete temp file containing OTP deployment (OSM extract and GTFS files) so that the server's disk storage
        // does not fill up.
        if (deployType.equals(DeployType.REPLACE)) {
            boolean deleted = deploymentTempFile.delete();
            if (!deleted) {
                LOG.error("Deployment {} not deleted! Disk space in danger of filling up.", deployment.id);
            }
        }
        String message;
        // FIXME: For some reason status duration is not getting set properly in MonitorableJob.
        status.duration = System.currentTimeMillis() - status.startTime;
        if (!status.error) {
            // Update status with successful completion state only if no error was encountered.
            status.completeSuccessfully("Deployment complete!");
            // Store the target server in the deployedTo field and set last deployed time.
            LOG.info("Updating deployment target and deploy time.");
            deployment.deployedTo = otpServer.id;
            long durationMinutes = TimeUnit.MILLISECONDS.toMinutes(status.duration);
            message = String.format("%s successfully deployed %s to %s in %s minutes.", owner.getEmail(), deployment.name, otpServer.publicUrl, durationMinutes);
        } else {
            message = String.format("WARNING: Deployment %s failed to deploy to %s. Error: %s", deployment.name, otpServer.publicUrl, status.message);
        }
        // Unconditionally add deploy summary. If the job fails, we should still record the summary.
        deployment.deployJobSummaries.add(0, new DeploySummary(this));
        Persistence.deployments.replace(deployment.id, deployment);
        // Send notification to those subscribed to updates for the deployment.
        NotifyUsersForSubscriptionJob.createNotification("deployment-updated", deployment.id, message);
    }

    /**
     * Start up EC2 instances as trip planning servers running on the provided ELB. After monitoring the server statuses
     * and verifying that they are running, remove the previous EC2 instances that were assigned to the ELB and terminate
     * them.
     */
    private void replaceEC2Servers() {
        try {
            // Track any previous instances running for the server we're deploying to in order to de-register and
            // terminate them later.
            List<EC2InstanceSummary> previousInstances = otpServer.retrieveEC2InstanceSummaries();
            // Track new instances that should be added to target group once the deploy job is completed.
            List<Instance> newInstancesForTargetGroup = new ArrayList<>();
            // First start graph-building instance and wait for graph to successfully build.
            if (!deployType.equals(DeployType.USE_PREBUILT_GRAPH)) {
                status.message = "Starting up graph building EC2 instance";
                List<Instance> graphBuildingInstances = startEC2Instances(1, false);
                // Exit if an error was encountered.
                if (status.error || graphBuildingInstances.size() == 0) {
                    ServerController.terminateInstances(ec2, graphBuildingInstances);
                    return;
                }
                status.message = "Waiting for graph build to complete...";
                MonitorServerStatusJob monitorInitialServerJob = new MonitorServerStatusJob(
                    owner,
                    this,
                    graphBuildingInstances.get(0),
                    false
                );
                monitorInitialServerJob.run();

                if (monitorInitialServerJob.status.error) {
                    // If an error occurred while monitoring the initial server, fail this job and instruct user to inspect
                    // build logs.
                    status.fail("Error encountered while building graph. Inspect build logs.");
                    ServerController.terminateInstances(ec2, graphBuildingInstances);
                    return;
                }

                status.update("Graph build is complete!", 40);
                // If only building graph, job is finished. Note: the graph building EC2 instance should automatically shut
                // itself down if this flag is turned on (happens in user data). We do not want to proceed with the rest of
                // the job which would shut down existing servers running for the deployment.
                if (deployment.buildGraphOnly) {
                    status.update("Graph build is complete!", 100);
                    return;
                }

                Persistence.deployments.replace(deployment.id, deployment);

                // Check if a new image of the instance with the completed graph build should be created.
                if (otpServer.ec2Info.recreateBuildImage) {
                    status.update("Creating build image", 42.5);
                    // Create a new image of this instance.
                    CreateImageRequest createImageRequest = new CreateImageRequest()
                        .withInstanceId(graphBuildingInstances.get(0).getInstanceId())
                        .withName(otpServer.ec2Info.buildImageName)
                        .withDescription(otpServer.ec2Info.buildImageDescription);
                    CreateImageResult createImageResult = ec2.createImage(createImageRequest);
                    // Deregister old image if it exists
                    if (otpServer.ec2Info.buildAmiId != null) {
                        status.message = "Deregistering old build image";
                        DeregisterImageRequest deregisterImageRequest = new DeregisterImageRequest()
                            .withImageId(otpServer.ec2Info.buildAmiId);
                        ec2.deregisterImage(deregisterImageRequest);
                    }
                    status.update("Updating Server build AMI info", 45);
                    // Update OTP Server info
                    otpServer.ec2Info.buildAmiId = createImageResult.getImageId();
                    otpServer.ec2Info.recreateBuildImage = false;
                    Persistence.servers.replace(otpServer.id, otpServer);
                }
                // Check whether the graph build instance type or AMI ID is different from the non-graph building type.
                // If so, terminate the graph building instance. If not, add the graph building instance to the list
                // of started instances.
                if (otpServer.ec2Info.hasSeparateGraphBuildConfig()) {
                    // different instance type and/or ami exists for graph building. Terminate graph building instance
                    status.update("Terminating build instance", 47.5);
                    ServerController.terminateInstances(ec2, graphBuildingInstances);
                    status.numServersRemaining = Math.max(otpServer.ec2Info.instanceCount, 1);
                } else {
                    // same configuration exists, so keep instance on and add to list of running instances
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
                    ServerController.terminateInstances(ec2, remainingInstances);
                    return;
                }
                // Create new thread pool to monitor server setup so that the servers are monitored in parallel.
                ExecutorService service = Executors.newFixedThreadPool(status.numServersRemaining);
                for (Instance instance : remainingInstances) {
                    // Note: new instances are added
                    MonitorServerStatusJob monitorServerStatusJob = new MonitorServerStatusJob(owner, this, instance, true);
                    remainingServerMonitorJobs.add(monitorServerStatusJob);
                    service.submit(monitorServerStatusJob);
                }
                // Shutdown thread pool once the jobs are completed and wait for its termination. Once terminated, we can
                // consider the servers up and running (or they have failed to initialize properly).
                service.shutdown();
                service.awaitTermination(4, TimeUnit.HOURS);
            }
            // Check if any of the monitor jobs encountered any errors and terminate the job's associated instance.
            for (MonitorServerStatusJob job : remainingServerMonitorJobs) {
                if (job.status.error) {
                    String id = job.getInstanceId();
                    LOG.warn("Error encountered while monitoring server {}. Terminating.", id);
                    remainingInstances.removeIf(instance -> instance.getInstanceId().equals(id));
                    ServerController.terminateInstances(ec2, id);
                }
            }
            // Add all servers that did not encounter issues to list for registration with ELB.
            newInstancesForTargetGroup.addAll(remainingInstances);
            // Fail deploy job if no instances are running at this point (i.e., graph builder instance has shut down
            // and the graph loading instance(s) failed to load graph successfully).
            if (newInstancesForTargetGroup.size() == 0) {
                status.fail("Job failed because no running instances remain.");
                return;
            }
            String finalMessage = "Server setup is complete!";
            // Get EC2 servers running that are associated with this server.
            if (deployType.equals(DeployType.REPLACE)) {
                List<String> previousInstanceIds = previousInstances.stream()
                    .filter(instance -> "running".equals(instance.state.getName()))
                    .map(instance -> instance.instanceId)
                    .collect(Collectors.toList());
                if (previousInstanceIds.size() > 0) {
                    boolean success = ServerController.deRegisterAndTerminateInstances(
                        credentials,
                        otpServer.ec2Info.targetGroupArn,
                        customRegion,
                        previousInstanceIds
                    );
                    // If there was a problem during de-registration/termination, notify via status message.
                    if (!success) {
                        finalMessage = String.format("Server setup is complete! (WARNING: Could not terminate previous EC2 instances: %s", previousInstanceIds);
                    }
                }
            }
            // Job is complete.
            status.completeSuccessfully(finalMessage);
        } catch (Exception e) {
            LOG.error("Could not deploy to EC2 server", e);
            status.fail("Could not deploy to EC2 server", e);
        }
    }

    /**
     * Start the specified number of EC2 instances based on the {@link OtpServer#ec2Info}.
     * @param count number of EC2 instances to start
     * @return a list of the instances is returned once the public IP addresses have been assigned
     *
     * TODO: Booting up R5 servers has not been fully tested.
     */
    private List<Instance> startEC2Instances(int count, boolean graphAlreadyBuilt) {
        // User data should contain info about:
        // 1. Downloading GTFS/OSM info (s3)
        // 2. Time to live until shutdown/termination (for test servers)
        // 3. Hosting / nginx
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
        if (amiId == null || !ServerController.amiExists(amiId, ec2)) {
            status.fail(String.format(
                "AMI ID (%s) is missing or bad. Check the deployment settings or the default value in the app config at %s",
                amiId,
                AMI_CONFIG_PATH
            ));
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
                DEFAULT_INSTANCE_TYPE
            ), e);
            return Collections.EMPTY_LIST;
        }
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
        final List<Instance> instances = ec2.runInstances(runInstancesRequest).getReservation().getInstances();

        List<String> instanceIds = getIds(instances);
        Set<String> instanceIpAddresses = new HashSet<>();
        // Wait so that create tags request does not fail because instances not found.
        try {
            Waiter<DescribeInstanceStatusRequest> waiter = ec2.waiters().instanceStatusOk();
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
            String serverName = String.format("%s %s (%s) %d %s", deployment.r5 ? "r5" : "otp", deployment.name, dateString, serverCounter++, graphAlreadyBuilt ? "clone" : "builder");
            LOG.info("Creating tags for new EC2 instance {}", serverName);
            ec2.createTags(new CreateTagsRequest()
                    .withTags(new Tag("Name", serverName))
                    .withTags(new Tag("projectId", deployment.projectId))
                    .withTags(new Tag("deploymentId", deployment.id))
                    .withTags(new Tag("jobId", this.jobId))
                    .withTags(new Tag("serverId", otpServer.id))
                    .withTags(new Tag("routerId", getRouterId()))
                    .withTags(new Tag("user", retrieveEmail()))
                    .withResources(instance.getInstanceId())
            );
        }
        // Store the instances with updated IP addresses here.
        List<Instance> updatedInstances = new ArrayList<>();
        // Store time we began checking for IP addresses for time out.
        long beginIpCheckTime = System.currentTimeMillis();
        Filter instanceIdFilter = new Filter("instance-id", instanceIds);
        // While all of the IPs have not been established, keep checking the EC2 instances, waiting a few seconds between
        // each check.
        while (instanceIpAddresses.size() < instances.size()) {
            LOG.info("Checking that public IP addresses have initialized for EC2 instances.");
            // Check that all of the instances have public IPs.
            List<Instance> instancesWithIps = DeploymentController.fetchEC2Instances(ec2, instanceIdFilter);
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
            if (System.currentTimeMillis() - beginIpCheckTime > TEN_MINUTES_IN_MILLISECONDS) {
                status.fail("Job timed out due to public IP assignment taking longer than ten minutes!");
                return updatedInstances;
            }
        }
        LOG.info("Public IP addresses have all been assigned. {}", String.join(",", instanceIpAddresses));
        return updatedInstances;
    }

    /**
     * @return the router ID for this deployment (defaults to "default")
     */
    private String getRouterId() {
        return deployment.routerId == null ? "default" : deployment.routerId;
    }

    /**
     * Construct the user data script (as string) that should be provided to the AMI and executed upon EC2 instance
     * startup.
     */
    private String constructUserData(boolean graphAlreadyBuilt) {
        // Prefix/name of JAR file (WITHOUT .jar)
        String jarName = deployment.r5 ? deployment.r5Version : deployment.otpVersion;
        if (jarName == null) {
            if (deployment.r5) deployment.r5Version = DEFAULT_R5_VERSION;
            else deployment.otpVersion = DEFAULT_OTP_VERSION;
            // If there is no version specified, use the default (and persist value).
            jarName = deployment.r5 ? deployment.r5Version : deployment.otpVersion;
            Persistence.deployments.replace(deployment.id, deployment);
        }
        // Construct URL for trip planner jar and check that it exists with a lightweight HEAD request.
        String s3JarKey = jarName + ".jar";
        String repoUrl = deployment.r5 ? R5_REPO_URL : OTP_REPO_URL;
        String s3JarUrl = String.join("/", repoUrl, s3JarKey);
        try {
            final URL url = new URL(s3JarUrl);
            HttpURLConnection huc = (HttpURLConnection) url.openConnection();
            huc.setRequestMethod("HEAD");
            int responseCode = huc.getResponseCode();
            if (responseCode != HttpStatus.OK_200) {
                status.fail(String.format("Requested trip planner jar does not exist at %s", s3JarUrl));
                return null;
            }
        } catch (IOException e) {
            status.fail(String.format("Error checking for trip planner jar: %s", s3JarUrl), e);
            return null;
        }
        String jarDir = String.format("/opt/%s", getTripPlannerString());
        List<String> lines = new ArrayList<>();
        String routerName = "default";
        final String uploadUserDataLogCommand = String.format("aws s3 cp $USERDATALOG %s/${INSTANCE_ID}.log", getS3FolderURI().toString());
        String routerDir = String.format("/var/%s/graphs/%s", getTripPlannerString(), routerName);
        String graphPath = String.join("/", routerDir, OTP_GRAPH_FILENAME);
        //////////////// BEGIN USER DATA
        lines.add("#!/bin/bash");
        // set some variables.
        lines.add(String.format("BUILDLOGFILE=/var/log/%s", getBuildLogFilename()));
        lines.add(String.format("LOGFILE=/var/log/%s.log", getTripPlannerString()));
        lines.add("USERDATALOG=/var/log/user-data.log");
        lines.add("WEB_DIR=/usr/share/nginx/client");
        // Remove previous files that might have been created during an Image creation
        lines.add(String.format("rm $WEB_DIR/%s || echo '' > /dev/null", BUNDLE_DOWNLOAD_COMPLETE_FILE));
        lines.add(String.format("rm $WEB_DIR/%s || echo '' > /dev/null", GRAPH_STATUS_FILE));
        lines.add("rm $BUILDLOGFILE || echo '' > /dev/null");
        lines.add("rm LOGFILE || echo '' > /dev/null");
        lines.add("rm USERDATALOG || echo '' > /dev/null");
        // Get the instance's instance ID from the AWS metadata endpoint.
        lines.add("INSTANCE_ID=`curl http://169.254.169.254/latest/meta-data/instance-id`");
        // Log user data setup to /var/log/user-data.log
        lines.add("exec > >(tee $USERDATALOG|logger -t user-data -s 2>/dev/console) 2>&1");
        // Get the total memory by grepping for MemTotal in meminfo file and removing non-numbers from the line
        // (leaving just the total mem in kb). This is used for starting up the OTP build/run processes.
        lines.add("TOTAL_MEM=`grep MemTotal /proc/meminfo | sed 's/[^0-9]//g'`");
        // If on a low-memory instance (assuming around 2GB of RAM), allocate 1.5GB for java.
        // Otherwise use as much as possible while leaving 2097152 kb (2GB) for the OS
        lines.add("if [ \"2500000\" -gt \"$TOTAL_MEM\" ]; then MEM=1500000; else MEM=`echo $(($TOTAL_MEM - 2097152))`; fi");
        // Configure some stuff for AWS CLI.
        // Note: too many threads/concurrent requests cause a lot of individual thread timeouts for some reason, which
        // ultimately causes the entire cp command to stall out.
        lines.add("aws configure set default.s3.max_concurrent_requests 3");
        lines.add("aws configure set default.s3.multipart_chunksize 32MB");

        // Get region from config or default to us-east-1
        String region;
        if (customRegion != null) {
            region = customRegion;
        } else {
            region = DataManager.getConfigPropertyAsText("application.data.s3_region");
        }
        if (region == null) region = "us-east-1";
        lines.add(String.format("aws configure set default.region %s", region));
        // Create the directory for the graph inputs.
        lines.add(String.format("mkdir -p %s", routerDir));
        lines.add(String.format("chown ubuntu %s", routerDir));
        // Remove the current inputs from router directory.
        lines.add(String.format("rm -rf %s/*", routerDir));
        // Download trip planner JAR.
        lines.add(String.format("mkdir -p %s", jarDir));
        // Add client static file directory for uploading deploy stage status files.
        // TODO: switch to AMI that uses /usr/share/nginx/html as static file dir so we don't have to create this new dir.
        lines.add("sudo mkdir $WEB_DIR");
        lines.add(String.format("wget %s -O %s/%s.jar", s3JarUrl, jarDir, jarName));
        if (graphAlreadyBuilt) {
            lines.add("echo 'downloading graph from s3'");
            // Download Graph from S3.
            String downloadGraph = String.format("time aws s3 --cli-read-timeout 60 cp %s %s ", getS3GraphURI(), graphPath);
            lines.add(downloadGraph);
            // If graph download times out, try again.
            lines.add(String.format("[ -f %s ] && echo 'Graph downloaded!' || %s", graphPath, downloadGraph));
            lines.add(String.format("ls -alh %s", graphPath));
            // Download router config if not null (normally this would be just included as part of bundle.zip, but the
            // bundle is not downloaded when the graph already exists).
            if (deployment.generateRouterConfig() != null) {
                lines.add(String.format("aws s3 --cli-read-timeout 60 cp %s %s ", joinToS3FolderURI("router-config.json"), routerDir + "/"));
            }
        } else {
            // Download data bundle from S3.
            String downloadBundle = String.format("time aws s3 --cli-read-timeout 60 cp %s /tmp/bundle.zip", getS3BundleURI());
            lines.add(downloadBundle);
            // Download OSM extract if exists at URL. Otherwise, it is assumed to be in the bundle.
            if (deployment.osmExtractUrl != null && !deployment.skipOsmExtract) {
                lines.add(String.format("wget %s -O %s/osm.pbf", deployment.osmExtractUrl, routerDir));
            }
            // Determine if bundle download was successful and try again if not.
            lines.add(String.format("[ -f /tmp/bundle.zip ] && echo 'Bundle downloaded!' || %s", downloadBundle));
            lines.add("[ -f /tmp/bundle.zip ] && BUNDLE_STATUS='SUCCESS' || BUNDLE_STATUS='FAILURE'");
            // Upload user data log after bundle download.
            lines.add(uploadUserDataLogCommand);
            // Create file with bundle status in web dir to notify Data Tools that download is complete.
            lines.add(String.format("sudo echo $BUNDLE_STATUS > $WEB_DIR/%s", BUNDLE_DOWNLOAD_COMPLETE_FILE));
            // Put unzipped bundle data into router directory.
            lines.add(String.format("unzip /tmp/bundle.zip -d %s", routerDir));
            lines.add("echo 'starting graph build'");
            // Build the graph.
            if (deployment.r5) lines.add(String.format("sudo -H -u ubuntu java -Xmx${MEM}k -jar %s/%s.jar point --build %s", jarDir, jarName, routerDir));
            else lines.add(String.format("sudo -H -u ubuntu java -jar -Xmx${MEM}k %s/%s.jar --build %s > $BUILDLOGFILE 2>&1", jarDir, jarName, routerDir));
            // Re-upload user data log after build command.
            lines.add(uploadUserDataLogCommand);
            // Upload the build log file, build report and graph to S3.
            if (!deployment.r5) {
                String s3BuildLogPath = joinToS3FolderURI(getBuildLogFilename());
                // upload log file
                lines.add(String.format("aws s3 cp $BUILDLOGFILE %s ", s3BuildLogPath));
                // upload report if it was generated
                String reportPath = String.format("%s/report", routerDir);
                lines.add(String.format(
                    "[ -e %s ] && cd %s && zip -r report.zip report && cd - && aws s3 cp %s.zip %s",
                    reportPath,
                    routerDir,
                    reportPath,
                    joinToS3FolderURI("graph-build-report.zip")
                ));
                // upload graph
                lines.add(String.format("aws s3 cp %s %s ", graphPath, getS3GraphURI()));
            }
        }
        // Determine if graph build/download was successful (and that Graph.obj is not zero bytes).
        lines.add(String.format("FILESIZE=$(wc -c <%s)", graphPath));
        lines.add(String.format("[ -f %s ] && (($FILESIZE > 0)) && GRAPH_STATUS='SUCCESS' || GRAPH_STATUS='FAILURE'", graphPath));
        // Re-upload user data log before indicating that graph build/download is complete.
        lines.add(uploadUserDataLogCommand);
        // Create file with bundle status in web dir to notify Data Tools that download is complete.
        lines.add(String.format("sudo echo $GRAPH_STATUS > $WEB_DIR/%s", GRAPH_STATUS_FILE));
        lines.add("echo 'Graph build/download status: $GRAPH_STATUS'");
        if (deployment.buildGraphOnly) {
            // If building graph only, tell the instance to shut itself down after the graph build (and log upload) is
            // complete.
            lines.add("echo 'shutting down server (build graph only specified in deployment target)'");
            lines.add("sudo poweroff");
        } else {
            // Otherwise, kick off the application.
            lines.add("echo 'kicking off trip planner (logs at $LOGFILE)'");
            if (deployment.r5) lines.add(String.format("sudo -H -u ubuntu nohup java -Xmx${MEM}k -Djava.util.Arrays.useLegacyMergeSort=true -jar %s/%s.jar point --isochrones %s > /var/log/r5.out 2>&1&", jarDir, jarName, routerDir));
            else lines.add(String.format("sudo -H -u ubuntu nohup java -jar -Xmx${MEM}k %s/%s.jar --server --bindAddress 127.0.0.1 --router default > $LOGFILE 2>&1 &", jarDir, jarName));
        }
        // Return the entire user data script as a single string.
        return String.join("\n", lines);
    }

    private String getBuildLogFilename() {
        return String.format("%s-build.log", getTripPlannerString());
    }

    private String getTripPlannerString() {
        return deployment.r5 ? "r5" : "otp";
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
    public String getS3GraphURI() {
        return joinToS3FolderURI(OTP_GRAPH_FILENAME);
    }

    /** Join list of paths to S3 URI for job folder to create a fully qualified URI (e.g., s3://bucket/path/to/file). */
    private String joinToS3FolderURI(CharSequence... paths) {
        List<CharSequence> pathList = new ArrayList<>();
        pathList.add(getS3FolderURI().toString());
        pathList.addAll(Arrays.asList(paths));
        return String.join("/", pathList);
    }

    @JsonIgnore
    public AmazonS3 getS3Client() {
        return s3Client;
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
