package com.conveyal.datatools.manager.jobs;

import com.amazonaws.AmazonClientException;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AmazonEC2Exception;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceNetworkInterfaceSpecification;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;
import com.amazonaws.services.elasticloadbalancingv2.model.DeregisterTargetsRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.DeregisterTargetsResult;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetDescription;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.amazonaws.waiters.Waiter;
import com.amazonaws.waiters.WaiterParameters;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.conveyal.datatools.manager.models.Deployment.DEFAULT_OTP_VERSION;
import static com.conveyal.datatools.manager.models.Deployment.DEFAULT_R5_VERSION;

/**
 * Deploy the given deployment to the OTP servers specified by targets.
 * @author mattwigway
 *
 */
public class DeployJob extends MonitorableJob {

    private static final Logger LOG = LoggerFactory.getLogger(DeployJob.class);
    private static final String bundlePrefix = "bundles";
    /** 
     * S3 bucket to upload deployment to. If not null, uses {@link OtpServer#s3Bucket}. Otherwise, defaults to 
     * {@link DataManager#feedBucket}
     * */
    private final String s3Bucket;
    private final int targetCount;
    private int tasksCompleted = 0;
    private int totalTasks;

    private AmazonEC2 ec2;
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");

    /** The deployment to deploy */
    private Deployment deployment;

    /** The OTP server to deploy to (also contains S3 information). */
    private final OtpServer otpServer;

    /** Temporary file that contains the deployment data */
    private File deploymentTempFile;

    /** This hides the status field on the parent class, providing additional fields. */
    public DeployStatus status;

    private String statusMessage;
    private int serverCounter = 0;
//    private String imageId;
    private String dateString = DATE_FORMAT.format(new Date());
    List<String> amiNameFilter = Collections.singletonList("ubuntu/images/hvm-ssd/ubuntu-xenial-16.04-amd64-server-????????");
    List<String> amiStateFilter = Collections.singletonList("available");

    @JsonProperty
    public String getDeploymentId () {
        return deployment.id;
    }

    public DeployJob(Deployment deployment, String owner, OtpServer otpServer) {
        // TODO add new job type or get rid of enum in favor of just using class names
        super(owner, "Deploying " + deployment.name, JobType.DEPLOY_TO_OTP);
        this.deployment = deployment;
        this.otpServer = otpServer;
        this.s3Bucket = otpServer.s3Bucket != null ? otpServer.s3Bucket : DataManager.feedBucket;
        // Use a special subclass of status here that has additional fields
        this.status = new DeployStatus();
        this.targetCount = otpServer.internalUrl != null ? otpServer.internalUrl.size() : 0;
        this.totalTasks = 1 + targetCount;
        status.message = "Initializing...";
        status.built = false;
        status.numServersCompleted = 0;
        status.totalServers = otpServer.internalUrl == null ? 0 : otpServer.internalUrl.size();
        // CONNECT TO EC2
        // FIXME Should this ec2 client be longlived?
        ec2 = AmazonEC2Client.builder().build();
//        imageId = ec2.describeImages(new DescribeImagesRequest().withOwners("099720109477").withFilters(new Filter("name", amiNameFilter), new Filter("state", amiStateFilter))).getImages().get(0).getImageId();
    }

    public void jobLogic () {
        if (otpServer.s3Bucket != null) totalTasks++;
        // FIXME
        if (otpServer.targetGroupArn != null) totalTasks++;
        try {
            deploymentTempFile = File.createTempFile("deployment", ".zip");
        } catch (IOException e) {
            statusMessage = "Could not create temp file for deployment";
            LOG.error(statusMessage);
            e.printStackTrace();
            status.fail(statusMessage);
            return;
        }

        LOG.info("Created deployment bundle file: " + deploymentTempFile.getAbsolutePath());

        // Dump the deployment bundle to the temp file.
        try {
            status.message = "Creating transit bundle (GTFS and OSM)";
            this.deployment.dump(deploymentTempFile, true, true, true);
            tasksCompleted++;
        } catch (Exception e) {
            statusMessage = "Error dumping deployment";
            LOG.error(statusMessage);
            e.printStackTrace();
            status.fail(statusMessage);
            return;
        }

        status.percentComplete = 100.0 * (double) tasksCompleted / totalTasks;
        LOG.info("Deployment pctComplete = {}", status.percentComplete);
        status.built = true;

        // Upload to S3, if specifically required by the OTPServer or needed for servers in the target group to fetch.
        if (otpServer.s3Bucket != null || otpServer.targetGroupArn != null) {
            if (!DataManager.useS3) {
                String message = "Cannot upload deployment to S3. Application not configured for s3 storage.";
                LOG.error(message);
                status.fail(message);
                return;
            }
            try {
                uploadBundleToS3();
            } catch (AmazonClientException | InterruptedException e) {
                statusMessage = String.format("Error uploading (or copying) deployment bundle to s3://%s", s3Bucket);
                LOG.error(statusMessage, e);
                status.fail(statusMessage);
            }

        }

        // Handle spinning up new EC2 servers for the load balancer's target group.
        if (otpServer.targetGroupArn != null) {
            if ("true".equals(DataManager.getConfigPropertyAsText("modules.deployment.ec2.enabled"))) {
                replaceEC2Servers();
                // If creating a new server, there is no need to deploy to an existing one.
                return;
            } else {
                String message = "Cannot complete deployment. EC2 deployment disabled in server configuration.";
                LOG.error(message);
                status.fail(message);
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

    private void uploadBundleToS3() throws InterruptedException, AmazonClientException {
        status.message = "Uploading to s3://" + s3Bucket;
        status.uploadingS3 = true;
        String key = getS3BundleKey();
        LOG.info("Uploading deployment {} to s3://{}/{}", deployment.name, s3Bucket, key);
        TransferManager tx = TransferManagerBuilder.standard().withS3Client(FeedStore.s3Client).build();
        final Upload upload = tx.upload(s3Bucket, key, deploymentTempFile);

        upload.addProgressListener(
            (ProgressListener) progressEvent -> status.percentUploaded = upload.getProgress().getPercentTransferred()
        );

        upload.waitForCompletion();

        // Shutdown the Transfer Manager, but don't shut down the underlying S3 client.
        // The default behavior for shutdownNow shut's down the underlying s3 client
        // which will cause any following s3 operations to fail.
        tx.shutdownNow(false);

        // copy to [name]-latest.zip
        String copyKey = getLatestS3BundleKey();
        CopyObjectRequest copyObjRequest = new CopyObjectRequest(s3Bucket, key, s3Bucket, copyKey);
        FeedStore.s3Client.copyObject(copyObjRequest);
        LOG.info("Copied to s3://{}/{}", s3Bucket, copyKey);
        LOG.info("Uploaded to s3://{}/{}", s3Bucket, getS3BundleKey());
        status.update("Upload to S3 complete.", status.percentComplete + 10);
        status.uploadingS3 = false;
    }

    private boolean buildGraphOverWire() {
        // figure out what router we're using
        String router = deployment.routerId != null ? deployment.routerId : "default";

        // Send the deployment file over the wire to each OTP server.
        for (String rawUrl : otpServer.internalUrl) {
            status.message = "Deploying to " + rawUrl;
            status.uploading = true;
            LOG.info(status.message);

            URL url;
            try {
                url = new URL(rawUrl + "/routers/" + router);
            } catch (MalformedURLException e) {
                statusMessage = String.format("Malformed deployment URL %s", rawUrl);
                LOG.error(statusMessage);

                // do not set percentComplete to 100 because we continue to the next server
                // TODO: should this return instead so that the job is cancelled?
                status.error = true;
                status.message = statusMessage;
                continue;
            }

            // grab them synchronously, so that we only take down one OTP server at a time
            HttpURLConnection conn;
            try {
                conn = (HttpURLConnection) url.openConnection();
            } catch (IOException e) {
                statusMessage = String.format("Unable to open URL of OTP server %s", url);
                LOG.error(statusMessage);

                // do not set percentComplete to 100 because we continue to the next server
                // TODO: should this return instead so that the job is cancelled?
                status.error = true;
                status.message = statusMessage;
                continue;
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
                statusMessage = String.format("Could not open channel to OTP server %s", url);
                LOG.error(statusMessage);
                e.printStackTrace();
                status.fail(statusMessage);
                return false;
            }

            // retrieveById the input file
            FileChannel input;
            try {
                input = new FileInputStream(deploymentTempFile).getChannel();
            } catch (FileNotFoundException e) {
                LOG.error("Internal error: could not read dumped deployment!");
                status.fail("Internal error: could not read dumped deployment!");
                return false;
            }

            try {
                conn.connect();
            } catch (IOException e) {
                statusMessage = String.format("Unable to open connection to OTP server %s", url);
                LOG.error(statusMessage);
                status.fail(statusMessage);
                return false;
            }

            // copy
            try {
                input.transferTo(0, Long.MAX_VALUE, post);
            } catch (IOException e) {
                statusMessage = String.format("Unable to transfer deployment to server %s", url);
                LOG.error(statusMessage);
                e.printStackTrace();
                status.fail(statusMessage);
                return false;
            }

            try {
                post.close();
            } catch (IOException e) {
                String message = String.format("Error finishing connection to server %s", url);
                LOG.error(message);
                e.printStackTrace();
                status.fail(message);
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
                    statusMessage = String.format("Got response code %d from server due to %s", code, response);
                    LOG.error(statusMessage);
                    status.fail(statusMessage);
                    // Skip deploying to any other servers.
                    // There is no reason to take out the rest of the servers, it's going to have the same result.
                    return false;
                }
            } catch (IOException e) {
                statusMessage = String.format("Could not finish request to server %s", url);
                LOG.error(statusMessage);
                status.fail(statusMessage);
            }

            status.numServersCompleted++;
            tasksCompleted++;
            status.percentComplete = 100.0 * (double) tasksCompleted / totalTasks;
        }
        return true;
    }

    private String getS3BundleKey() {
        return String.format("%s/%s/%s.zip", bundlePrefix, deployment.projectId, this.jobId);
    }

    private String getLatestS3BundleKey() {
        return String.format("%s/%s/%s-latest.zip", bundlePrefix, deployment.projectId, deployment.parentProject().name.toLowerCase());
    }

    @Override
    public void jobFinished () {
        // Delete temp file containing OTP deployment (OSM extract and GTFS files) so that the server's disk storage
        // does not fill up.
        boolean deleted = deploymentTempFile.delete();
        if (!deleted) {
            LOG.error("Deployment {} not deleted! Disk space in danger of filling up.", deployment.id);
        }
        String message;
        if (!status.error) {
            // Update status with successful completion state only if no error was encountered.
            status.update(false, "Deployment complete!", 100, true);
            // Store the target server in the deployedTo field.
            LOG.info("Updating deployment target to {} id={}", otpServer.id, deployment.id);
            Persistence.deployments.updateField(deployment.id, "deployedTo", otpServer.id);
            // Update last deployed field.
            Persistence.deployments.updateField(deployment.id, "lastDeployed", new Date());
            message = String.format("Deployment %s successfully deployed to %s", deployment.name, otpServer.publicUrl);
        } else {
            message = String.format("WARNING: Deployment %s failed to deploy to %s", deployment.name, otpServer.publicUrl);
        }
        // Send notification to those subscribed to updates for the deployment.
        NotifyUsersForSubscriptionJob.createNotification("deployment-updated", deployment.id, message);
    }

    public void replaceEC2Servers() {
        try {
            // First start graph-building instance and wait for graph to successfully build.
            List<Instance> instances = startEC2Instances(1);
            if (instances.size() > 1) {
                // FIXME is this check/shutdown entirely unnecessary?
                status.fail("CRITICAL: More than one server initialized for graph building. Cancelling job. Please contact system administrator.");
                // Terminate new instances.
                // FIXME Should this ec2 client be longlived?
                ec2.terminateInstances(new TerminateInstancesRequest(getIds(instances)));
            }
            // FIXME What if instances list is empty?
            MonitorServerStatusJob monitorInitialServerJob = new MonitorServerStatusJob(owner, deployment, instances.get(0), otpServer);
            monitorInitialServerJob.run();
            status.update("Graph build is complete!", 50);
            // Spin up remaining servers which will download the graph from S3.
            int remainingServerCount = otpServer.instanceCount <= 0 ? 0 : otpServer.instanceCount - 1;
            if (remainingServerCount > 0) {
                // Spin up remaining EC2 instances.
                List<Instance> remainingInstances = startEC2Instances(remainingServerCount);
                instances.addAll(remainingInstances);
                // Create new thread pool to monitor server setup so that the servers are monitored in parallel.
                ExecutorService service = Executors.newFixedThreadPool(remainingServerCount);
                for (Instance instance : remainingInstances) {
                    // Note: new instances are added
                    MonitorServerStatusJob monitorServerStatusJob = new MonitorServerStatusJob(owner, deployment, instance, otpServer);
                    service.submit(monitorServerStatusJob);
                }
                // Shutdown thread pool once the jobs are completed and wait for its termination. Once terminated, we can
                // consider the servers up and running (or they have failed to initialize properly).
                service.shutdown();
                service.awaitTermination(4, TimeUnit.HOURS);
            }
            String finalMessage = "Server setup is complete!";
            if (otpServer.instanceIds != null) {
                // Deregister old instances from load balancer. (Note: new instances are registered with load balancer in
                // MonitorServerStatusJob.)
                LOG.info("Deregistering instances from load balancer {}", otpServer.instanceIds);
                TargetDescription[] targetDescriptions = otpServer.instanceIds
                        .stream()
                        .map(id -> new TargetDescription().withId(id)).toArray(TargetDescription[]::new);
                DeregisterTargetsRequest deregisterTargetsRequest = new DeregisterTargetsRequest()
                        .withTargetGroupArn(otpServer.targetGroupArn)
                        .withTargets(targetDescriptions);
                DeregisterTargetsResult deregisterTargetsResult = com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingClient.builder().build()
                        .deregisterTargets(deregisterTargetsRequest);
                // Terminate old instances.
                LOG.info("Terminating instances {}", otpServer.instanceIds);
                try {
                    TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest().withInstanceIds(otpServer.instanceIds);
                    TerminateInstancesResult terminateInstancesResult = ec2.terminateInstances(terminateInstancesRequest);
                } catch (AmazonEC2Exception e) {
                    LOG.warn("Could not terminate EC2 instances {}", otpServer.instanceIds);
                    finalMessage = String.format("Server setup is complete! (WARNING: Could not terminate previous EC2 instances: %s", otpServer.instanceIds);
                }
            }
            // Update list of instance IDs with new list.
            Persistence.servers.updateField(otpServer.id, "instanceIds", getIds(instances));
            // Job is complete? FIXME Do we need a status check here?
            status.update(false, finalMessage, 100, true);
        } catch (Exception e) {
            LOG.error("Could not deploy to EC2 server", e);
            status.fail("Could not deploy to EC2 server", e);
        }
    }

    private List<Instance> startEC2Instances(int count) {
        // User data should contain info about:
        // 1. Downloading GTFS/OSM info (s3)
        // 2. Time to live until shutdown/termination (for test servers)
        // 3. Hosting / nginx
        // FIXME: Allow for r5 servers to be created.
        String userData = constructUserData(deployment.r5);
        // The subnet ID should only change if starting up a server in some other AWS account. This is not
        // likely to be a requirement.
        // Define network interface so that a public IP can be associated with server.
        InstanceNetworkInterfaceSpecification interfaceSpecification = new InstanceNetworkInterfaceSpecification()
                .withSubnetId(DataManager.getConfigPropertyAsText("modules.deployment.ec2.subnet"))
                .withAssociatePublicIpAddress(true)
                .withGroups(DataManager.getConfigPropertyAsText("modules.deployment.ec2.securityGroup"))
                .withDeviceIndex(0);

        RunInstancesRequest runInstancesRequest = new RunInstancesRequest()
                .withNetworkInterfaces(interfaceSpecification)
                .withInstanceType(otpServer.instanceType)
                .withMinCount(count)
                .withMaxCount(count)
                .withIamInstanceProfile(new IamInstanceProfileSpecification().withArn(DataManager.getConfigPropertyAsText("modules.deployment.ec2.arn")))
                .withImageId(DataManager.getConfigPropertyAsText("modules.deployment.ec2.ami"))
                .withKeyName(DataManager.getConfigPropertyAsText("modules.deployment.ec2.keyName"))
                // This will have the instance terminate when it is shut down.
                .withInstanceInitiatedShutdownBehavior("terminate")
                .withUserData(Base64.encodeBase64String(userData.getBytes()));
        final List<Instance> instances = ec2.runInstances(runInstancesRequest).getReservation().getInstances();

        List<String> instanceIds = getIds(instances);
        Map<String, String> instanceIpAddresses = new HashMap<>();
        // Wait so that create tags request does not fail because instances not found.
        try {
            Waiter<DescribeInstanceStatusRequest> waiter = ec2.waiters().instanceStatusOk();
//            ec2.waiters().systemStatusOk()
            long beginWaiting = System.currentTimeMillis();
            waiter.run(new WaiterParameters<>(new DescribeInstanceStatusRequest().withInstanceIds(instanceIds)));
            LOG.info("Instance status is OK after {} ms", (System.currentTimeMillis() - beginWaiting));
        } catch (Exception e) {
            LOG.error("Waiter for instance status check failed.", e);
            status.fail("Waiter for instance status check failed.");
            // FIXME: Terminate instance???
            return Collections.EMPTY_LIST;
        }
        for (Instance instance : instances) {
            // The public IP addresses will likely be null at this point because they take a few seconds to initialize.
            instanceIpAddresses.put(instance.getInstanceId(), instance.getPublicIpAddress());
            String serverName = String.format("%s %s (%s) %d", deployment.r5 ? "r5" : "otp", deployment.name, dateString, serverCounter++);
            LOG.info("Creating tags for new EC2 instance {}", serverName);
            ec2.createTags(new CreateTagsRequest()
                    .withTags(new Tag("Name", serverName))
                    .withTags(new Tag("projectId", deployment.projectId))
                    .withResources(instance.getInstanceId())
            );
        }
        List<Instance> updatedInstances = new ArrayList<>();
        while (instanceIpAddresses.values().contains(null)) {
            LOG.info("Checking that public IP addresses have initialized for EC2 instances.");
            // Reset instances list so that updated instances have the latest state information (e.g., public IP has
            // been assigned).
            updatedInstances.clear();
            // Check that all of the instances have public IPs.
            DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest().withInstanceIds(instanceIds);
            List<Reservation> reservations = ec2.describeInstances(describeInstancesRequest).getReservations();
            for (Reservation reservation  : reservations) {
                for (Instance instance : reservation.getInstances()) {
                    instanceIpAddresses.put(instance.getInstanceId(), instance.getPublicIpAddress());
                    updatedInstances.add(instance);
                }
            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        LOG.info("Public IP addresses have all been assigned. {}", instanceIpAddresses.values().toString());
        return updatedInstances;
    }

    private List<String> getIds (List<Instance> instances) {
        return instances.stream().map(Instance::getInstanceId).collect(Collectors.toList());
    }

    private String constructUserData(boolean r5) {
        // Prefix/name of JAR file (WITHOUT .jar)
        String jarName = r5 ? deployment.r5Version : deployment.otpVersion;
        if (jarName == null) {
            // If there is no version specified, use the default (and persist value).
            jarName = r5 ? DEFAULT_R5_VERSION : DEFAULT_OTP_VERSION;
            Persistence.deployments.updateField(deployment.id, r5 ? "r5Version" : "otpVersion", jarName);
        }
        String tripPlanner = r5 ? "r5" : "otp";
        String s3JarBucket = r5 ? "r5-builds" : "opentripplanner-builds";
        String s3JarUrl = String.format("https://%s.s3.amazonaws.com/%s.jar", s3JarBucket, jarName);
        // TODO Check that jar URL exists?
        String jarDir = String.format("/opt/%s", tripPlanner);
        String s3BundlePath = String.format("s3://%s/%s", s3Bucket, getS3BundleKey());
        boolean graphAlreadyBuilt = FeedStore.s3Client.doesObjectExist(s3Bucket, getS3GraphKey());
        List<String> lines = new ArrayList<>();
        String routerName = "default";
        String routerDir = String.format("/var/%s/graphs/%s", tripPlanner, routerName);
        // BEGIN USER DATA
        lines.add("#!/bin/bash");
        // Send trip planner logs to LOGFILE
        lines.add(String.format("BUILDLOGFILE=/var/log/%s-build.log", tripPlanner));
        lines.add(String.format("LOGFILE=/var/log/%s.log", tripPlanner));
        // Log user data setup to /var/log/user-data.log
        lines.add("exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1");
        // Create the directory for the graph inputs.
        lines.add(String.format("mkdir -p %s", routerDir));
        lines.add(String.format("chown ubuntu %s", routerDir));
        // Remove the current inputs and replace with inputs from S3.
        lines.add(String.format("rm -rf %s/*", routerDir));
        lines.add(String.format("aws s3 --region us-east-1 cp %s /tmp/bundle.zip", s3BundlePath));
        lines.add(String.format("unzip /tmp/bundle.zip -d %s", routerDir));
        // FIXME: Add ability to fetch custom bikeshare.xml file (CarFreeAtoZ)
        if (false) {
            lines.add(String.format("wget -O %s/bikeshare.xml ${config.bikeshareFeed}", routerDir));
            lines.add(String.format("printf \"{\\n  bikeRentalFile: \"bikeshare.xml\"\\n}\" >> %s/build-config.json\"", routerDir));
        }
        // Download trip planner JAR.
        lines.add(String.format("mkdir -p %s", jarDir));
        lines.add(String.format("wget %s -O %s/%s.jar", s3JarUrl, jarDir, jarName));
        if (graphAlreadyBuilt) {
            lines.add("echo 'downloading graph from s3'");
            // Download Graph from S3 and spin up trip planner.
            lines.add(String.format("aws s3 --region us-east-1 cp %s %s/Graph.obj", getS3GraphPath(), routerDir));
        } else {
            lines.add("echo 'starting graph build'");
            // Build the graph if Graph object (presumably this is the first instance to be started up).
            if (deployment.r5) lines.add(String.format("sudo -H -u ubuntu java -Xmx6G -jar %s/%s.jar point --build %s", jarDir, jarName, routerDir));
            else lines.add(String.format("sudo -H -u ubuntu java -jar %s/%s.jar --build %s > $BUILDLOGFILE 2>&1", jarDir, jarName, routerDir));
            // Upload the graph to S3.
            if (!deployment.r5) lines.add(String.format("aws s3 --region us-east-1 cp %s/Graph.obj %s", routerDir, getS3GraphPath()));
        }
        if (deployment.buildGraphOnly) {
            lines.add("echo 'shutting down server (build graph only specified in deployment target)'");
            lines.add("sudo poweroff");
        } else {
            lines.add("echo 'kicking off trip planner (logs at $LOGFILE)'");
            // Kick off the application.
            if (deployment.r5) lines.add(String.format("sudo -H -u ubuntu nohup java -Xmx6G -Djava.util.Arrays.useLegacyMergeSort=true -jar %s/%s.jar point --isochrones %s > /var/log/r5.out 2>&1&", jarDir, jarName, routerDir));
            else lines.add(String.format("sudo -H -u ubuntu nohup java -jar %s/%s.jar --server --bindAddress 127.0.0.1 --router default > $LOGFILE 2>&1 &", jarDir, jarName));
        }
        return String.join("\n", lines);
    }

    private String getS3GraphKey() {
        return String.format("%s/%s/Graph.obj", deployment.projectId, this.jobId);
    }

    private String getS3GraphPath() {
        return String.format("s3://%s/%s", otpServer.s3Bucket, getS3GraphKey());
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

        /** How many servers are we attempting to deploy to? */
        public int totalServers;

        /** Where can the user see the result? */
        public String baseUrl;

    }
}
