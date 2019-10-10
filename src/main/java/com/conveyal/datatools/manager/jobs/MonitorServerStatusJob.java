package com.conveyal.datatools.manager.jobs;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStateChange;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancing;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancingv2.model.RegisterTargetsRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetDescription;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.conveyal.datatools.manager.jobs.DeployJob.BUNDLE_DOWNLOAD_COMPLETE_FILE;
import static com.conveyal.datatools.manager.jobs.DeployJob.GRAPH_STATUS_FILE;

/**
 * Job that is dispatched during a {@link DeployJob} that spins up EC2 instances. This handles waiting for the server to
 * come online and for the OTP application/API to become available.
 */
public class MonitorServerStatusJob extends MonitorableJob {
    private static final Logger LOG = LoggerFactory.getLogger(MonitorServerStatusJob.class);
    private final DeployJob deployJob;
    private final Deployment deployment;
    private final Instance instance;
    private final boolean graphAlreadyBuilt;
    private final OtpServer otpServer;
    private final AmazonEC2 ec2 = AmazonEC2Client.builder().build();
    private final CloseableHttpClient httpClient = HttpClients.createDefault();
    // If the job takes longer than XX seconds, fail the job.
    private static final int TIMEOUT_MILLIS = 60 * 60 * 1000; // One hour
    private static final int DELAY_SECONDS = 5;
    private final long startTime;
    public long graphBuildSeconds;

    public MonitorServerStatusJob(Auth0UserProfile owner, DeployJob deployJob, Instance instance, boolean graphAlreadyBuilt) {
        super(
            owner,
            String.format("Monitor server setup %s", instance.getPublicIpAddress()),
            JobType.MONITOR_SERVER_STATUS
        );
        this.deployJob = deployJob;
        this.deployment = deployJob.getDeployment();
        this.otpServer = deployJob.getOtpServer();
        this.instance = instance;
        this.graphAlreadyBuilt = graphAlreadyBuilt;
        status.message = "Checking server status...";
        startTime = System.currentTimeMillis();
    }

    @JsonProperty
    public String getInstanceId () {
        return instance != null ? instance.getInstanceId() : null;
    }

    @JsonProperty
    public String getDeploymentId () {
        return deployJob.getDeploymentId();
    }

    @Override
    public void jobLogic() {
        String message;
        String ipUrl = "http://" + instance.getPublicIpAddress();
        // Get OTP URL for instance to check for availability.
        boolean routerIsAvailable = false, graphIsAvailable = false;
        // If graph was not already built by a previous server, wait for it to build.
        if (!graphAlreadyBuilt) {
            boolean bundleIsDownloaded = false;
            // Progressively check status of OTP server
            if (deployment.buildGraphOnly) {
                // No need to check that OTP is running. Just check to see that the graph is built.
                bundleIsDownloaded = true;
                routerIsAvailable = true;
            }
            // First, check that OTP has started up.
            status.update("Prepping for graph build...", 20);
            String bundleUrl = String.join("/", ipUrl, BUNDLE_DOWNLOAD_COMPLETE_FILE);
            long bundleDownloadStartTime = System.currentTimeMillis();
            while (!bundleIsDownloaded) {
                // If the request is successful, the OTP instance has started.
                wait("bundle download check:" + bundleUrl);
                bundleIsDownloaded = checkForSuccessfulRequest(bundleUrl);
                if (jobHasTimedOut()) {
                    status.fail(String.format("Job timed out while checking for server bundle download status (%s)", instance.getInstanceId()));
                    return;
                }
            }
            // Check status of bundle download and fail job if there was a failure.
            String bundleStatus = getUrlAsString(bundleUrl);
            if (bundleStatus == null || !bundleStatus.contains("SUCCESS")) {
                status.fail("Failure encountered while downloading transit bundle.");
                return;
            }
            long bundleDownloadSeconds = (System.currentTimeMillis() - bundleDownloadStartTime) / 1000;
            message = String.format("Bundle downloaded in %d seconds!", bundleDownloadSeconds);
            LOG.info(message);
            status.update("Building graph...", 30);
        }
        status.update("Loading graph...", 40);
        long graphBuildStartTime = System.currentTimeMillis();
        String graphStatusUrl = String.join("/", ipUrl, GRAPH_STATUS_FILE);
        while (!graphIsAvailable) {
            // If the request is successful, the OTP instance has started.
            wait("graph build/download check: " + graphStatusUrl);
            graphIsAvailable = checkForSuccessfulRequest(graphStatusUrl);
            if (jobHasTimedOut()) {
                message = String.format("Job timed out while waiting for graph build/download (%s)", instance.getInstanceId());
                LOG.error(message);
                status.fail(message);
                return;
            }
        }
        // Check status of bundle download and fail job if there was a failure.
        String graphStatus = getUrlAsString(graphStatusUrl);
        if (graphStatus == null || !graphStatus.contains("SUCCESS")) {
            message = String.format("Failure encountered while building/downloading graph (%s).", instance.getInstanceId());
            LOG.error(message);
            status.fail(message);
            return;
        }
        graphBuildSeconds = (System.currentTimeMillis() - graphBuildStartTime) / 1000;
        message = String.format("Graph build/download completed in %d seconds!", graphBuildSeconds);
        LOG.info(message);
        // If only task is to build graph, this machine's job is complete and we can consider this job done.
        if (deployment.buildGraphOnly) {
            status.update(false, message, 100);
            return;
        }
        // Once this is confirmed, check for the existence of the router, which will indicate that the graph build is
        // complete.
        String routerUrl = String.join("/", ipUrl, "otp/routers/default");
        while (!routerIsAvailable) {
            // If the request was successful, the graph build is complete!
            // TODO: Substitute in specific router ID? Or just default to... default.
            wait("router to become available: " + routerUrl);
            routerIsAvailable = checkForSuccessfulRequest(routerUrl);
            if (jobHasTimedOut()) {
                message = String.format("Job timed out while waiting for trip planner to start up (%s)", instance.getInstanceId());
                status.fail(message);
                LOG.error(message);
                return;
            }
        }
        status.update("Graph loaded!", 90);
        if (otpServer.ec2Info != null && otpServer.ec2Info.targetGroupArn != null) {
            // After the router is available, the EC2 instance can be registered with the load balancer.
            // REGISTER INSTANCE WITH LOAD BALANCER
            AmazonElasticLoadBalancing elbClient = AmazonElasticLoadBalancingClient.builder().build();
            RegisterTargetsRequest registerTargetsRequest = new RegisterTargetsRequest()
                    .withTargetGroupArn(otpServer.ec2Info.targetGroupArn)
                    .withTargets(new TargetDescription().withId(instance.getInstanceId()));
            elbClient.registerTargets(registerTargetsRequest);
            // FIXME how do we know it was successful?
            message = String.format("Server successfully registered with load balancer %s. OTP running at %s", otpServer.ec2Info.targetGroupArn, routerUrl);
            LOG.info(message);
            status.update(false, message, 100, true);
            deployJob.incrementCompletedServers();
        } else {
            message = String.format("There is no load balancer under which to register ec2 instance %s.", instance.getInstanceId());
            LOG.error(message);
            status.fail(message);
        }
    }

    /** Determine if job has passed time limit for its run time. */
    private boolean jobHasTimedOut() {
        long runTime = System.currentTimeMillis() - startTime;
        return runTime > TIMEOUT_MILLIS;
    }

    /**
     * Checks for Graph object on S3.
     */
    private boolean isGraphBuilt() {
        AmazonS3URI uri = new AmazonS3URI(deployJob.getS3GraphURI());
        LOG.info("Checking for graph at {}", uri.toString());
        // Surround with try/catch (exception thrown if object does not exist).
        try {
            return FeedStore.s3Client.doesObjectExist(uri.getBucket(), uri.getKey());
        } catch (AmazonS3Exception e) {
            LOG.warn("Object not found for key " + uri.getKey(), e);
            return false;
        }
    }

    /** Have the current thread sleep for a few seconds in order to pause during a while loop. */
    private void wait(String waitingFor) {
        try {
            LOG.info("Waiting {} seconds for {}", DELAY_SECONDS, waitingFor);
            Thread.sleep(1000 * DELAY_SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get the S3 key for the bundle status file, which is uploaded by the graph-building EC2 instance after the graph
     * build completes. The file contains either "SUCCESS" or "FAILURE".
     */
    private String getBundleStatusKey () {
        return String.join("/", deployJob.getJobRelativePath(), BUNDLE_DOWNLOAD_COMPLETE_FILE);
    }

    private String getUrlAsString(String url) {
        HttpGet httpGet = new HttpGet(url);
        try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
            return EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            LOG.error("Could not complete request to {}", url);
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Checks the provided URL for a successful response (i.e., HTTP status code is 200).
     */
    private boolean checkForSuccessfulRequest(String url) {
        HttpGet httpGet = new HttpGet(url);
        try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
            HttpEntity entity = response.getEntity();
            int statusCode = response.getStatusLine().getStatusCode();
            // Ensure the response body is fully consumed
            EntityUtils.consume(entity);
            return statusCode == 200;
        } catch (IOException e) {
            LOG.error("Could not complete request to {}", url);
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public void jobFinished() {
        if (status.error) {
            // Terminate server.
            TerminateInstancesResult terminateInstancesResult = ec2.terminateInstances(
                    new TerminateInstancesRequest().withInstanceIds(instance.getInstanceId())
            );
            InstanceStateChange instanceStateChange = terminateInstancesResult.getTerminatingInstances().get(0);
            // If instance state code is 48 that means it has been terminated.
            if (instanceStateChange.getCurrentState().getCode() == 48) {
                // FIXME: this message will not make it to the client because the status has already been failed. Also,
                //   I'm not sure if this is even the right way to handle the instance state check.
                status.update("Instance is terminated!", 100);
            }
        }
    }
}
