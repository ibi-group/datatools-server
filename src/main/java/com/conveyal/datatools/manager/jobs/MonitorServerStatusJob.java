package com.conveyal.datatools.manager.jobs;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStateChange;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancing;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingClientBuilder;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeTargetHealthRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeTargetHealthResult;
import com.amazonaws.services.elasticloadbalancingv2.model.RegisterTargetsRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetDescription;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetHealthDescription;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.common.utils.AWSUtils;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.OtpServer;
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
import java.util.Collections;

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
    private final AWSStaticCredentialsProvider credentials;
    private final AmazonEC2 ec2;
    private final AmazonElasticLoadBalancing elbClient;
    private final CloseableHttpClient httpClient = HttpClients.createDefault();
    // Delay checks by twenty seconds to give user-data script time to upload the instance's user data log if part of the
    // script fails (e.g., uploading or downloading a file).
    private static final int DELAY_SECONDS = 20;
    public long graphTaskSeconds;

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
        credentials = AWSUtils.getCredentialsForRole(otpServer.role, "monitor-" + instance.getInstanceId());
        ec2 = deployJob.getCustomRegion() == null
            ? AWSUtils.getEC2ClientForCredentials(credentials)
            : AWSUtils.getEC2ClientForCredentials(credentials, deployJob.getCustomRegion());
        AmazonElasticLoadBalancingClientBuilder elbBuilder = AmazonElasticLoadBalancingClient.builder()
            .withCredentials(credentials);
        elbClient = deployJob.getCustomRegion() == null
            ? elbBuilder.build()
            : elbBuilder.withRegion(deployJob.getCustomRegion()).build();
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
        String ipUrl = "http://" + instance.getPublicIpAddress();
        // Get OTP URL for instance to check for availability.
        boolean routerIsAvailable = false, graphIsAvailable = false;
        if (otpServer.ec2Info == null || otpServer.ec2Info.targetGroupArn == null) {
            // Fail the job from the outset if there is no target group defined.
            failJob("There is no load balancer under which to register ec2 instance.");
        }
        try {
            if (graphAlreadyBuilt) {
                // If graph already build, instance's user data will download Graph.obj automatically instead of bundle.
                status.update("Loading graph...", 40);
            } else {
                // Otherwise, we need to verify that the bundle downloaded successfully.
                boolean bundleIsDownloaded = false;
                // Progressively check status of OTP server
                if (deployment.buildGraphOnly) {
                    // No need to check that OTP is running. Just check to see that the graph is built.
                    routerIsAvailable = true;
                }
                // First, check that OTP has started up.
                status.update("Prepping for graph build...", 20);
                String bundleUrl = String.join("/", ipUrl, BUNDLE_DOWNLOAD_COMPLETE_FILE);
                long bundleDownloadStartTime = System.currentTimeMillis();
                while (!bundleIsDownloaded) {
                    // If the request is successful, the OTP instance has started.
                    waitAndCheckInstanceHealth("bundle download check: " + bundleUrl);
                    bundleIsDownloaded = checkForSuccessfulRequest(bundleUrl);
                    // wait 20 minutes max for the bundle to download
                    long maxBundleDownloadTimeMillis = 20 * 60 * 1000;
                    if (taskHasTimedOut(bundleDownloadStartTime, maxBundleDownloadTimeMillis)) {
                        failJob("Job timed out while checking for server bundle download status.");
                        return;
                    }
                }
                // Check status of bundle download and fail job if there was a failure.
                String bundleStatus = getUrlAsString(bundleUrl);
                if (bundleStatus == null || !bundleStatus.contains("SUCCESS")) {
                    failJob("Failure encountered while downloading transit bundle.");
                    return;
                }
                long bundleDownloadSeconds = (System.currentTimeMillis() - bundleDownloadStartTime) / 1000;
                LOG.info("Bundle downloaded in {} seconds!", bundleDownloadSeconds);
                status.update("Building graph...", 30);
            }
            // Once bundle is downloaded, we await the build (or download if graph already built) of the graph.
            long graphCheckStartTime = System.currentTimeMillis();
            String graphStatusUrl = String.join("/", ipUrl, GRAPH_STATUS_FILE);
            while (!graphIsAvailable) {
                // If the request is successful, the OTP instance has started.
                waitAndCheckInstanceHealth("graph build/download check: " + graphStatusUrl);
                graphIsAvailable = checkForSuccessfulRequest(graphStatusUrl);
                // wait a maximum of 4 hours if building the graph, or 20 minutes if downloading a graph
                long maxGraphBuildOrDownloadWaitTimeMillis = graphAlreadyBuilt ? 20 * 60 * 1000 : 4 * 60 * 60 * 1000;
                if (taskHasTimedOut(graphCheckStartTime, maxGraphBuildOrDownloadWaitTimeMillis)) {
                    failJob("Job timed out while waiting for graph build/download. If this was a graph building machine, it may have run out of memory.");
                    return;
                }
            }
            // Check graph status and fail job if there was a failure.
            String graphStatus = getUrlAsString(graphStatusUrl);
            if (graphStatus == null || !graphStatus.contains("SUCCESS")) {
                failJob("Failure encountered while building/downloading graph.");
                return;
            }
            graphTaskSeconds = (System.currentTimeMillis() - graphCheckStartTime) / 1000;
            String message = String.format("Graph build/download completed in %d seconds!", graphTaskSeconds);
            LOG.info(message);
            // If only task for this instance is to build the graph (either because that is the deployment purpose or
            // because this instance type/image is for graph building only), this machine's job is complete and we can
            // consider this job done.
            if (deployment.buildGraphOnly || (!graphAlreadyBuilt && otpServer.ec2Info.hasSeparateGraphBuildConfig())) {
                status.completeSuccessfully(message);
                LOG.info("View logs at {}", getUserDataLogS3Path());
                return;
            }
            status.update("Loading graph...", 70);
            // Once this is confirmed, check for the availability of the router, which will indicate that the graph
            // load has completed successfully.
            String routerUrl = String.join("/", ipUrl, "otp/routers/default");
            long routerCheckStartTime = System.currentTimeMillis();
            while (!routerIsAvailable) {
                // If the request was successful, the graph build is complete!
                // TODO: Substitute in specific router ID? Or just default to... "default".
                waitAndCheckInstanceHealth("router to become available: " + routerUrl);
                routerIsAvailable = checkForSuccessfulRequest(routerUrl);
                // wait a maximum of 20 minutes to load the graph and for the router to become available.
                long maxRouterAvailableWaitTimeMillis = 20 * 60 * 1000;
                if (taskHasTimedOut(routerCheckStartTime, maxRouterAvailableWaitTimeMillis)) {
                    failJob("Job timed out while waiting for trip planner to start up.");
                    return;
                }
            }
            status.update("Graph loaded!", 90);
            // After the router is available, the EC2 instance can be registered with the load balancer.
            // REGISTER INSTANCE WITH LOAD BALANCER
            RegisterTargetsRequest registerTargetsRequest = new RegisterTargetsRequest()
                .withTargetGroupArn(otpServer.ec2Info.targetGroupArn)
                .withTargets(new TargetDescription().withId(instance.getInstanceId()));
            boolean targetAddedSuccessfully = false;
            long registerTargetStartTime = System.currentTimeMillis();
            while (!targetAddedSuccessfully) {
                // Register target with target group.
                elbClient.registerTargets(registerTargetsRequest);
                waitAndCheckInstanceHealth("instance to register with ELB target group");
                // Check that the instance ID shows up in the health check.
                DescribeTargetHealthRequest healthRequest = new DescribeTargetHealthRequest()
                    .withTargetGroupArn(otpServer.ec2Info.targetGroupArn);
                DescribeTargetHealthResult healthResult = elbClient.describeTargetHealth(healthRequest);
                for (TargetHealthDescription health : healthResult.getTargetHealthDescriptions()) {
                    if (instance.getInstanceId().equals(health.getTarget().getId())) {
                        LOG.info("Instance {} successfully added to target group!", instance.getInstanceId());
                        targetAddedSuccessfully = true;
                    }
                }
                // Wait for two minutes.
                if (taskHasTimedOut(registerTargetStartTime, 2 * 60 * 1000)) {
                    failJob("Job timed out while waiting to register EC2 instance with load balancer target group.");
                    return;
                }
            }
            status.completeSuccessfully(
                String.format(
                    "Server successfully registered with load balancer %s. OTP running at %s",
                    otpServer.ec2Info.targetGroupArn,
                    routerUrl
                )
            );
            LOG.info("View logs at {}", getUserDataLogS3Path());
            deployJob.incrementCompletedServers();
        } catch (InstanceHealthException e) {
            // If at any point during the job, an instance health check indicates that the EC2 instance being monitored
            // was terminated or stopped, an InstanceHealthException will be thrown. Whether the instance termination
            // was accidental or intentional, we want the result to be that the job fails and the deployment be aborted.
            // This gives us a failsafe in case we kick off a deployment accidentally or otherwise need to cancel the
            // deployment job (e.g., due to an incorrect configuration).
            failJob("Ec2 Instance was stopped or terminated before job could complete!");
        }
    }

    /**
     * Gets the expected path to the user data logs that get uploaded to s3
     */
    private String getUserDataLogS3Path() {
        return String.format("%s/%s.log", deployJob.getS3FolderURI(), instance.getInstanceId());
    }

    /**
     * Helper that fails with a helpful message about where to find uploaded logs.
     */
    private void failJob(String message) {
        LOG.error(message);
        status.fail(String.format("%s Check logs at: %s", message, getUserDataLogS3Path()));
    }

    /** Determine if a specific task has passed time limit for its run time. */
    private boolean taskHasTimedOut(long startTime, long maxRunTimeMillis) {
        long runTimeMillis = System.currentTimeMillis() - startTime;
        return runTimeMillis > maxRunTimeMillis;
    }

    /**
     * Have the current thread sleep for a few seconds in order to pause during a while loop. Also, before and after
     * waiting, check the instance health to make sure it is still running. If a user has terminated the instance, the
     * job should be failed.
     */
    private void waitAndCheckInstanceHealth(String waitingFor) throws InstanceHealthException {
        checkInstanceHealth();
        try {
            LOG.info("Waiting {} seconds for {}", DELAY_SECONDS, waitingFor);
            Thread.sleep(1000 * DELAY_SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        checkInstanceHealth();
    }

    /**
     * Checks whether the instance is running. If it has entered a state where it is stopped, terminated or about to be
     * stopped or terminated, then this method throws an exception.
     */
    private void checkInstanceHealth() throws InstanceHealthException {
        DescribeInstancesRequest request = new DescribeInstancesRequest()
            .withInstanceIds(Collections.singletonList(instance.getInstanceId()));
        DescribeInstancesResult result = ec2.describeInstances(request);
        for (Reservation reservation : result.getReservations()) {
            for (Instance reservationInstance : reservation.getInstances()) {
                if (reservationInstance.getInstanceId().equals(instance.getInstanceId())) {
                    // Code 16 is running. Anything above that is either stopped, terminated or about to be stopped or
                    // terminated
                    if (reservationInstance.getState().getCode() > 16) {
                        throw new InstanceHealthException(reservationInstance.getState().getName());
                    }
                }
            }
        }
    }

    /**
     * An exception for when the ec2 Instance has entered a state where it is no longer running.
     */
    private class InstanceHealthException extends Exception {
        public InstanceHealthException(String instanceStateName) {
            super(String.format("Instance state no longer healthy! It changed to: %s", instanceStateName));
        }
    }

    /** Make HTTP request to URL and return the string response. */
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
