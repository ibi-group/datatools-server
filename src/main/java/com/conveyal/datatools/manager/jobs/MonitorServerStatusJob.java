package com.conveyal.datatools.manager.jobs;

import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancing;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeTargetHealthRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeTargetHealthResult;
import com.amazonaws.services.elasticloadbalancingv2.model.RegisterTargetsRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetDescription;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetHealthDescription;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.utils.ErrorUtils;
import com.conveyal.datatools.manager.utils.SimpleHttpResponse;
import com.conveyal.datatools.manager.utils.TimeTracker;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.conveyal.datatools.manager.jobs.DeployJob.OTP_RUNNER_STATUS_FILE;

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
    private final CloseableHttpClient httpClient = HttpClients.createDefault();
    // Delay checks by four seconds to give user-data script time to upload the instance's user data log if part of the
    // script fails (e.g., uploading or downloading a file).
    private static final int DELAY_SECONDS = 4;
    private static final int MAX_INSTANCE_HEALTH_RETRIES = 5;

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
        // Get OTP URL for instance to check for availability.
        String ipUrl = "http://" + instance.getPublicIpAddress();
        if (otpServer.ec2Info == null || otpServer.ec2Info.targetGroupArn == null) {
            // Fail the job from the outset if there is no target group defined.
            failJob("There is no load balancer under which to register ec2 instance.");
        }
        try {
            // Wait for otp-runner to produce first status file
            TimeTracker otpRunnerStatusAvailableTracker = new TimeTracker(5, TimeUnit.MINUTES);
            String statusUrl = String.join("/", ipUrl, OTP_RUNNER_STATUS_FILE);
            boolean otpRunnerStatusAvailable = false;
            while (!otpRunnerStatusAvailable) {
                // If the request is successful, the OTP instance has started.
                waitAndCheckInstanceHealth("otp-runner status file availability check: " + statusUrl);
                otpRunnerStatusAvailable = checkForSuccessfulRequest(statusUrl);
                if (otpRunnerStatusAvailableTracker.hasTimedOut()) {
                    failJob("Job timed out while waiting for otp-runner to produce a status file!");
                    return;
                }
            }
            // Wait for otp-runner to write a status that fulfills expectations of this job. Wait a maximum of 5 hours
            // if building a graph, or 1 hour if starting a server-only instance.
            TimeTracker otpRunnerCompletionTracker = new TimeTracker(graphAlreadyBuilt ? 1 : 5, TimeUnit.HOURS);
            boolean otpRunnerCompleted = false;
            while (!otpRunnerCompleted) {
                waitAndCheckInstanceHealth("otp-runner completion check: " + statusUrl);
                // Analyze the contents of the otp-runner status file to see if the job is complete
                otpRunnerCompleted = checkForOtpRunnerCompletion(statusUrl);
                // Check if an otp-runner status file check has already failed this job.
                if (status.error) {
                    return;
                }
                if (otpRunnerCompletionTracker.hasTimedOut()) {
                    failJob("Job timed out while waiting for otp-runner to finish!");
                    return;
                }
            }
            String message = String.format(
                "Graph build/download completed in %d seconds!",
                otpRunnerStatusAvailableTracker.elapsedSeconds()
            );
            LOG.info(message);
            // If only task for this instance is to build the graph (either because that is the deployment purpose or
            // because this instance type/image is for graph building only), this machine's job is complete and we can
            // consider this job done.
            if (isBuildOnlyServer()) {
                status.completeSuccessfully(message);
                LOG.info("View logs at {}", getOtpRunnerLogS3Path());
                return;
            }
            // Once this is confirmed, check for the availability of the router, which will indicate that the graph
            // load has completed successfully. Wait a maximum of 20 minutes to load the graph and for the router to
            // become available.
            TimeTracker routerCheckTracker = new TimeTracker(20, TimeUnit.MINUTES);
            String routerUrl = String.join("/", ipUrl, "otp/routers/default");
            boolean routerIsAvailable = false;
            while (!routerIsAvailable) {
                // If the request was successful, the graph build is complete!
                // TODO: Substitute in specific router ID? Or just default to... "default".
                waitAndCheckInstanceHealth("router to become available: " + routerUrl);
                routerIsAvailable = checkForSuccessfulRequest(routerUrl);
                if (routerCheckTracker.hasTimedOut()) {
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
            // Wait for two minutes for targets to register.
            TimeTracker registerTargetTracker = new TimeTracker(2, TimeUnit.MINUTES);
            // obtain an ELB client suitable for this deploy job. It is important to obtain a client this way to ensure
            // that the proper AWS credentials are used and that the client has a valid session if it is obtained from a
            // role.
            AmazonElasticLoadBalancing elbClient = deployJob.getELBClientForDeployJob();
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
                if (registerTargetTracker.hasTimedOut()) {
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
            LOG.info("View logs at {}", getOtpRunnerLogS3Path());
            deployJob.incrementCompletedServers();
        } catch (InstanceHealthException e) {
            // If at any point during the job, an instance health check indicates that the EC2 instance being monitored
            // was terminated or stopped, an InstanceHealthException will be thrown. Whether the instance termination
            // was accidental or intentional, we want the result to be that the job fails and the deployment be aborted.
            // This gives us a failsafe in case we kick off a deployment accidentally or otherwise need to cancel the
            // deployment job (e.g., due to an incorrect configuration).
            failJob("EC2 Instance was stopped or terminated before job could complete!", e);
        } catch (Exception e) {
            // This catch-all block is needed to make sure any exceptions that are not handled elsewhere are properly
            // caught here so that the job can be properly failed. If the job is not failed properly this could result
            // in hanging instances that do not get terminated properly by the parent DeployJob.
            failJob("An internal datatools error occurred before the job could complete!", e);
        }
    }

    private boolean isBuildOnlyServer() {
        return deployment.buildGraphOnly || (!graphAlreadyBuilt && otpServer.ec2Info.hasSeparateGraphBuildConfig());
    }

    /**
     * Gets the expected path to the otp-runner logs that get uploaded to s3
     */
    private String getOtpRunnerLogS3Path() {
        return String.format("%s/%s-otp-runner.log", deployJob.getS3FolderURI(), instance.getInstanceId());
    }

    /**
     * Helper for marking the job as failed with just a message.
     */
    private void failJob(String message) {
        failJob(message, null);
    }

    /**
     * Helper that fails with a message and optional exception. A helpful message about where to find uploaded logs is
     * appended to the failure message.
     */
    private void failJob(String message, Exception e) {
        if (e == null) {
            LOG.error(message);
        } else {
            LOG.error(message, e);
        }
        status.fail(String.format("%s Check logs at: %s", message, getOtpRunnerLogS3Path()), e);
    }

    /**
     * Have the current thread sleep for a few seconds in order to pause during a while loop. Also, before and after
     * waiting, check the instance health to make sure it is still running. If a user has terminated the instance, the
     * job should be failed.
     */
    private void waitAndCheckInstanceHealth(String waitingFor) throws InstanceHealthException, InterruptedException {
        LOG.info("Waiting {} seconds for {}", DELAY_SECONDS, waitingFor);
        Thread.sleep(1000 * DELAY_SECONDS);
        checkInstanceHealth(1);
    }

    /**
     * Checks whether the instance is running. If it has entered a state where it is stopped, terminated or about to be
     * stopped or terminated, then this method throws an exception. It is possible that some describe instance requests
     * might fail either during instance startup or due to brief network connectivity issues, so this method will retry
     * checking the instance health using recursion up to the
     * ${@link MonitorServerStatusJob#MAX_INSTANCE_HEALTH_RETRIES} value.
     */
    private void checkInstanceHealth(int attemptNumber) throws InstanceHealthException, InterruptedException {
        DescribeInstancesRequest request = new DescribeInstancesRequest()
            .withInstanceIds(Collections.singletonList(instance.getInstanceId()));
        DescribeInstancesResult result;
        try {
            result = deployJob.getEC2ClientForDeployJob().describeInstances(request);
        } catch (Exception e) {
            LOG.warn(
                "Failed on attempt {}/{} to execute request to obtain instance health!",
                attemptNumber,
                MAX_INSTANCE_HEALTH_RETRIES,
                e
            );
            if (attemptNumber > MAX_INSTANCE_HEALTH_RETRIES) {
                throw new InstanceHealthException("AWS Describe Instances error!");
            }
            LOG.info("Waiting 5 seconds to try to get instance status again");
            Thread.sleep(5000);
            checkInstanceHealth(attemptNumber + 1);
            return;
        }
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

    /**
     * Checks the status of otp-runner and returns whether otp-runner has completed (either with success or failure).
     * The overall job is updated with the percent progress and message from the otp-runner status file as well. This
     * will make a request to the given URL to retrieve an otp-runner JSON status file. If the otp-runner status file
     * indicates that an error occurred, this method will fail the job.
     *
     * @param url The URL to retrieve the otp-runner status file from
     * @return true if otp-runner has completed all necessary tasks
     */
    private boolean checkForOtpRunnerCompletion(String url) {
        // make request to otp-runner
        HttpGet httpGet = new HttpGet(url);
        OtpRunnerStatus otpRunnerStatus;
        try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
            otpRunnerStatus = JsonUtil.getPOJOFromResponse(new SimpleHttpResponse(response), OtpRunnerStatus.class);
        } catch (IOException e) {
            LOG.warn("Could not get otp-runner status from {}. It might not be available yet.", url);
            return false;
        }

        // make sure otp-runner status file contains a matching nonce. Sometimes a otp-runner status from a previous
        // deploy job could be present, so this makes sure that the otp-runner status file applies to this particular
        // deploy job
        if (otpRunnerStatus.nonce == null || !otpRunnerStatus.nonce.equals(deployJob.getNonce())) {
            LOG.warn("otp-runner status nonce does not match deployment nonce");
            return false;
        }

        // if the otp-runner status file contains an error message, fail the job
        if (otpRunnerStatus.error) {
            // report to bugsnag if configured
            ErrorUtils.reportToBugsnag(
                new RuntimeException("otp-runner reported an error"),
                "otp-runner",
                otpRunnerStatus.message,
                owner
            );
            failJob(otpRunnerStatus.message);
            return false;
        }

        // update overall job status and percentage with the status and percentage found in the otp-runner status file
        status.update(otpRunnerStatus.message, otpRunnerStatus.pctProgress);

        // return completion based off of whether this instance is a graph-build only instance or is one that is
        // exepcted to run an OTP server
        if (graphAlreadyBuilt || !isBuildOnlyServer()) {
            // A successful completion is after the OTP server is successfully started
            return otpRunnerStatus.serverStarted;
        } else {
            // A successful completion is after the graph is uploaded
            return otpRunnerStatus.graphUploaded;
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
        // jobs that failed will have their associated instances terminated in the parent deploy job
    }
}
