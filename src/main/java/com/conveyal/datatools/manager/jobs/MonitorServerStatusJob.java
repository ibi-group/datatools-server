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
import com.amazonaws.services.elasticloadbalancingv2.model.RegisterTargetsResult;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetDescription;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.OtpServer;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MonitorServerStatusJob extends MonitorableJob {
    private static final Logger LOG = LoggerFactory.getLogger(MonitorServerStatusJob.class);
    private final Deployment deployment;
    private final Instance instance;
    private final OtpServer otpServer;
    private final AmazonEC2 ec2 = AmazonEC2Client.builder().build();
    private final CloseableHttpClient httpClient = HttpClients.createDefault();
    // If the job takes longer than XX seconds, fail the job.
    private static final int TIMEOUT_MILLIS = 60 * 60 * 1000; // One hour

    public MonitorServerStatusJob(String owner, Deployment deployment, Instance instance, OtpServer otpServer) {
        super(
            owner,
            String.format("Monitor server setup %s", instance.getPublicIpAddress()),
            JobType.MONITOR_SERVER_STATUS
        );
        this.deployment = deployment;
        this.instance = instance;
        this.otpServer = otpServer;
        status.message = "Checking server status...";
    }

    @Override
    public void jobLogic() {
        long startTime = System.currentTimeMillis();
        // FIXME use private IP?
        String otpUrl = String.format("http://%s/otp", instance.getPublicIpAddress());
        boolean otpIsRunning = false, routerIsAvailable = false;
        // Progressively check status of OTP server
        if (deployment.buildGraphOnly) {
            // FIXME No need to check that OTP is running. Just check to see that the graph is built.
//            FeedStore.s3Client.doesObjectExist(otpServer.s3Bucket, );
        }
        // First, check that OTP has started up.
        status.update("Instance status is OK.", 20);
        while (!otpIsRunning) {
            LOG.info("Checking that OTP is running on server at {}.", otpUrl);
            // If the request is successful, the OTP instance has started.
            otpIsRunning = checkForSuccessfulRequest(otpUrl, 5);
            if (System.currentTimeMillis() - startTime > TIMEOUT_MILLIS) {
                status.fail(String.format("Job timed out while monitoring setup for server %s", instance.getInstanceId()));
                return;
            }
        }
        String otpMessage = String.format("OTP is running on server %s (%s). Building graph...", instance.getInstanceId(), otpUrl);
        LOG.info(otpMessage);
        status.update(otpMessage, 30);
        // Once this is confirmed, check for the existence of the router, which will indicate that the graph build is
        // complete.
        String routerUrl = String.format("%s/routers/default", otpUrl);
        while (!routerIsAvailable) {
            LOG.info("Checking that router is available (i.e., graph build or read is finished) at {}", routerUrl);
            // If the request was successful, the graph build is complete!
            // TODO: Substitute in specific router ID? Or just default to... default.
            routerIsAvailable = checkForSuccessfulRequest(routerUrl, 5);
        }
        status.update(String.format("Graph build completed on server %s (%s).", instance.getInstanceId(), routerUrl), 90);
        if (otpServer.targetGroupArn != null) {
            // After the router is available, the EC2 instance can be registered with the load balancer.
            // REGISTER INSTANCE WITH LOAD BALANCER
            AmazonElasticLoadBalancing elbClient = AmazonElasticLoadBalancingClient.builder().build();
            RegisterTargetsRequest registerTargetsRequest = new RegisterTargetsRequest()
                    .withTargetGroupArn(otpServer.targetGroupArn)
                    .withTargets(new TargetDescription().withId(instance.getInstanceId()));
            RegisterTargetsResult registerTargetsResult = elbClient.registerTargets(registerTargetsRequest);
//            try {
//                elbClient.waiters().targetInService().run(new WaiterParameters<>(new DescribeTargetHealthRequest().withTargetGroupArn(otpServer.targetGroupArn)));
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
            // FIXME how do we know it was successful?
            String message = String.format("Server %s successfully registered with load balancer %s", instance.getInstanceId(), otpServer.targetGroupArn);
            LOG.info(message);
            status.update(false, message, 100, true);
        } else {
            LOG.info("There is no load balancer under which to register ec2 instance {}.", instance.getInstanceId());
        }
    }

    /**
     * Checks the provided URL for a successful response (i.e., HTTP status code is 200).
     */
    private boolean checkForSuccessfulRequest(String url, int delaySeconds) {
        // Wait for the specified seconds before the request is initiated.
        try {
            Thread.sleep(1000 * delaySeconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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
