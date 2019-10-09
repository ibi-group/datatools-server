package com.conveyal.datatools.manager.controllers.api;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AmazonEC2Exception;
import com.amazonaws.services.ec2.model.DescribeImagesRequest;
import com.amazonaws.services.ec2.model.DescribeImagesResult;
import com.amazonaws.services.ec2.model.DescribeKeyPairsResult;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.DescribeSubnetsRequest;
import com.amazonaws.services.ec2.model.DescribeSubnetsResult;
import com.amazonaws.services.ec2.model.Image;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.KeyPairInfo;
import com.amazonaws.services.ec2.model.SecurityGroup;
import com.amazonaws.services.ec2.model.Subnet;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancing;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancingv2.model.AmazonElasticLoadBalancingException;
import com.amazonaws.services.elasticloadbalancingv2.model.DeregisterTargetsRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeTargetGroupsRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetDescription;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetGroup;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.identitymanagement.model.InstanceProfile;
import com.amazonaws.services.identitymanagement.model.ListInstanceProfilesResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.MonitorServerStatusJob;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.HaltException;
import spark.Request;
import spark.Response;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;
import static com.conveyal.datatools.manager.jobs.DeployJob.DEFAULT_INSTANCE_TYPE;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.options;
import static spark.Spark.post;
import static spark.Spark.put;

/**
 * Handlers for HTTP API requests that affect deployment Servers.
 * These methods are mapped to API endpoints by Spark.
 */
public class ServerController {
    private static JsonManager<OtpServer> json = new JsonManager<>(OtpServer.class, JsonViews.UserInterface.class);
    private static final Logger LOG = LoggerFactory.getLogger(ServerController.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final AmazonEC2 ec2 = AmazonEC2Client.builder().build();
    private static final AmazonIdentityManagement iam = AmazonIdentityManagementClientBuilder.defaultClient();
    private static final AmazonElasticLoadBalancing elb = AmazonElasticLoadBalancingClient.builder().build();

    /**
     * Gets the server specified by the request's id parameter and ensure that user has access to the
     * deployment. If the user does not have permission the Spark request is halted with an error.
     */
    private static OtpServer getServerWithPermissions(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        String serverId = req.params("id");
        OtpServer server = Persistence.servers.getById(serverId);
        if (server == null) {
            logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Server does not exist.");
        }
        boolean isProjectAdmin = userProfile.canAdministerProject(server.projectId, server.organizationId());
        if (!isProjectAdmin && !userProfile.getUser_id().equals(server.user())) {
            // If user is not a project admin and did not create the deployment, access to the deployment is denied.
            logMessageAndHalt(req, HttpStatus.UNAUTHORIZED_401, "User not authorized for deployment.");
        }
        return server;
    }

    /** HTTP endpoint for deleting an {@link OtpServer}. */
    private static OtpServer deleteServer(Request req, Response res) {
        OtpServer server = getServerWithPermissions(req, res);
        // Ensure that there are no active EC2 instances associated with server. Halt deletion if so.
        List<Instance> activeInstances = server.retrieveEC2Instances().stream()
            .filter(instance -> "running".equals(instance.getState().getName()))
            .collect(Collectors.toList());
        if (activeInstances.size() > 0) {
            logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Cannot delete server with active EC2 instances: " + getIds(activeInstances));
        }
        server.delete();
        return server;
    }

    /** HTTP method for terminating EC2 instances associated with an ELB OTP server. */
    private static OtpServer terminateEC2InstancesForServer(Request req, Response res) {
        OtpServer server = getServerWithPermissions(req, res);
        List<Instance> instances = server.retrieveEC2Instances();
        List<String> ids = getIds(instances);
        terminateInstances(ids);
        for (Deployment deployment : Deployment.retrieveDeploymentForServerAndRouterId(server.id, null)) {
            Persistence.deployments.updateField(deployment.id, "deployedTo", null);
        }
        return server;
    }

    /**
     * Shorthand method for getting list of string identifiers from a list of EC2 instances.
     */
    public static List<String> getIds (List<Instance> instances) {
        return instances.stream().map(Instance::getInstanceId).collect(Collectors.toList());
    }

    /** Terminate the list of EC2 instance IDs. */
    public static TerminateInstancesResult terminateInstances(Collection<String> instanceIds) throws AmazonEC2Exception {
        if (instanceIds.size() == 0) {
            LOG.warn("No instance IDs provided in list. Skipping termination request.");
            return null;
        }
        LOG.info("Terminating EC2 instances {}", instanceIds);
        TerminateInstancesRequest request = new TerminateInstancesRequest().withInstanceIds(instanceIds);
        return ec2.terminateInstances(request);
    }

    /** Convenience method to override {@link #terminateInstances(Collection)}. */
    public static TerminateInstancesResult terminateInstances(String... instanceIds) throws AmazonEC2Exception {
        return terminateInstances(Arrays.asList(instanceIds));
    }

    /** Convenience method to override {@link #terminateInstances(Collection)}. */
    public static TerminateInstancesResult terminateInstances(List<Instance> instances) throws AmazonEC2Exception {
        return terminateInstances(getIds(instances));
    }

    /**
     * De-register instances from the specified target group/load balancer and terminate the instances.
     *
     */
    public static boolean deRegisterAndTerminateInstances(String targetGroupArn, List<String> instanceIds) {
        LOG.info("De-registering instances from load balancer {}", instanceIds);
        TargetDescription[] targetDescriptions = instanceIds.stream()
            .map(id -> new TargetDescription().withId(id))
            .toArray(TargetDescription[]::new);
        try {
            DeregisterTargetsRequest request = new DeregisterTargetsRequest()
                .withTargetGroupArn(targetGroupArn)
                .withTargets(targetDescriptions);
            AmazonElasticLoadBalancing elb = AmazonElasticLoadBalancingClient.builder().build();
            elb.deregisterTargets(request);
            ServerController.terminateInstances(instanceIds);
        } catch (AmazonEC2Exception | AmazonElasticLoadBalancingException e) {
            LOG.warn("Could not terminate EC2 instances: " + String.join(",", instanceIds), e);
            return false;
        }
        return true;
    }

    /**
     * Create a new server for the project. All feed sources with a valid latest version are added to the new
     * deployment.
     */
    private static OtpServer createServer(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        OtpServer newServer = getServerFromRequestBody(req);
        // If server has no project ID specified, user must be an application admin to create it. Otherwise, they must
        // be a project admin.
        boolean allowedToCreate = newServer.projectId == null
            ? userProfile.canAdministerApplication()
            : userProfile.canAdministerProject(newServer.projectId, newServer.organizationId());
        if (allowedToCreate) {
            validateFields(req, newServer);
            Persistence.servers.create(newServer);
            return newServer;
        } else {
            logMessageAndHalt(req, 403, "Not authorized to create a server for project " + newServer.projectId);
            return null;
        }
    }

    /** Utility method to parse OtpServer object from Spark request body. */
    private static OtpServer getServerFromRequestBody(Request req) {
        try {
            return mapper.readValue(req.body(), OtpServer.class);
        } catch (IOException e) {
            logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Error parsing OTP server JSON.", e);
            return null;
        }
    }

    /**
     * HTTP controller to fetch all servers or servers assigned to a particular project. This should only be used for the
     * management of these servers. For checking servers that a project can deploy to, use {@link Project#availableOtpServers()}.
     */
    private static List<OtpServer> fetchServers (Request req, Response res) {
        String projectId = req.queryParams("projectId");
        Auth0UserProfile userProfile = req.attribute("user");
        if (projectId != null) {
            Project project = Persistence.projects.getById(projectId);
            if (project == null) logMessageAndHalt(req, 400, "Must provide a valid project ID.");
            else if (userProfile.canAdministerProject(projectId, null)) return project.availableOtpServers();
        }
        else if (userProfile.canAdministerApplication()) return Persistence.servers.getAll();
        return Collections.emptyList();
    }

    /**
     * Update a single OTP server.
     */
    private static OtpServer updateServer(Request req, Response res) {
        OtpServer serverToUpdate = getServerWithPermissions(req, res);
        OtpServer updatedServer = getServerFromRequestBody(req);
        Auth0UserProfile user = req.attribute("user");
        if ((serverToUpdate.admin || serverToUpdate.projectId == null) && !user.canAdministerApplication()) {
            logMessageAndHalt(req, HttpStatus.UNAUTHORIZED_401, "User cannot modify admin-only or application-wide server.");
        }
        validateFields(req, updatedServer);
        Persistence.servers.replace(serverToUpdate.id, updatedServer);
        return Persistence.servers.getById(updatedServer.id);
    }

    /**
     * Validate certain fields found in the document representing a server. This also currently modifies the document by
     * removing problematic date fields.
     */
    private static void validateFields(Request req, OtpServer server) throws HaltException {
        try {
            // Check that projectId is valid.
            if (server.projectId != null) {
                Project project = Persistence.projects.getById(server.projectId);
                if (project == null)
                    logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Must specify valid project ID.");
            }
            // If a server's ec2 info object is not null, it must pass a few validation checks on various fields related to
            // AWS. (e.g., target group ARN and instance type).
            if (server.ec2Info != null) {
                validateTargetGroup(server.ec2Info.targetGroupArn, req);
                validateInstanceType(server.ec2Info.instanceType, req);
                validateSubnetId(server.ec2Info.subnetId, req);
                validateSecurityGroupId(server.ec2Info.securityGroupId, req);
                validateIamInstanceProfileArn(server.ec2Info.iamInstanceProfileArn, req);
                validateKeyName(server.ec2Info.keyName, req);
                validateAmiId(server.ec2Info.amiId, req);
                if (server.ec2Info.instanceCount < 0) server.ec2Info.instanceCount = 0;
            }
            // Server must have name.
            if (isEmpty(server.name))
                logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Server must have valid name.");
            // Server must have an internal URL (for build graph over wire) or an s3 bucket (for auto deploy ec2).
            if (isEmpty(server.s3Bucket)) {
                if (server.internalUrl == null || server.internalUrl.size() == 0) {
                    logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Server must contain either internal URL(s) or s3 bucket name.");
                }
            } else {
                verifyS3WritePermissions(server, req);
            }
        } catch (Exception e) {
            if (e instanceof HaltException) throw e;
            else logMessageAndHalt(req, 400, "Error encountered while validating server field", e);
        }
    }

    /**
     * Verify that application has permission to write to/delete from S3 bucket. We're following the recommended
     * approach from https://stackoverflow.com/a/17284647/915811, but perhaps there is a way to do this
     * effectively without incurring AWS costs (although writing/deleting an empty file to S3 is probably
     * miniscule).
     * @param s3Bucket
     */
    private static boolean verifyS3WritePermissions(AmazonS3 s3Client, String s3Bucket, Request req) {
        String key = UUID.randomUUID().toString();
        try {
            s3Client.putObject(s3Bucket, key, File.createTempFile("test", ".zip"));
            s3Client.deleteObject(s3Bucket, key);
        } catch (IOException | AmazonS3Exception e) {
            LOG.warn("S3 client cannot write to bucket" + s3Bucket, e);
            return false;
        }
        return true;
    }

    /**
     * Verify that application can write to S3 bucket.
     *
     * TODO: Also verify that, with AWS credentials, application can assume instance profile
     */
    private static void verifyS3WritePermissions(OtpServer server, Request req) {
        // Verify first that this application can write to the S3 bucket, which is needed to write the transit bundle
        // file to S3.
        if (!verifyS3WritePermissions(FeedStore.s3Client, server.s3Bucket, req)) {
            String message = "Application cannot write to specified S3 bucket: " + server.s3Bucket;
            logMessageAndHalt(req, 400, message);
        }
        // TODO: If EC2 info is not null, check that the IAM role ARN is able to write to the S3 bucket. I keep running
        //  into errors with this code, but will leave it commented out for now. LTR 2019/09/20
//        if (server.ec2Info != null) {
////            InstanceProfile iamInstanceProfile = getIamInstanceProfile(server.ec2Info.iamInstanceProfileArn);
//            AWSSecurityTokenServiceClient tokenServiceClient = new
//                AWSSecurityTokenServiceClient(FeedStore.getAWSCreds().getCredentials());
////            AWSSecurityTokenServiceClient tokenServiceClient = new AWSSecurityTokenServiceClient();
//            AssumeRoleRequest request = new AssumeRoleRequest()
//                .withRoleArn(server.ec2Info.iamInstanceProfileArn)
//                .withDurationSeconds(900)
//                .withRoleSessionName("test");
//            AssumeRoleResult result = tokenServiceClient.assumeRole(request);
//            Credentials credentials = result.getCredentials();
//            BasicSessionCredentials basicSessionCredentials = new BasicSessionCredentials(
//                credentials.getAccessKeyId(), credentials.getSecretAccessKey(),
//                credentials.getSessionToken());
//            AmazonS3 temporaryS3Client = AmazonS3ClientBuilder.standard()
//                .withCredentials(new AWSStaticCredentialsProvider(basicSessionCredentials))
////                .withRegion(clientRegion)
//                .build();
//            if (!verifyS3WritePermissions(temporaryS3Client, server.s3Bucket, req)) {
//                String message = "EC2 IAM role cannot write to specified S3 bucket " + server.s3Bucket;
//                logMessageAndHalt(req, 400, message);
//            }
//        }
    }

    /** Validate that AMI exists and value is not empty. */
    private static void validateAmiId(String amiId, Request req) {
        String message = "Server must have valid AMI ID (or field must be empty)";
        if (isEmpty(amiId)) return;
        if (!amiExists(amiId)) logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, message);
    }

    /** Determine if AMI ID exists (and is gettable by the application's AWS credentials). */
    public static boolean amiExists(String amiId) {
        try {
            DescribeImagesRequest request = new DescribeImagesRequest().withImageIds(amiId);
            DescribeImagesResult result = ec2.describeImages(request);
            // Iterate over AMIs to find a matching ID.
            for (Image image : result.getImages()) if (image.getImageId().equals(amiId)) return true;
        } catch (AmazonEC2Exception e) {
            LOG.warn("AMI does not exist or some error prevented proper checking of the AMI ID.", e);
        }
        return false;
    }

    /** Validate that AWS key name (the first part of a .pem key) exists and is not empty. */
    private static void validateKeyName(String keyName, Request req) {
        String message = "Server must have valid key name";
        if (isEmpty(keyName)) logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, message);
        DescribeKeyPairsResult response = ec2.describeKeyPairs();
        for (KeyPairInfo key_pair : response.getKeyPairs()) if (key_pair.getKeyName().equals(keyName)) return;
        logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, message);
    }

    /** Get IAM instance profile for the provided role ARN. */
    private static InstanceProfile getIamInstanceProfile (String iamInstanceProfileArn) {
        ListInstanceProfilesResult result = iam.listInstanceProfiles();
        // Iterate over instance profiles. If a matching ARN is found, silently return.
        for (InstanceProfile profile: result.getInstanceProfiles()) if (profile.getArn().equals(iamInstanceProfileArn)) return profile;
        return null;
    }

    /** Validate IAM instance profile ARN exists and is not empty. */
    private static void validateIamInstanceProfileArn(String iamInstanceProfileArn, Request req) {
        String message = "Server must have valid IAM instance profile ARN (e.g., arn:aws:iam::123456789012:instance-profile/otp-ec2-role).";
        if (isEmpty(iamInstanceProfileArn)) logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, message);
        if (getIamInstanceProfile(iamInstanceProfileArn) == null) logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, message);
    }

    /** Validate that EC2 security group exists and is not empty. */
    private static void validateSecurityGroupId(String securityGroupId, Request req) {
        String message = "Server must have valid security group ID";
        if (isEmpty(securityGroupId)) logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, message);
        DescribeSecurityGroupsRequest request = new DescribeSecurityGroupsRequest().withGroupIds(securityGroupId);
        DescribeSecurityGroupsResult result = ec2.describeSecurityGroups(request);
        // Iterate over groups. If a matching ID is found, silently return.
        for (SecurityGroup group : result.getSecurityGroups()) if (group.getGroupId().equals(securityGroupId)) return;
        logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, message);
    }

    /** Validate that subnet exists and is not empty. */
    private static void validateSubnetId(String subnetId, Request req) {
        String message = "Server must have valid subnet ID";
        if (isEmpty(subnetId)) logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, message);
        try {
            DescribeSubnetsRequest request = new DescribeSubnetsRequest().withSubnetIds(subnetId);
            DescribeSubnetsResult result = ec2.describeSubnets(request);
            // Iterate over subnets. If a matching ID is found, silently return.
            for (Subnet subnet : result.getSubnets()) if (subnet.getSubnetId().equals(subnetId)) return;
        } catch (AmazonEC2Exception e) {
            logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, message, e);
        }
        logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, message);
    }

    /**
     * Validate that EC2 instance type (e.g., t2-medium) exists. This value can be empty and will default to
     * {@link com.conveyal.datatools.manager.jobs.DeployJob#DEFAULT_INSTANCE_TYPE} at deploy time.
     */
    private static void validateInstanceType(String instanceType, Request req) {
        if (instanceType == null) return;
        try {
            InstanceType.fromValue(instanceType);
        } catch (IllegalArgumentException e) {
            String message = String.format("Must provide valid instance type (if none provided, defaults to %s).", DEFAULT_INSTANCE_TYPE);
            logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, message, e);
        }
    }

    /** Validate that ELB target group exists and is not empty. */
    private static void validateTargetGroup(String targetGroupArn, Request req) {
        if (isEmpty(targetGroupArn)) logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Invalid value for Target Group ARN.");
        try {
            DescribeTargetGroupsRequest request = new DescribeTargetGroupsRequest().withTargetGroupArns(targetGroupArn);
            List<TargetGroup> targetGroups = elb.describeTargetGroups(request).getTargetGroups();
            if (targetGroups.size() == 0) {
                logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Invalid value for Target Group ARN. Could not locate Target Group.");
            }
        } catch (AmazonElasticLoadBalancingException e) {
            logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Invalid value for Target Group ARN.", e);
        }
    }

    /**
     * @return false if string value is empty or null
     */
    public static boolean isEmpty(String val) {
        return val == null || "".equals(val);
    }

    /**
     * Register HTTP methods with handler methods.
     */
    public static void register (String apiPrefix) {
        options(apiPrefix + "secure/servers", (q, s) -> "");
        delete(apiPrefix + "secure/servers/:id", ServerController::deleteServer, json::write);
        delete(apiPrefix + "secure/servers/:id/ec2", ServerController::terminateEC2InstancesForServer, json::write);
        get(apiPrefix + "secure/servers", ServerController::fetchServers, json::write);
        post(apiPrefix + "secure/servers", ServerController::createServer, json::write);
        put(apiPrefix + "secure/servers/:id", ServerController::updateServer, json::write);
    }
}
