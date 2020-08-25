package com.conveyal.datatools.manager.controllers.api;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.AmazonEC2Exception;
import com.amazonaws.services.ec2.model.DescribeImagesRequest;
import com.amazonaws.services.ec2.model.DescribeImagesResult;
import com.amazonaws.services.ec2.model.DescribeKeyPairsResult;
import com.amazonaws.services.ec2.model.DescribeSubnetsRequest;
import com.amazonaws.services.ec2.model.DescribeSubnetsResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Image;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.KeyPairInfo;
import com.amazonaws.services.ec2.model.Subnet;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancing;
import com.amazonaws.services.elasticloadbalancingv2.model.AmazonElasticLoadBalancingException;
import com.amazonaws.services.elasticloadbalancingv2.model.DeregisterTargetsRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeLoadBalancersRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeLoadBalancersResult;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeTargetGroupsRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.LoadBalancer;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetDescription;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetGroup;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.model.InstanceProfile;
import com.amazonaws.services.identitymanagement.model.ListInstanceProfilesResult;
import com.amazonaws.services.s3.AmazonS3;
import com.conveyal.datatools.common.utils.CheckedAWSException;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.EC2Info;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.HaltException;
import spark.Request;
import spark.Response;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.conveyal.datatools.common.utils.AWSUtils.getEC2Client;
import static com.conveyal.datatools.common.utils.AWSUtils.getELBClient;
import static com.conveyal.datatools.common.utils.AWSUtils.getIAMClient;
import static com.conveyal.datatools.common.utils.AWSUtils.getS3Client;
import static com.conveyal.datatools.common.utils.SparkUtils.getPOJOFromRequestBody;
import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;
import static com.conveyal.datatools.manager.models.EC2Info.DEFAULT_INSTANCE_TYPE;
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
    private static OtpServer deleteServer(Request req, Response res) throws CheckedAWSException {
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
    private static OtpServer terminateEC2InstancesForServer(Request req, Response res) throws CheckedAWSException {
        OtpServer server = getServerWithPermissions(req, res);
        List<Instance> instances = server.retrieveEC2Instances();
        List<String> ids = getIds(instances);
        try {
            AmazonEC2 ec2Client = getEC2Client(server);
            terminateInstances(ec2Client, ids);
        } catch (AmazonServiceException | CheckedAWSException e) {
            logMessageAndHalt(req, 500, "Failed to terminate instances!", e);
        }
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
    public static TerminateInstancesResult terminateInstances(
        AmazonEC2 ec2Client,
        Collection<String> instanceIds
    ) throws CheckedAWSException {
        if (instanceIds.size() == 0) {
            LOG.warn("No instance IDs provided in list. Skipping termination request.");
            return null;
        }
        LOG.info("Terminating EC2 instances {}", instanceIds);
        TerminateInstancesRequest request = new TerminateInstancesRequest().withInstanceIds(instanceIds);
        try {
            return ec2Client.terminateInstances(request);
        } catch (AmazonEC2Exception e) {
            throw new CheckedAWSException(e);
        }
    }

    /** Convenience method to override terminateInstances. */
    public static TerminateInstancesResult terminateInstances(
        AmazonEC2 ec2Client,
        String... instanceIds
    ) throws CheckedAWSException {
        return terminateInstances(ec2Client, Arrays.asList(instanceIds));
    }

    /** Convenience method to override. */
    public static TerminateInstancesResult terminateInstances(
        AmazonEC2 ec2Client,
        List<Instance> instances
    ) throws CheckedAWSException {
        return terminateInstances(ec2Client, getIds(instances));
    }

    /**
     * De-register instances from the specified target group/load balancer and terminate the instances.
     *
     */
    public static boolean deRegisterAndTerminateInstances(
        String role,
        String targetGroupArn,
        String region,
        List<String> instanceIds
    ) {
        LOG.info("De-registering instances from load balancer {}", instanceIds);
        TargetDescription[] targetDescriptions = instanceIds.stream()
            .map(id -> new TargetDescription().withId(id))
            .toArray(TargetDescription[]::new);
        try {
            DeregisterTargetsRequest request = new DeregisterTargetsRequest()
                .withTargetGroupArn(targetGroupArn)
                .withTargets(targetDescriptions);
            getELBClient(role, region).deregisterTargets(request);
            ServerController.terminateInstances(getEC2Client(role, region), instanceIds);
        } catch (AmazonServiceException | CheckedAWSException e) {
            LOG.warn("Could not terminate EC2 instances: " + String.join(",", instanceIds), e);
            return false;
        }
        return true;
    }

    /**
     * Create a new server for the project. All feed sources with a valid latest version are added to the new
     * deployment.
     */
    private static OtpServer createServer(Request req, Response res) throws IOException, CheckedAWSException {
        Auth0UserProfile userProfile = req.attribute("user");
        OtpServer newServer = getPOJOFromRequestBody(req, OtpServer.class);
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
    private static OtpServer updateServer(Request req, Response res) throws IOException, CheckedAWSException {
        OtpServer serverToUpdate = getServerWithPermissions(req, res);
        OtpServer updatedServer = getPOJOFromRequestBody(req, OtpServer.class);
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
    private static void validateFields(Request req, OtpServer server) throws HaltException, CheckedAWSException {
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
                try {
                    EC2ValidationResult result = validateEC2Config(server);
                    if (!result.isValid()) {
                        logMessageAndHalt(req, 400, result.getMessage(), result.getException());
                    }
                } catch (ExecutionException | InterruptedException e) {
                    logMessageAndHalt(req, 500, "Failed to validate EC2 config", e);
                }
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
     * Asynchrnously validates all ec2 config of a particular OtpServer instance.
     */
    public static EC2ValidationResult validateEC2Config(
        OtpServer server
    ) throws ExecutionException, InterruptedException, CheckedAWSException {
        String region = null;
        if (server.ec2Info != null && server.ec2Info.region != null) region = server.ec2Info.region;

        AmazonEC2 ec2Client = getEC2Client(server.role, region);
        AmazonElasticLoadBalancing elbClient = getELBClient(server.role, region);
        AmazonIdentityManagement iamClient = getIAMClient(server.role, region);

        List<Callable<EC2ValidationResult>> validationTasks = new ArrayList<>();
        validationTasks.add(() -> validateInstanceType(server.ec2Info.instanceType));
        validationTasks.add(() -> validateInstanceType(server.ec2Info.buildInstanceType));
        validationTasks.add(() -> validateIamInstanceProfileArn(server.ec2Info.iamInstanceProfileArn, iamClient));
        validationTasks.add(() -> validateKeyName(server.ec2Info.keyName, ec2Client));
        validationTasks.add(() -> validateAmiId(server.ec2Info.amiId, ec2Client));
        validationTasks.add(() -> validateAmiId(server.ec2Info.buildAmiId, ec2Client));
        validationTasks.add(() -> validateGraphBuildReplacementAmiName(server.ec2Info, ec2Client));
        // add the load balancer task to the end since it can produce aggregate messages
        validationTasks.add(() -> validateTargetGroupLoadBalancerSubnetIdAndSecurityGroup(
            server.ec2Info,
            ec2Client,
            elbClient
        ));

        return executeValidationTasks(
            validationTasks,
            "Invalid EC2 config for the following reasons!\n"
        );
    }

    /**
     * Verify that application can write to S3 bucket either through its own credentials or by assuming the provided IAM
     * role. We're following the recommended approach from https://stackoverflow.com/a/17284647/915811, but perhaps
     * there is a way to do this effectively without incurring AWS costs (although writing/deleting an empty file to S3
     * is probably miniscule).
     */
    private static void verifyS3WritePermissions(OtpServer server, Request req) {
        String bucket = server.s3Bucket;
        String key = UUID.randomUUID().toString();
        try {
            String region = null;
            if (server.ec2Info != null && server.ec2Info.region != null) region = server.ec2Info.region;
            AmazonS3 client = getS3Client(server.role, region);
            client.putObject(bucket, key, File.createTempFile("test", ".zip"));
            client.deleteObject(bucket, key);
        } catch (Exception e) {
            logMessageAndHalt(
                req,
                400,
                "Application cannot write to specified S3 bucket: " + server.s3Bucket,
                e
            );
        }
    }

    /**
     * Validate that AMI exists and value is not empty.
     *
     * TODO: Should we warn user if the AMI provided is older than the default AMI registered with this application as
     *   DEFAULT_AMI_ID?
     */
    private static EC2ValidationResult validateAmiId(String amiId, AmazonEC2 ec2Client) {
        EC2ValidationResult result = new EC2ValidationResult();
        if (isEmpty(amiId)) return result;
        try {
            if (!amiExists(amiId, ec2Client)) {
                result.setInvalid("Server must have valid AMI ID (or field must be empty)");
            }
        } catch (AmazonEC2Exception e) {
            result.setInvalid("AMI does not exist or some error prevented proper checking of the AMI ID.", e);
        }
        return result;
    }

    /** Determine if AMI ID exists (and is gettable by the application's AWS credentials). */
    public static boolean amiExists(String amiId, AmazonEC2 ec2Client) {
        DescribeImagesRequest request = new DescribeImagesRequest().withImageIds(amiId);
        DescribeImagesResult result = ec2Client.describeImages(request);
        // Iterate over AMIs to find a matching ID.
        for (Image image : result.getImages()) {
            if (image.getImageId().equals(amiId) && image.getState().toLowerCase().equals("available")) return true;
        }
        return false;
    }

    /**
     * Validates whether the replacement graph build image name is unique. Although it is possible to have duplicate AMI
     * names when copying images, they must be unique when creating images.
     * See https://forums.aws.amazon.com/message.jspa?messageID=845159
     */
    private static EC2ValidationResult validateGraphBuildReplacementAmiName(
        EC2Info ec2Info,
        AmazonEC2 ec2Client
    ) {
        EC2ValidationResult result = new EC2ValidationResult();
        if (!ec2Info.recreateBuildImage) return result;
        String buildImageName = ec2Info.buildImageName;
        try {
            DescribeImagesRequest describeImagesRequest = new DescribeImagesRequest()
                // limit AMIs to only those owned by the current ec2 user.
                .withOwners("self");
            DescribeImagesResult describeImagesResult = ec2Client.describeImages(describeImagesRequest);
            // Iterate over AMIs to see if any images have a duplicate name.
            for (Image image : describeImagesResult.getImages()) {
                if (image.getName().equals(buildImageName)) {
                    result.setInvalid(String.format("An image with the name `%s` already exists!", buildImageName));
                    break;
                }
            }
        } catch (AmazonEC2Exception e) {
            String message = "Some error prevented proper checking of for duplicate AMI names.";
            LOG.error(message, e);
            result.setInvalid(message, e);
        }
        return result;
    }

    /** Validate that AWS key name (the first part of a .pem key) exists and is not empty. */
    private static EC2ValidationResult validateKeyName(String keyName, AmazonEC2 ec2Client) {
        String message = "Server must have valid key name";
        EC2ValidationResult result = new EC2ValidationResult();
        if (isEmpty(keyName)) {
            result.setInvalid(message);
            return result;
        };
        DescribeKeyPairsResult response = ec2Client.describeKeyPairs();
        for (KeyPairInfo key_pair : response.getKeyPairs()) if (key_pair.getKeyName().equals(keyName)) return result;
        result.setInvalid(message);
        return result;
    }

    /** Get IAM instance profile for the provided role ARN. */
    private static InstanceProfile getIamInstanceProfile (String iamInstanceProfileArn, AmazonIdentityManagement iamClient) {
        ListInstanceProfilesResult result = iamClient.listInstanceProfiles();
        // Iterate over instance profiles. If a matching ARN is found, silently return.
        for (InstanceProfile profile: result.getInstanceProfiles()) if (profile.getArn().equals(iamInstanceProfileArn)) return profile;
        return null;
    }

    /** Validate IAM instance profile ARN exists and is not empty. */
    private static EC2ValidationResult validateIamInstanceProfileArn(
        String iamInstanceProfileArn,
        AmazonIdentityManagement iamClient
    ) {
        EC2ValidationResult result = new EC2ValidationResult();
        String message = "Server must have valid IAM instance profile ARN (e.g., arn:aws:iam::123456789012:instance-profile/otp-ec2-role).";
        if (isEmpty(iamInstanceProfileArn)) {
            result.setInvalid(message);
            return result;
        }
        if (getIamInstanceProfile(iamInstanceProfileArn, iamClient) == null) {
            result.setInvalid(message);
        };
        return result;
    }

    /** Validate that EC2 security group exists and is not empty. */
    private static EC2ValidationResult validateSecurityGroupId(LoadBalancer loadBalancer, EC2Info ec2Info) {
        EC2ValidationResult result = new EC2ValidationResult();
        String message = "Server must have valid security group ID";
        List<String> securityGroups = loadBalancer.getSecurityGroups();
        if (isEmpty(ec2Info.securityGroupId)) {
            // Attempt to assign security group by deriving the value from target group/ELB.
            String securityGroupId = securityGroups.iterator().next();
            if (securityGroupId != null) {
                // Set security group to the first value found attached to ELB.
                ec2Info.securityGroupId = securityGroupId;
                return result;
            }
            // If no security group found with load balancer (for whatever reason), halt request.
            result.setInvalid("Load balancer for target group does not have valid security group");
            return result;
        }
        // Iterate over groups. If a matching ID is found, silently return.
        for (String groupId : securityGroups) if (groupId.equals(ec2Info.securityGroupId)) return result;
        result.setInvalid(message);
        return result;
    }

    /**
     * Validate that subnet exists and is not empty. If empty, attempt to set to an ID drawn from the load balancer's
     * VPC.
     */
    private static EC2ValidationResult validateSubnetId(
        LoadBalancer loadBalancer,
        EC2Info ec2Info,
        AmazonEC2 ec2Client
    ) {
        EC2ValidationResult result = new EC2ValidationResult();
        String message = "Server must have valid subnet ID";
        // Make request for all subnets associated with load balancer's vpc
        Filter filter = new Filter("vpc-id").withValues(loadBalancer.getVpcId());
        DescribeSubnetsRequest describeSubnetsRequest = new DescribeSubnetsRequest().withFilters(filter);
        DescribeSubnetsResult describeSubnetsResult = ec2Client.describeSubnets(describeSubnetsRequest);
        List<Subnet> subnets = describeSubnetsResult.getSubnets();
        // Attempt to assign subnet by deriving the value from target group/ELB.
        if (isEmpty(ec2Info.subnetId)) {
            // Set subnetID to the first value found.
            // TODO: could this end up with an incorrect subnet value? (i.e., a subnet that is not publicly available on
            //  the Internet?
            Subnet subnet = subnets.iterator().next();
            if (subnet != null) {
                ec2Info.subnetId = subnet.getSubnetId();
                return result;
            }
            result.setInvalid(message);
        } else {
            // Otherwise, verify the value set in the EC2Info.
            try {
                // Iterate over subnets. If a matching ID is found, silently return.
                for (Subnet subnet : subnets) if (subnet.getSubnetId().equals(ec2Info.subnetId)) return result;
            } catch (AmazonEC2Exception e) {
                result.setInvalid(message, e);
                return result;
            }
            result.setInvalid(message);
        }
        return result;
    }

    /**
     * Validate that EC2 instance type (e.g., t2-medium) exists. This value can be empty and will default to
     * {@link com.conveyal.datatools.manager.models.EC2Info#DEFAULT_INSTANCE_TYPE} at deploy time.
     */
    private static EC2ValidationResult validateInstanceType(String instanceType) {
        EC2ValidationResult result = new EC2ValidationResult();
        if (instanceType == null) return result;
        try {
            InstanceType.fromValue(instanceType);
        } catch (IllegalArgumentException e) {
            result.setInvalid(
                String.format(
                    "Must provide valid instance type (if none provided, defaults to %s).",
                    DEFAULT_INSTANCE_TYPE
                ),
                e
            );
        }
        return result;
    }

    /**
     * Gets the load balancer that the target group ARN is assigned to. Note: according to AWS docs/Stack Overflow, a
     * target group can only be assigned to a single load balancer (one-to-one relationship), so there should be no
     * risk of this giving inconsistent results.
     *  - https://serverfault.com/a/865422
     *  - https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-limits.html
     */
    private static LoadBalancer getLoadBalancerForTargetGroup (EC2Info ec2Info, AmazonElasticLoadBalancing elbClient) {
        try {
            DescribeTargetGroupsRequest targetGroupsRequest = new DescribeTargetGroupsRequest()
                .withTargetGroupArns(ec2Info.targetGroupArn);
            List<TargetGroup> targetGroups = elbClient.describeTargetGroups(targetGroupsRequest).getTargetGroups();
            for (TargetGroup tg : targetGroups) {
                DescribeLoadBalancersRequest request = new DescribeLoadBalancersRequest()
                    .withLoadBalancerArns(tg.getLoadBalancerArns());
                DescribeLoadBalancersResult result = elbClient.describeLoadBalancers(request);
                // Return the first load balancer
                return result.getLoadBalancers().iterator().next();
            }
        } catch (AmazonElasticLoadBalancingException e) {
            LOG.warn("Invalid value for Target Group ARN: {}", ec2Info.targetGroupArn);
        }
        // If no target group/load balancer found, return null.
        return null;
    }

    /**
     * Validate that ELB target group exists and is not empty and return associated load balancer for validating related
     * fields.
     */
    private static EC2ValidationResult validateTargetGroupLoadBalancerSubnetIdAndSecurityGroup(
        EC2Info ec2Info,
        AmazonEC2 ec2Client,
        AmazonElasticLoadBalancing elbClient
    ) throws ExecutionException, InterruptedException {
        EC2ValidationResult result = new EC2ValidationResult();
        if (isEmpty(ec2Info.targetGroupArn)) {
            result.setInvalid("Invalid value for Target Group ARN.");
            return result;
        }
        // Get load balancer for target group. This essentially checks that the target group exists and is assigned
        // to a load balancer.
        LoadBalancer loadBalancer = getLoadBalancerForTargetGroup(ec2Info, elbClient);
        if (loadBalancer == null) {
            result.setInvalid("Invalid value for Target Group ARN. Could not locate Target Group or Load Balancer.");
            return result;
        }

        // asynchronously execute the two validation tasks that depend on the load balancer info
        List<Callable<EC2ValidationResult>> loadBalancerValidationTasks = new ArrayList<>();
        loadBalancerValidationTasks.add(() -> validateSubnetId(loadBalancer, ec2Info, ec2Client));
        loadBalancerValidationTasks.add(() -> validateSecurityGroupId(loadBalancer, ec2Info));

        return executeValidationTasks(
            loadBalancerValidationTasks,
            "Invalid EC2 load balancer config for the following reasons:\n"
        );
    }

    private static EC2ValidationResult executeValidationTasks(
        List<Callable<EC2ValidationResult>> validationTasks,
        String overallInvalidMessage
    ) throws ExecutionException, InterruptedException {
        // create overall result
        EC2ValidationResult result = new EC2ValidationResult();

        // Create a thread pool that is the size of the total number of validation tasks so each task gets its own
        // thread
        ExecutorService pool = Executors.newFixedThreadPool(validationTasks.size());

        // Execute all tasks
        for (Future<EC2ValidationResult> resultFuture : pool.invokeAll(validationTasks)) {
            EC2ValidationResult taskResult = resultFuture.get();
            // check if task yielded a valid result
            if (!taskResult.isValid()) {
                // task had an invalid result, check if overall validation result has been changed to false yet
                if (result.isValid()) {
                    // first invalid result. Write a header message.
                    result.setInvalid(overallInvalidMessage);
                }
                // add to list of messages and exceptions
                result.appendResult(taskResult);
            }
        }
        pool.shutdown();
        return result;
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

    /**
     * A helper class that returns a validation result and accompanying message.
     */
    public static class EC2ValidationResult {
        private Exception exception;
        private String message;
        private boolean valid = true;

        public Exception getException() {
            return exception;
        }

        public String getMessage() {
            return message;
        }

        public boolean isValid() {
            return valid;
        }

        public void setInvalid(String message) {
            this.setInvalid(message, null);
        }

        public void setInvalid(String message, Exception e) {
            this.exception = e;
            this.message = message;
            this.valid = false;
        }

        public void appendResult(EC2ValidationResult taskValidationResult) {
            if (this.message == null) throw new IllegalStateException("Must have initialized message before appending");
            this.message = String.format("%s  - %s\n", this.message, taskValidationResult.message);
            // add to list of supressed exceptions if needed
            if (taskValidationResult.exception != null) {
                if (this.exception == null) {
                    throw new IllegalStateException("Must have initialized exception before appending");
                }
                this.exception.addSuppressed(taskValidationResult.exception);
            }
        }
    }
}
