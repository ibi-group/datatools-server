package com.conveyal.datatools.manager.controllers.api;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
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
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingClientBuilder;
import com.amazonaws.services.elasticloadbalancingv2.model.AmazonElasticLoadBalancingException;
import com.amazonaws.services.elasticloadbalancingv2.model.DeregisterTargetsRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeLoadBalancersRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeLoadBalancersResult;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeTargetGroupsRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.LoadBalancer;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetDescription;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetGroup;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.identitymanagement.model.InstanceProfile;
import com.amazonaws.services.identitymanagement.model.ListInstanceProfilesResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.conveyal.datatools.common.utils.AWSUtils;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.EC2Info;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.FeedStore;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.conveyal.datatools.common.utils.SparkUtils.getPOJOFromRequestBody;
import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;
import static com.conveyal.datatools.manager.models.EC2Info.DEFAULT_INSTANCE_TYPE;
import static com.conveyal.datatools.manager.persistence.FeedStore.getAWSCreds;
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
        AmazonEC2 ec2Client = AWSUtils.getEC2ClientForRole(
            server.role,
            server.ec2Info == null ? null : server.ec2Info.region
        );
        terminateInstances(ec2Client, ids);
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
    public static TerminateInstancesResult terminateInstances(AmazonEC2 ec2Client, Collection<String> instanceIds) throws AmazonEC2Exception {
        if (instanceIds.size() == 0) {
            LOG.warn("No instance IDs provided in list. Skipping termination request.");
            return null;
        }
        LOG.info("Terminating EC2 instances {}", instanceIds);
        TerminateInstancesRequest request = new TerminateInstancesRequest().withInstanceIds(instanceIds);
        return ec2Client.terminateInstances(request);
    }

    /** Convenience method to override terminateInstances. */
    public static TerminateInstancesResult terminateInstances(AmazonEC2 ec2Client, String... instanceIds) throws AmazonEC2Exception {
        return terminateInstances(ec2Client, Arrays.asList(instanceIds));
    }

    /** Convenience method to override. */
    public static TerminateInstancesResult terminateInstances(AmazonEC2 ec2Client, List<Instance> instances) throws AmazonEC2Exception {
        return terminateInstances(ec2Client, getIds(instances));
    }

    /**
     * De-register instances from the specified target group/load balancer and terminate the instances.
     *
     */
    public static boolean deRegisterAndTerminateInstances(
        AWSStaticCredentialsProvider credentials,
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
            AmazonElasticLoadBalancing elbClient = elb;
            AmazonEC2 ec2Client = ec2;
            // If OTP Server has role defined/alt credentials, override default AWS clients.
            if (credentials != null || region != null) {
                AmazonElasticLoadBalancingClientBuilder elbBuilder = AmazonElasticLoadBalancingClient.builder();
                AmazonEC2ClientBuilder ec2Builder = AmazonEC2Client.builder();
                if (credentials != null) {
                    elbBuilder.withCredentials(credentials);
                    ec2Builder.withCredentials(credentials);
                }
                if (region != null) {
                    elbBuilder.withRegion(region);
                    ec2Builder.withRegion(region);
                }
                elbClient = elbBuilder.build();
                ec2Client = ec2Builder.build();
            }
            elbClient.deregisterTargets(request);
            ServerController.terminateInstances(ec2Client, instanceIds);
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
    private static OtpServer createServer(Request req, Response res) throws IOException {
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
    private static OtpServer updateServer(Request req, Response res) throws IOException {
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
    private static void validateFields(Request req, OtpServer server) throws HaltException {
        // Default to standard AWS clients.
        AmazonEC2 ec2Client = ec2;
        AmazonIdentityManagement iamClient = iam;
        AmazonS3 s3Client = FeedStore.s3Client;
        try {
            // Construct credentials if role is provided.
            AWSStaticCredentialsProvider credentials = AWSUtils.getCredentialsForRole(server.role, "validate");
            // If alternative credentials exist, override the default AWS clients.
            if (credentials != null) {
                // build ec2 client
                ec2Client = AmazonEC2Client.builder().withCredentials(credentials).build();
                iamClient = AmazonIdentityManagementClientBuilder.standard().withCredentials(credentials).build();
                s3Client = AWSUtils.getS3ClientForRole(server.role, null);
            }
            // Check that projectId is valid.
            if (server.projectId != null) {
                Project project = Persistence.projects.getById(server.projectId);
                if (project == null)
                    logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Must specify valid project ID.");
            }
            // If a server's ec2 info object is not null, it must pass a few validation checks on various fields related to
            // AWS. (e.g., target group ARN and instance type).
            if (server.ec2Info != null) {
                // create custom clients if credentials and or a custom region exist
                if (server.ec2Info.region != null) {
                    AmazonEC2ClientBuilder builder = AmazonEC2Client.builder();
                    if (credentials != null) {
                        builder.withCredentials(credentials);
                    }
                    builder.withRegion(server.ec2Info.region);
                    ec2Client = builder.build();
                    if (credentials !=  null) {
                        s3Client = AWSUtils.getS3ClientForRole(server.role, server.ec2Info.region);
                    } else {
                        s3Client = AWSUtils.getS3ClientForCredentials(getAWSCreds(), server.ec2Info.region);
                    }
                }
                validateInstanceType(server.ec2Info.instanceType, req);
                // Validate target group and get load balancer to validate subnetId and security group ID.
                LoadBalancer loadBalancer = validateTargetGroupAndGetLoadBalancer(server.ec2Info, req, credentials);
                validateSubnetId(loadBalancer, server.ec2Info, req, ec2Client);
                validateSecurityGroupId(loadBalancer, server.ec2Info, req);
                // Validate remaining AWS values.
                validateIamInstanceProfileArn(server.ec2Info.iamInstanceProfileArn, req, iamClient);
                validateKeyName(server.ec2Info.keyName, req, ec2Client);
                validateAmiId(server.ec2Info.amiId, req, ec2Client);
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
                verifyS3WritePermissions(server, req, s3Client);
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
            LOG.warn("S3 client cannot write to bucket: " + s3Bucket, e);
            return false;
        }
        return true;
    }

    /**
     * Verify that application can write to S3 bucket either through its own credentials or by assuming the provided IAM
     * role.
     */
    private static void verifyS3WritePermissions(OtpServer server, Request req, AmazonS3 s3Client) {
        if (!verifyS3WritePermissions(s3Client, server.s3Bucket, req)) {
            // Else, verify that this application can write to the S3 bucket, which is needed to write the transit bundle
            // file to S3.
            String message = "Application cannot write to specified S3 bucket: " + server.s3Bucket;
            logMessageAndHalt(req, 400, message);
        }
    }

    /**
     * Validate that AMI exists and value is not empty.
     *
     * TODO: Should we warn user if the AMI provided is older than the default AMI registered with this application as
     *   DEFAULT_AMI_ID?
     */
    private static void validateAmiId(String amiId, Request req, AmazonEC2 ec2Client) {
        String message = "Server must have valid AMI ID (or field must be empty)";
        if (isEmpty(amiId)) return;
        if (!amiExists(amiId, ec2Client)) logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, message);
    }

    /** Determine if AMI ID exists (and is gettable by the application's AWS credentials). */
    public static boolean amiExists(String amiId, AmazonEC2 ec2Client) {
        if (ec2Client == null) ec2Client = ec2;
        try {
            DescribeImagesRequest request = new DescribeImagesRequest().withImageIds(amiId);
            DescribeImagesResult result = ec2Client.describeImages(request);
            // Iterate over AMIs to find a matching ID.
            for (Image image : result.getImages()) if (image.getImageId().equals(amiId)) return true;
        } catch (AmazonEC2Exception e) {
            LOG.warn("AMI does not exist or some error prevented proper checking of the AMI ID.", e);
        }
        return false;
    }

    /** Validate that AWS key name (the first part of a .pem key) exists and is not empty. */
    private static void validateKeyName(String keyName, Request req, AmazonEC2 ec2Client) {
        String message = "Server must have valid key name";
        if (isEmpty(keyName)) logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, message);
        DescribeKeyPairsResult response = ec2Client.describeKeyPairs();
        for (KeyPairInfo key_pair : response.getKeyPairs()) if (key_pair.getKeyName().equals(keyName)) return;
        logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, message);
    }

    /** Get IAM instance profile for the provided role ARN. */
    private static InstanceProfile getIamInstanceProfile (String iamInstanceProfileArn, AmazonIdentityManagement iamClient) {
        ListInstanceProfilesResult result = iamClient.listInstanceProfiles();
        // Iterate over instance profiles. If a matching ARN is found, silently return.
        for (InstanceProfile profile: result.getInstanceProfiles()) if (profile.getArn().equals(iamInstanceProfileArn)) return profile;
        return null;
    }

    /** Validate IAM instance profile ARN exists and is not empty. */
    private static void validateIamInstanceProfileArn(String iamInstanceProfileArn, Request req, AmazonIdentityManagement iamClient) {
        String message = "Server must have valid IAM instance profile ARN (e.g., arn:aws:iam::123456789012:instance-profile/otp-ec2-role).";
        if (isEmpty(iamInstanceProfileArn)) logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, message);
        if (getIamInstanceProfile(iamInstanceProfileArn, iamClient) == null) logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, message);
    }

    /** Validate that EC2 security group exists and is not empty. */
    private static void validateSecurityGroupId(LoadBalancer loadBalancer, EC2Info ec2Info, Request req) {
        String message = "Server must have valid security group ID";
        List<String> securityGroups = loadBalancer.getSecurityGroups();
        if (isEmpty(ec2Info.securityGroupId)) {
            // Attempt to assign security group by deriving the value from target group/ELB.
            String securityGroupId = securityGroups.iterator().next();
            if (securityGroupId != null) {
                // Set security group to the first value found attached to ELB.
                ec2Info.securityGroupId = securityGroupId;
                return;
            }
            // If no security group found with load balancer (for whatever reason), halt request.
            logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Load balancer for target group does not have valid security group");
        }
        // Iterate over groups. If a matching ID is found, silently return.
        for (String groupId : securityGroups) if (groupId.equals(ec2Info.securityGroupId)) return;
        logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, message);
    }

    /**
     * Validate that subnet exists and is not empty. If empty, attempt to set to an ID drawn from the load balancer's
     * VPC.
     */
    private static void validateSubnetId(LoadBalancer loadBalancer, EC2Info ec2Info, Request req, AmazonEC2 ec2Client) {
        String message = "Server must have valid subnet ID";
        // Make request for all subnets associated with load balancer's vpc
        Filter filter = new Filter("vpc-id").withValues(loadBalancer.getVpcId());
        DescribeSubnetsRequest request = new DescribeSubnetsRequest().withFilters(filter);
        DescribeSubnetsResult result = ec2Client.describeSubnets(request);
        List<Subnet> subnets = result.getSubnets();
        // Attempt to assign subnet by deriving the value from target group/ELB.
        if (isEmpty(ec2Info.subnetId)) {
            // Set subnetID to the first value found.
            // TODO: could this end up with an incorrect subnet value? (i.e., a subnet that is not publicly available on
            //  the Internet?
            Subnet subnet = subnets.iterator().next();
            if (subnet != null) {
                ec2Info.subnetId = subnet.getSubnetId();
                return;
            }
            logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, message);
        } else {
            // Otherwise, verify the value set in the EC2Info.
            try {
                // Iterate over subnets. If a matching ID is found, silently return.
                for (Subnet subnet : subnets) if (subnet.getSubnetId().equals(ec2Info.subnetId)) return;
            } catch (AmazonEC2Exception e) {
                logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, message, e);
            }
            logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, message);
        }
    }

    /**
     * Validate that EC2 instance type (e.g., t2-medium) exists. This value can be empty and will default to
     * {@link com.conveyal.datatools.manager.models.EC2Info#DEFAULT_INSTANCE_TYPE} at deploy time.
     */
    private static void validateInstanceType(String instanceType, Request req) {
        if (instanceType == null) return;
        try {
            InstanceType.fromValue(instanceType);
        } catch (IllegalArgumentException e) {
            String message = String.format(
                "Must provide valid instance type (if none provided, defaults to %s).",
                DEFAULT_INSTANCE_TYPE
            );
            logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, message, e);
        }
    }

    /**
     * Gets the load balancer that the target group ARN is assigned to. Note: according to AWS docs/Stack Overflow, a
     * target group can only be assigned to a single load balancer (one-to-one relationship), so there should be no
     * risk of this giving inconsistent results.
     *  - https://serverfault.com/a/865422
     *  - https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-limits.html
     */
    private static LoadBalancer getLoadBalancerForTargetGroup (EC2Info ec2Info, AWSStaticCredentialsProvider credentials) {
        // If alternative credentials exist, use them to assume the role. Otherwise, use default ELB client.
        AmazonElasticLoadBalancingClientBuilder builder = AmazonElasticLoadBalancingClient.builder();
        if (credentials != null) {
            builder.withCredentials(credentials);
        }

        if (ec2Info.region != null) {
            builder.withRegion(ec2Info.region);
        }

        AmazonElasticLoadBalancing elbClient = builder.build();
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
    private static LoadBalancer validateTargetGroupAndGetLoadBalancer(EC2Info ec2Info, Request req, AWSStaticCredentialsProvider credentials) {
        if (isEmpty(ec2Info.targetGroupArn)) {
            logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Invalid value for Target Group ARN.");
        }
        // Get load balancer for target group. This essentially checks that the target group exists and is assigned
        // to a load balancer.
        LoadBalancer loadBalancer = getLoadBalancerForTargetGroup(ec2Info, credentials);
        if (loadBalancer == null) {
            logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Invalid value for Target Group ARN. Could not locate Target Group or Load Balancer.");
        }
        return loadBalancer;
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
