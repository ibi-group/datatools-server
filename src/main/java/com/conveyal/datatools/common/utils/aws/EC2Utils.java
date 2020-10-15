package com.conveyal.datatools.common.utils.aws;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.AmazonEC2Exception;
import com.amazonaws.services.ec2.model.DescribeImagesRequest;
import com.amazonaws.services.ec2.model.DescribeImagesResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.DescribeKeyPairsResult;
import com.amazonaws.services.ec2.model.DescribeSubnetsRequest;
import com.amazonaws.services.ec2.model.DescribeSubnetsResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Image;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.KeyPairInfo;
import com.amazonaws.services.ec2.model.Reservation;
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
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.EC2InstanceSummary;
import com.conveyal.datatools.manager.models.OtpServer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * This class contains utilities related to using AWS EC2 and ELB services.
 */
public class EC2Utils {
    private static final Logger LOG = LoggerFactory.getLogger(EC2Utils.class);

    public static final String AMI_CONFIG_PATH = "modules.deployment.ec2.default_ami";
    public static final String DEFAULT_AMI_ID = DataManager.getConfigPropertyAsText(AMI_CONFIG_PATH);
    public static final String DEFAULT_INSTANCE_TYPE = "t2.medium";

    private static final AmazonEC2 DEFAULT_EC2_CLIENT = AmazonEC2Client.builder().build();
    private static final AmazonElasticLoadBalancing DEFAULT_ELB_CLIENT = AmazonElasticLoadBalancingClient
        .builder()
        .build();
    private static final EC2ClientManagerImpl EC2ClientManager = new EC2ClientManagerImpl(DEFAULT_EC2_CLIENT);
    private static final ELBClientManagerImpl ELBClientManager = new ELBClientManagerImpl(EC2Utils.DEFAULT_ELB_CLIENT);

    /**
     * A class that manages the creation of EC2 clients.
     */
    private static class EC2ClientManagerImpl extends AWSClientManager<AmazonEC2> {
        public EC2ClientManagerImpl(AmazonEC2 defaultClient) {
            super(defaultClient);
        }

        @Override
        public AmazonEC2 buildDefaultClientWithRegion(String region) {
            return AmazonEC2Client.builder().withRegion(region).build();
        }

        @Override
        public AmazonEC2 buildCredentialedClientForRoleAndRegion(
            AWSCredentialsProvider credentials, String region, String role
        ) {
            AmazonEC2ClientBuilder builder = AmazonEC2Client.builder().withCredentials(credentials);
            if (region != null) {
                builder = builder.withRegion(region);
            }
            return builder.build();
        }
    }

    /**
     * A class that manages the creation of ELB clients.
     */
    private static class ELBClientManagerImpl extends AWSClientManager<AmazonElasticLoadBalancing> {
        public ELBClientManagerImpl(AmazonElasticLoadBalancing defaultClient) {
            super(defaultClient);
        }

        @Override
        public AmazonElasticLoadBalancing buildDefaultClientWithRegion(String region) {
            return AmazonElasticLoadBalancingClient.builder().withRegion(region).build();
        }

        @Override
        public AmazonElasticLoadBalancing buildCredentialedClientForRoleAndRegion(
            AWSCredentialsProvider credentials, String region, String role
        ) {
            AmazonElasticLoadBalancingClientBuilder builder = AmazonElasticLoadBalancingClient
                .builder()
                .withCredentials(credentials);
            if (region != null) {
                builder = builder.withRegion(region);
            }
            return builder.build();
        }
    }

    /** Determine if AMI ID exists (and is gettable by the application's AWS credentials). */
    public static boolean amiExists(AmazonEC2 ec2Client, String amiId) {
        DescribeImagesRequest request = new DescribeImagesRequest().withImageIds(amiId);
        DescribeImagesResult result = ec2Client.describeImages(request);
        // Iterate over AMIs to find a matching ID.
        for (Image image : result.getImages()) {
            if (image.getImageId().equals(amiId) && image.getState().toLowerCase().equals("available")) return true;
        }
        return false;
    }

    /**
     * De-register instances from the specified target group/load balancer and terminate the instances.
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
            terminateInstances(getEC2Client(role, region), instanceIds);
        } catch (AmazonServiceException | CheckedAWSException e) {
            LOG.warn("Could not terminate EC2 instances: {}", String.join(",", instanceIds), e);
            return false;
        }
        return true;
    }

    /**
     * Fetches list of {@link EC2InstanceSummary} for all instances matching the provided filters.
     */
    public static List<EC2InstanceSummary> fetchEC2InstanceSummaries(AmazonEC2 ec2Client, Filter... filters) {
        return fetchEC2Instances(ec2Client, filters).stream().map(EC2InstanceSummary::new).collect(Collectors.toList());
    }

    /**
     * Fetch EC2 instances from AWS that match the provided set of filters (e.g., tags, instance ID, or other properties).
     */
    public static List<Instance> fetchEC2Instances(AmazonEC2 ec2Client, Filter... filters) {
        if (ec2Client == null) throw new IllegalArgumentException("Must provide EC2Client");
        List<Instance> instances = new ArrayList<>();
        DescribeInstancesRequest request = new DescribeInstancesRequest().withFilters(filters);
        DescribeInstancesResult result = ec2Client.describeInstances(request);
        for (Reservation reservation : result.getReservations()) {
            instances.addAll(reservation.getInstances());
        }
        // Sort by launch time (most recent first).
        instances.sort(Comparator.comparing(Instance::getLaunchTime).reversed());
        return instances;
    }

    public static AmazonEC2 getEC2Client(String role, String region) throws CheckedAWSException {
        return EC2ClientManager.getClient(role, region);
    }

    public static AmazonElasticLoadBalancing getELBClient(String role, String region) throws CheckedAWSException {
        return ELBClientManager.getClient(role, region);
    }

    /**
     * Gets the load balancer that the target group ARN is assigned to. Note: according to AWS docs/Stack Overflow, a
     * target group can only be assigned to a single load balancer (one-to-one relationship), so there should be no
     * risk of this giving inconsistent results.
     *  - https://serverfault.com/a/865422
     *  - https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-limits.html
     */
    public static LoadBalancer getLoadBalancerForTargetGroup(
        AmazonElasticLoadBalancing elbClient,
        String targetGroupArn
    ) {
        try {
            DescribeTargetGroupsRequest targetGroupsRequest = new DescribeTargetGroupsRequest()
                .withTargetGroupArns(targetGroupArn);
            List<TargetGroup> targetGroups = elbClient.describeTargetGroups(targetGroupsRequest).getTargetGroups();
            for (TargetGroup tg : targetGroups) {
                DescribeLoadBalancersRequest request = new DescribeLoadBalancersRequest()
                    .withLoadBalancerArns(tg.getLoadBalancerArns());
                DescribeLoadBalancersResult result = elbClient.describeLoadBalancers(request);
                // Return the first load balancer
                return result.getLoadBalancers().iterator().next();
            }
        } catch (AmazonElasticLoadBalancingException e) {
            LOG.warn("Invalid value for Target Group ARN: {}", targetGroupArn);
        }
        // If no target group/load balancer found, return null.
        return null;
    }

    /**
     * Terminate the EC2 instances associated with the given string collection of EC2 instance IDs.
     *
     * @param ec2Client The client to use when terminating the instances.
     * @param instanceIds A collection of strings of EC2 instance IDs that should be terminated.
     */
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

    /**
     * Convenience method to override {@link EC2Utils#terminateInstances(AmazonEC2, Collection)}.
     *
     * @param ec2Client The client to use when terminating the instances.
     * @param instanceIds Each argument should be a string of an instance ID that should be terminated.
     */
    public static TerminateInstancesResult terminateInstances(
        AmazonEC2 ec2Client,
        String... instanceIds
    ) throws CheckedAWSException {
        return terminateInstances(ec2Client, Arrays.asList(instanceIds));
    }

    /**
     * Convenience method to override {@link EC2Utils#terminateInstances(AmazonEC2, Collection)}.
     *
     * @param ec2Client The client to use when terminating the instances.
     * @param instances A list of EC2 Instances that should be terminated.
     */
    public static TerminateInstancesResult terminateInstances(
        AmazonEC2 ec2Client,
        List<Instance> instances
    ) throws CheckedAWSException {
        return terminateInstances(ec2Client, getIds(instances));
    }

    /**
     * Shorthand method for getting list of string identifiers from a list of EC2 instances.
     */
    public static List<String> getIds (List<Instance> instances) {
        return instances.stream().map(Instance::getInstanceId).collect(Collectors.toList());
    }

    /**
     * Validate that AMI exists and value is not empty.
     *
     * TODO: Should we warn user if the AMI provided is older than the default AMI registered with this application as
     *   DEFAULT_AMI_ID?
     */
    public static EC2ValidationResult validateAmiId(AmazonEC2 ec2Client, String amiId) {
        EC2ValidationResult result = new EC2ValidationResult();
        if (StringUtils.isEmpty(amiId))
            return result;
        try {
            if (!EC2Utils.amiExists(ec2Client, amiId)) {
                result.setInvalid("Server must have valid AMI ID (or field must be empty)");
            }
        } catch (AmazonEC2Exception e) {
            result.setInvalid("AMI does not exist or some error prevented proper checking of the AMI ID.", e);
        }
        return result;
    }

    /**
     * Validates whether the replacement graph build image name is unique. Although it is possible to have duplicate AMI
     * names when copying images, they must be unique when creating images.
     * See https://forums.aws.amazon.com/message.jspa?messageID=845159
     */
    public static EC2ValidationResult validateGraphBuildReplacementAmiName(OtpServer otpServer) {
        EC2ValidationResult result = new EC2ValidationResult();
        if (!otpServer.ec2Info.recreateBuildImage) return result;
        String buildImageName = otpServer.ec2Info.buildImageName;
        try {
            DescribeImagesRequest describeImagesRequest = new DescribeImagesRequest()
                // limit AMIs to only those owned by the current ec2 user.
                .withOwners("self");
            DescribeImagesResult describeImagesResult = otpServer.getEC2Client().describeImages(describeImagesRequest);
            // Iterate over AMIs to see if any images have a duplicate name.
            for (Image image : describeImagesResult.getImages()) {
                if (image.getName().equals(buildImageName)) {
                    result.setInvalid(String.format("An image with the name `%s` already exists!", buildImageName));
                    break;
                }
            }
        } catch (AmazonEC2Exception | CheckedAWSException e) {
            String message = "Some error prevented proper checking of for duplicate AMI names.";
            LOG.error(message, e);
            result.setInvalid(message, e);
        }
        return result;
    }

    /**
     * Validate that EC2 instance type (e.g., t2-medium) exists. This value can be empty and will default to
     * {@link EC2Utils#DEFAULT_INSTANCE_TYPE} at deploy time.
     */
    public static EC2ValidationResult validateInstanceType(String instanceType) {
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
     * Validate that the AWS key name (the first part of a .pem key) exists and is not empty.
     */
    public static EC2ValidationResult validateKeyName(AmazonEC2 ec2Client, String keyName) {
        String message = "Server must have valid key name";
        EC2ValidationResult result = new EC2ValidationResult();
        if (StringUtils.isEmpty(keyName)) {
            result.setInvalid(message);
            return result;
        }
        DescribeKeyPairsResult response = ec2Client.describeKeyPairs();
        for (KeyPairInfo key_pair : response.getKeyPairs()) {
            if (key_pair.getKeyName().equals(keyName)) return result;
        }
        result.setInvalid(message);
        return result;
    }

    /**
     * Validate that EC2 security group exists and is not empty. If it is empty, attempt to assign security group by
     * deriving the value from target group/ELB.
     */
    public static EC2ValidationResult validateSecurityGroupId(
        OtpServer otpServer,
        LoadBalancer loadBalancer
    ) {
        EC2ValidationResult result = new EC2ValidationResult();
        String message = "Server must have valid security group ID";
        List<String> securityGroups = loadBalancer.getSecurityGroups();
        if (StringUtils.isEmpty(otpServer.ec2Info.securityGroupId)) {
            // Attempt to assign security group by deriving the value from target group/ELB.
            String securityGroupId = securityGroups.iterator().next();
            if (securityGroupId != null) {
                // Set security group to the first value found attached to ELB.
                otpServer.ec2Info.securityGroupId = securityGroupId;
                return result;
            }
            // If no security group found with load balancer (for whatever reason), halt request.
            result.setInvalid("Load balancer for target group does not have valid security group");
            return result;
        }
        // Iterate over groups. If a matching ID is found, silently return.
        for (String groupId : securityGroups) if (groupId.equals(otpServer.ec2Info.securityGroupId)) return result;
        result.setInvalid(message);
        return result;
    }

    /**
     * Validate that subnet exists and is not empty. If empty, attempt to set to an ID drawn from the load balancer's
     * VPC.
     */
    public static EC2ValidationResult validateSubnetId(OtpServer otpServer, LoadBalancer loadBalancer) {
        EC2ValidationResult result = new EC2ValidationResult();
        String message = "Server must have valid subnet ID";
        // Make request for all subnets associated with load balancer's vpc
        Filter filter = new Filter("vpc-id").withValues(loadBalancer.getVpcId());
        DescribeSubnetsRequest describeSubnetsRequest = new DescribeSubnetsRequest().withFilters(filter);
        DescribeSubnetsResult describeSubnetsResult;
        try {
            describeSubnetsResult = otpServer.getEC2Client().describeSubnets(describeSubnetsRequest);
        } catch (CheckedAWSException e) {
            result.setInvalid(message, e);
            return result;
        }
        List<Subnet> subnets = describeSubnetsResult.getSubnets();
        // Attempt to assign subnet by deriving the value from target group/ELB.
        if (StringUtils.isEmpty(otpServer.ec2Info.subnetId)) {
            // Set subnetID to the first value found.
            // TODO: could this end up with an incorrect subnet value? (i.e., a subnet that is not publicly available on
            //  the Internet?
            Subnet subnet = subnets.iterator().next();
            if (subnet != null) {
                otpServer.ec2Info.subnetId = subnet.getSubnetId();
                return result;
            }
        } else {
            // Otherwise, verify the value set in the EC2Info.
            try {
                // Iterate over subnets. If a matching ID is found, silently return.
                for (Subnet subnet : subnets) if (subnet.getSubnetId().equals(otpServer.ec2Info.subnetId)) return result;
            } catch (AmazonEC2Exception e) {
                result.setInvalid(message, e);
                return result;
            }
        }
        result.setInvalid(message);
        return result;
    }

    /**
     * Validate that ELB target group exists and is not empty and return associated load balancer for validating related
     * fields.
     */
    public static EC2ValidationResult validateTargetGroupLoadBalancerSubnetIdAndSecurityGroup(OtpServer otpServer)
        throws ExecutionException, InterruptedException, CheckedAWSException {
        EC2ValidationResult result = new EC2ValidationResult();
        if (StringUtils.isEmpty(otpServer.ec2Info.targetGroupArn)) {
            result.setInvalid("Invalid value for Target Group ARN.");
            return result;
        }
        // Get load balancer for target group. This essentially checks that the target group exists and is assigned
        // to a load balancer.
        LoadBalancer loadBalancer = getLoadBalancerForTargetGroup(
            getELBClient(otpServer.role, otpServer.getRegion()),
            otpServer.ec2Info.targetGroupArn
        );
        if (loadBalancer == null) {
            result.setInvalid("Invalid value for Target Group ARN. Could not locate Target Group or Load Balancer.");
            return result;
        }

        // asynchronously execute the two validation tasks that depend on the load balancer info
        List<Callable<EC2ValidationResult>> loadBalancerValidationTasks = new ArrayList<>();
        loadBalancerValidationTasks.add(() -> validateSubnetId(otpServer, loadBalancer));
        loadBalancerValidationTasks.add(() -> validateSecurityGroupId(otpServer, loadBalancer));

        return EC2ValidationResult.executeValidationTasks(
            loadBalancerValidationTasks,
            "Invalid EC2 load balancer config for the following reasons:\n"
        );
    }
}
