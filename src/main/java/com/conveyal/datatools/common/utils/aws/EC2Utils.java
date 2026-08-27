package com.conveyal.datatools.common.utils.aws;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.EC2InstanceSummary;
import com.conveyal.datatools.manager.models.OtpServer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.Ec2ClientBuilder;
import software.amazon.awssdk.services.ec2.model.DescribeImagesResponse;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.DescribeKeyPairsResponse;
import software.amazon.awssdk.services.ec2.model.DescribeSubnetsResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.Image;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.KeyPairInfo;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.Subnet;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesResponse;
import software.amazon.awssdk.services.elasticloadbalancingv2.ElasticLoadBalancingV2Client;
import software.amazon.awssdk.services.elasticloadbalancingv2.ElasticLoadBalancingV2ClientBuilder;
import software.amazon.awssdk.services.elasticloadbalancingv2.model.ElasticLoadBalancingV2Exception;
import software.amazon.awssdk.services.elasticloadbalancingv2.model.LoadBalancer;
import software.amazon.awssdk.services.elasticloadbalancingv2.model.TargetDescription;
import software.amazon.awssdk.services.elasticloadbalancingv2.model.TargetGroup;

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

    private static final Ec2Client DEFAULT_EC2_CLIENT = Ec2Client.builder().build();
    private static final ElasticLoadBalancingV2Client DEFAULT_ELB_CLIENT = ElasticLoadBalancingV2Client
        .builder()
        .build();
    private static final EC2ClientManagerImpl EC2ClientManager = new EC2ClientManagerImpl(DEFAULT_EC2_CLIENT);
    private static final ELBClientManagerImpl ELBClientManager = new ELBClientManagerImpl(EC2Utils.DEFAULT_ELB_CLIENT);

    /**
     * A class that manages the creation of EC2 clients.
     */
    private static class EC2ClientManagerImpl extends AWSClientManager<Ec2Client> {
        public EC2ClientManagerImpl(Ec2Client defaultClient) {
            super(defaultClient);
        }

        @Override
        public Ec2Client buildDefaultClientWithRegion(String region) {
            return Ec2Client.builder().region(Region.of(region)).build();
        }

        @Override
        public Ec2Client buildCredentialedClientForRoleAndRegion(
            AwsCredentialsProvider credentials, String region, String role
        ) {
            Ec2ClientBuilder builder = Ec2Client.builder().credentialsProvider(credentials);
            if (region != null) {
                builder = builder.region(Region.of(region));
            }
            return builder.build();
        }
    }

    /**
     * A class that manages the creation of ELB clients.
     */
    private static class ELBClientManagerImpl extends AWSClientManager<ElasticLoadBalancingV2Client> {
        public ELBClientManagerImpl(ElasticLoadBalancingV2Client defaultClient) {
            super(defaultClient);
        }

        @Override
        public ElasticLoadBalancingV2Client buildDefaultClientWithRegion(String region) {
            return ElasticLoadBalancingV2Client.builder().region(Region.of(region)).build();
        }

        @Override
        public ElasticLoadBalancingV2Client buildCredentialedClientForRoleAndRegion(
            AwsCredentialsProvider credentials, String region, String role
        ) {
            ElasticLoadBalancingV2ClientBuilder builder = ElasticLoadBalancingV2Client
                .builder()
                .credentialsProvider(credentials);
            if (region != null) {
                builder = builder.region(Region.of(region));
            }
            return builder.build();
        }
    }

    /** Determine if AMI ID exists (and is gettable by the application's AWS credentials). */
    public static boolean amiExists(Ec2Client ec2Client, String amiId) {
        DescribeImagesResponse result = ec2Client.describeImages(req -> req.imageIds(amiId));
        // Iterate over AMIs to find a matching ID.
        for (Image image : result.images()) {
            if (image.imageId().equals(amiId) && image.state().name().equalsIgnoreCase("available")) return true;
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
            .map(id -> TargetDescription.builder().id(id).build())
            .toArray(TargetDescription[]::new);
        try {
            getELBClient(role, region).deregisterTargets(
                req -> req
                    .targetGroupArn(targetGroupArn)
                    .targets(targetDescriptions)
            );
            terminateInstances(getEC2Client(role, region), instanceIds);
        } catch (AwsServiceException | CheckedAWSException e) {
            LOG.warn("Could not terminate EC2 instances: {}", String.join(",", instanceIds), e);
            return false;
        }
        return true;
    }

    /**
     * Fetches list of {@link EC2InstanceSummary} for all instances matching the provided filters.
     */
    public static List<EC2InstanceSummary> fetchEC2InstanceSummaries(Ec2Client ec2Client, Filter... filters) {
        return fetchEC2Instances(ec2Client, filters).stream().map(EC2InstanceSummary::new).collect(Collectors.toList());
    }

    /**
     * Fetch EC2 instances from AWS that match the provided set of filters (e.g., tags, instance ID, or other properties).
     */
    public static List<Instance> fetchEC2Instances(Ec2Client ec2Client, Filter... filters) {
        if (ec2Client == null) throw new IllegalArgumentException("Must provide EC2Client");
        DescribeInstancesResponse result = ec2Client.describeInstances(req -> req.filters(filters));
        List<Instance> instances = new ArrayList<>();
        for (Reservation reservation : result.reservations()) {
            instances.addAll(reservation.instances());
        }
        // Sort by launch time (most recent first).
        instances.sort(Comparator.comparing(Instance::launchTime).reversed());
        return instances;
    }

    public static Ec2Client getEC2Client(String role, String region) throws CheckedAWSException {
        return EC2ClientManager.getClient(role, region);
    }

    public static ElasticLoadBalancingV2Client getELBClient(String role, String region) throws CheckedAWSException {
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
        ElasticLoadBalancingV2Client elbClient,
        String targetGroupArn
    ) {
        try {
            List<TargetGroup> targetGroups = elbClient
                .describeTargetGroups(req -> req.targetGroupArns(targetGroupArn))
                .targetGroups();
            for (TargetGroup tg : targetGroups) {
                // Return the first load balancer
                return elbClient
                    .describeLoadBalancers(req -> req.loadBalancerArns(tg.loadBalancerArns()))
                    .loadBalancers()
                    .iterator().next();
            }
        } catch (ElasticLoadBalancingV2Exception e) {
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
    public static TerminateInstancesResponse terminateInstances(
        Ec2Client ec2Client,
        Collection<String> instanceIds
    ) throws CheckedAWSException {
        if (instanceIds.isEmpty()) {
            LOG.warn("No instance IDs provided in list. Skipping termination request.");
            return null;
        }
        LOG.info("Terminating EC2 instances {}", instanceIds);
        try {
            return ec2Client.terminateInstances(req -> req.instanceIds(instanceIds));
        } catch (Ec2Exception e) {
            throw new CheckedAWSException(e);
        }
    }

    /**
     * Convenience method to override {@link EC2Utils#terminateInstances(Ec2Client, Collection)}.
     *
     * @param ec2Client The client to use when terminating the instances.
     * @param instanceIds Each argument should be a string of an instance ID that should be terminated.
     */
    public static TerminateInstancesResponse terminateInstances(
        Ec2Client ec2Client,
        String... instanceIds
    ) throws CheckedAWSException {
        return terminateInstances(ec2Client, Arrays.asList(instanceIds));
    }

    /**
     * Convenience method to override {@link EC2Utils#terminateInstances(Ec2Client, Collection)}.
     *
     * @param ec2Client The client to use when terminating the instances.
     * @param instances A list of EC2 Instances that should be terminated.
     */
    public static TerminateInstancesResponse terminateInstances(
        Ec2Client ec2Client,
        List<Instance> instances
    ) throws CheckedAWSException {
        return terminateInstances(ec2Client, getIds(instances));
    }

    /**
     * Shorthand method for getting list of string identifiers from a list of EC2 instances.
     */
    public static List<String> getIds (List<Instance> instances) {
        return instances.stream().map(Instance::instanceId).collect(Collectors.toList());
    }

    /**
     * Validate that AMI exists and value is not empty.
     *
     * TODO: Should we warn user if the AMI provided is older than the default AMI registered with this application as
     *   DEFAULT_AMI_ID?
     */
    public static EC2ValidationResult validateAmiId(Ec2Client ec2Client, String amiId) {
        EC2ValidationResult result = new EC2ValidationResult();
        if (StringUtils.isEmpty(amiId))
            return result;
        try {
            if (!EC2Utils.amiExists(ec2Client, amiId)) {
                result.setInvalid("Server must have valid AMI ID (or field must be empty)");
            }
        } catch (Ec2Exception e) {
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
            DescribeImagesResponse describeImagesResult = otpServer.getEC2Client().describeImages(
                // limit AMIs to only those owned by the current ec2 user.
                req -> req.owners("self")
            );
            // Iterate over AMIs to see if any images have a duplicate name.
            for (Image image : describeImagesResult.images()) {
                if (image.name().equals(buildImageName)) {
                    result.setInvalid(String.format("An image with the name `%s` already exists!", buildImageName));
                    break;
                }
            }
        } catch (Ec2Exception | CheckedAWSException e) {
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
    public static EC2ValidationResult validateKeyName(Ec2Client ec2Client, String keyName) {
        String message = "Server must have valid key name";
        EC2ValidationResult result = new EC2ValidationResult();
        if (StringUtils.isEmpty(keyName)) {
            result.setInvalid(message);
            return result;
        }
        DescribeKeyPairsResponse response = ec2Client.describeKeyPairs();
        for (KeyPairInfo key_pair : response.keyPairs()) {
            if (key_pair.keyName().equals(keyName)) return result;
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
        List<String> securityGroups = loadBalancer.securityGroups();
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
        Filter filter = Filter.builder().name("vpc-id").values(loadBalancer.vpcId()).build();
        DescribeSubnetsResponse describeSubnetsResult;
        try {
            describeSubnetsResult = otpServer.getEC2Client().describeSubnets(req -> req.filters(filter));
        } catch (CheckedAWSException e) {
            result.setInvalid(message, e);
            return result;
        }
        List<Subnet> subnets = describeSubnetsResult.subnets();
        // Attempt to assign subnet by deriving the value from target group/ELB.
        if (StringUtils.isEmpty(otpServer.ec2Info.subnetId)) {
            // Set subnetID to the first value found.
            // TODO: could this end up with an incorrect subnet value? (i.e., a subnet that is not publicly available on
            //  the Internet?
            Subnet subnet = subnets.iterator().next();
            if (subnet != null) {
                otpServer.ec2Info.subnetId = subnet.subnetId();
                return result;
            }
        } else {
            // Otherwise, verify the value set in the EC2Info.
            try {
                // Iterate over subnets. If a matching ID is found, silently return.
                for (Subnet subnet : subnets) if (subnet.subnetId().equals(otpServer.ec2Info.subnetId)) return result;
            } catch (Ec2Exception e) {
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
