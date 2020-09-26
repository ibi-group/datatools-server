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
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Image;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancing;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingClientBuilder;
import com.amazonaws.services.elasticloadbalancingv2.model.DeregisterTargetsRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetDescription;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.controllers.api.ServerController;
import com.conveyal.datatools.manager.models.EC2InstanceSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class has some base utility fields and also methods for managing AWS credential creation when using roles. It is
 *     * expected that whenever a client is created, it will end up using AWS client for a various AWS service. This class will
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
    private static EC2ClientManagerImpl EC2ClientManager = new EC2ClientManagerImpl(DEFAULT_EC2_CLIENT);
    private static ELBClientManagerImpl ELBClientManager = new ELBClientManagerImpl(EC2Utils.DEFAULT_ELB_CLIENT);

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
        ) throws CheckedAWSException {
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
            terminateInstances(getEC2Client(role, region), instanceIds);
        } catch (AmazonServiceException | CheckedAWSException e) {
            LOG.warn("Could not terminate EC2 instances: " + String.join(",", instanceIds), e);
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
     * Shorthand method for getting list of string identifiers from a list of EC2 instances.
     */
    public static List<String> getIds (List<Instance> instances) {
        return instances.stream().map(Instance::getInstanceId).collect(Collectors.toList());
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
}
