package com.conveyal.datatools.manager.models;

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
import com.amazonaws.services.ec2.model.KeyPairInfo;
import com.amazonaws.services.ec2.model.Subnet;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancing;
import com.amazonaws.services.elasticloadbalancingv2.model.AmazonElasticLoadBalancingException;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeLoadBalancersRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeLoadBalancersResult;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeTargetGroupsRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.LoadBalancer;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetGroup;
import com.amazonaws.services.s3.AmazonS3;
import com.conveyal.datatools.common.utils.aws.CheckedAWSException;
import com.conveyal.datatools.common.utils.aws.EC2Utils;
import com.conveyal.datatools.common.utils.aws.EC2ValidationResult;
import com.conveyal.datatools.common.utils.aws.IAMUtils;
import com.conveyal.datatools.common.utils.aws.S3Utils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * An OtpServer represents a deployment target for deploying transit and OSM data to. This can take the shape of a number
 * of things:
 * 1. Simply writing a data bundle to S3.
 * 2. Deploying to an internal URL for a build graph over wire request.
 * 3. Spinning up an EC2 instance to build the graph, write it to S3, and have a collection of instances start up, become
 *    part of an Elastic Load Balancer (ELB) target group, and download/read in the OTP graph.
 *    read in that graph.
 * 4. Spinning up an EC2 instance to only build the OTP graph and write it to S3 (dependent on {@link Deployment#buildGraphOnly}
 *    value).
 *
 * Created by landon on 5/20/16.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OtpServer extends Model {
    private static final Logger LOG = LoggerFactory.getLogger(OtpServer.class);

    private static final long serialVersionUID = 1L;
    public String name;
    /** URL to direct build graph over wire requests to (if not using ELB target group). */
    public List<String> internalUrl;
    /** Optional project to associate this server with (server can also be made available to entire application). */
    public String projectId;
    /** Contains all of the information needed to commission EC2 instances for an AWS Elastic Load Balancer (ELB) target group. */
    public EC2Info ec2Info;
    /**
     * AWS role that must be assumed in order to access S3 or EC2 services. Should be null if default credentials should
     * be used.
     */
    public String role;
    /**
     * URL location of the publicly-available user interface asssociated with either the {@link #internalUrl} or the
     * load balancer/target group.
     */
    public String publicUrl;
    /** Whether deploying to this server is limited to admins only. */
    public boolean admin;
    /** S3 bucket name to upload deployment artifacts to (e.g., Graph.obj and/or transit + OSM data). */
    public String s3Bucket;

    /** Empty constructor for serialization. */
    public OtpServer () {}

    /** The EC2 instances that are associated with this serverId. */
    @JsonProperty("ec2Instances")
    public List<EC2InstanceSummary> retrieveEC2InstanceSummaries() throws CheckedAWSException {
        // Prevent calling EC2 method on servers that do not have EC2 info defined because this is a JSON property.
        if (ec2Info == null) return Collections.EMPTY_LIST;
        Filter serverFilter = new Filter("tag:serverId", Collections.singletonList(id));
        return EC2Utils.fetchEC2InstanceSummaries(getEC2Client(), serverFilter);
    }

    public List<Instance> retrieveEC2Instances() throws CheckedAWSException {
        if (
            !"true".equals(DataManager.getConfigPropertyAsText("modules.deployment.ec2.enabled")) ||
                ec2Info == null
        ) return Collections.EMPTY_LIST;
        Filter serverFilter = new Filter("tag:serverId", Collections.singletonList(id));
        return EC2Utils.fetchEC2Instances(getEC2Client(), serverFilter);
    }

    @JsonProperty("organizationId")
    public String organizationId() {
        Project project = parentProject();
        return project == null ? null : project.organizationId;
    }

    public Project parentProject() {
        return Persistence.projects.getById(projectId);
    }

    /**
     * Nothing fancy here. Just delete the Mongo record.
     *
     * TODO should this also check refs in deployments?
     */
    public void delete () {
        Persistence.servers.removeById(this.id);
    }

    @JsonIgnore
    @BsonIgnore
    public AmazonEC2 getEC2Client() throws CheckedAWSException {
        return EC2Utils.getEC2Client(role, getRegion());
    }

    @JsonIgnore
    @BsonIgnore
    private String getRegion() {
        return ec2Info != null && ec2Info.region != null
            ? ec2Info.region
            : null;
    }

    /**
     * Asynchronously validates all ec2 config of a the OtpServer instance.
     */
    @JsonIgnore
    @BsonIgnore
    public EC2ValidationResult validateEC2Config()
        throws ExecutionException, InterruptedException, CheckedAWSException {

        List<Callable<EC2ValidationResult>> validationTasks = new ArrayList<>();
        validationTasks.add(() -> EC2Utils.validateInstanceType(ec2Info.instanceType));
        validationTasks.add(() -> EC2Utils.validateInstanceType(ec2Info.buildInstanceType));
        validationTasks.add(() -> validateIamInstanceProfileArn());
        validationTasks.add(() -> validateKeyName());
        validationTasks.add(() -> validateAmiId(ec2Info.amiId));
        validationTasks.add(() -> validateAmiId(ec2Info.buildAmiId));
        validationTasks.add(() -> validateGraphBuildReplacementAmiName());
        // add the load balancer task to the end since it can produce aggregate messages
        validationTasks.add(() -> validateTargetGroupLoadBalancerSubnetIdAndSecurityGroup());

        return executeValidationTasks(
            validationTasks,
            "Invalid EC2 config for the following reasons!\n"
        );
    }

    /**
     * Validate that AMI exists and value is not empty.
     *
     * TODO: Should we warn user if the AMI provided is older than the default AMI registered with this application as
     *   DEFAULT_AMI_ID?
     */
    @JsonIgnore
    @BsonIgnore
    private EC2ValidationResult validateAmiId(String amiId) {
        EC2ValidationResult result = new EC2ValidationResult();
        if (isEmpty(amiId)) return result;
        try {
            if (!EC2Utils.amiExists(getEC2Client(), amiId)) {
                result.setInvalid("Server must have valid AMI ID (or field must be empty)");
            }
        } catch (AmazonEC2Exception | CheckedAWSException e) {
            result.setInvalid("AMI does not exist or some error prevented proper checking of the AMI ID.", e);
        }
        return result;
    }

    /**
     * Validates whether the replacement graph build image name is unique. Although it is possible to have duplicate AMI
     * names when copying images, they must be unique when creating images.
     * See https://forums.aws.amazon.com/message.jspa?messageID=845159
     */
    @JsonIgnore
    @BsonIgnore
    private EC2ValidationResult validateGraphBuildReplacementAmiName() {
        EC2ValidationResult result = new EC2ValidationResult();
        if (!ec2Info.recreateBuildImage) return result;
        String buildImageName = ec2Info.buildImageName;
        try {
            DescribeImagesRequest describeImagesRequest = new DescribeImagesRequest()
                // limit AMIs to only those owned by the current ec2 user.
                .withOwners("self");
            DescribeImagesResult describeImagesResult = getEC2Client().describeImages(describeImagesRequest);
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

    /** Validate that AWS key name (the first part of a .pem key) exists and is not empty. */
    @JsonIgnore
    @BsonIgnore
    private EC2ValidationResult validateKeyName() throws CheckedAWSException {
        String message = "Server must have valid key name";
        EC2ValidationResult result = new EC2ValidationResult();
        if (isEmpty(ec2Info.keyName)) {
            result.setInvalid(message);
            return result;
        };
        DescribeKeyPairsResult response = this.getEC2Client().describeKeyPairs();
        for (KeyPairInfo key_pair : response.getKeyPairs()) {
            if (key_pair.getKeyName().equals(ec2Info.keyName)) return result;
        }
        result.setInvalid(message);
        return result;
    }

    /** Validate IAM instance profile ARN exists and is not empty. */
    @JsonIgnore
    @BsonIgnore
    private EC2ValidationResult validateIamInstanceProfileArn() throws CheckedAWSException {
        EC2ValidationResult result = new EC2ValidationResult();
        String message = "Server must have valid IAM instance profile ARN (e.g., arn:aws:iam::123456789012:instance-profile/otp-ec2-role).";
        if (isEmpty(ec2Info.iamInstanceProfileArn)) {
            result.setInvalid(message);
            return result;
        }
        if (
            IAMUtils.getIamInstanceProfile(
                IAMUtils.getIAMClient(role, getRegion()),
                ec2Info.iamInstanceProfileArn
            ) == null
        ) {
            result.setInvalid(message);
        };
        return result;
    }

    /** Validate that EC2 security group exists and is not empty. */
    @JsonIgnore
    @BsonIgnore
    private EC2ValidationResult validateSecurityGroupId(LoadBalancer loadBalancer) {
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
    @JsonIgnore
    @BsonIgnore
    private EC2ValidationResult validateSubnetId(LoadBalancer loadBalancer) {
        EC2ValidationResult result = new EC2ValidationResult();
        String message = "Server must have valid subnet ID";
        // Make request for all subnets associated with load balancer's vpc
        Filter filter = new Filter("vpc-id").withValues(loadBalancer.getVpcId());
        DescribeSubnetsRequest describeSubnetsRequest = new DescribeSubnetsRequest().withFilters(filter);
        DescribeSubnetsResult describeSubnetsResult;
        try {
            describeSubnetsResult = getEC2Client().describeSubnets(describeSubnetsRequest);
        } catch (CheckedAWSException e) {
            result.setInvalid(message, e);
            return result;
        }
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
     * Gets the load balancer that the target group ARN is assigned to. Note: according to AWS docs/Stack Overflow, a
     * target group can only be assigned to a single load balancer (one-to-one relationship), so there should be no
     * risk of this giving inconsistent results.
     *  - https://serverfault.com/a/865422
     *  - https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-limits.html
     */
    @JsonIgnore
    @BsonIgnore
    private LoadBalancer getLoadBalancerForTargetGroup () {
        try {
            AmazonElasticLoadBalancing elbClient = EC2Utils.getELBClient(role, getRegion());
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
        } catch (AmazonElasticLoadBalancingException | CheckedAWSException e) {
            LOG.warn("Invalid value for Target Group ARN: {}", ec2Info.targetGroupArn);
        }
        // If no target group/load balancer found, return null.
        return null;
    }

    /**
     * Validate that ELB target group exists and is not empty and return associated load balancer for validating related
     * fields.
     */
    @JsonIgnore
    @BsonIgnore
    private EC2ValidationResult validateTargetGroupLoadBalancerSubnetIdAndSecurityGroup()
        throws ExecutionException, InterruptedException {
        EC2ValidationResult result = new EC2ValidationResult();
        if (isEmpty(ec2Info.targetGroupArn)) {
            result.setInvalid("Invalid value for Target Group ARN.");
            return result;
        }
        // Get load balancer for target group. This essentially checks that the target group exists and is assigned
        // to a load balancer.
        LoadBalancer loadBalancer = getLoadBalancerForTargetGroup();
        if (loadBalancer == null) {
            result.setInvalid("Invalid value for Target Group ARN. Could not locate Target Group or Load Balancer.");
            return result;
        }

        // asynchronously execute the two validation tasks that depend on the load balancer info
        List<Callable<EC2ValidationResult>> loadBalancerValidationTasks = new ArrayList<>();
        loadBalancerValidationTasks.add(() -> validateSubnetId(loadBalancer));
        loadBalancerValidationTasks.add(() -> validateSecurityGroupId(loadBalancer));

        return executeValidationTasks(
            loadBalancerValidationTasks,
            "Invalid EC2 load balancer config for the following reasons:\n"
        );
    }

    /**
     * @return false if string value is empty or null
     */
    public static boolean isEmpty(String val) {
        return val == null || "".equals(val);
    }

    public static EC2ValidationResult executeValidationTasks(
        List<Callable<EC2ValidationResult>> validationTasks, String overallInvalidMessage
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
     * Verify that application can write to S3 bucket either through its own credentials or by assuming the provided IAM
     * role. We're following the recommended approach from https://stackoverflow.com/a/17284647/915811, but perhaps
     * there is a way to do this effectively without incurring AWS costs (although writing/deleting an empty file to S3
     * is probably miniscule).
     */
    public void verifyS3WritePermissions() throws IOException, CheckedAWSException {
        String key = UUID.randomUUID().toString();
        AmazonS3 client = S3Utils.getS3Client(role, getRegion());
        client.putObject(s3Bucket, key, File.createTempFile("test", ".zip"));
        client.deleteObject(s3Bucket, key);
    }
}
