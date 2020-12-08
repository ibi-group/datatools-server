package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.common.utils.aws.EC2Utils;
import com.conveyal.datatools.manager.DataManager;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Contains the fields specific to starting up new EC2 servers for an ELB target group. If null, at least one internal
 * URLs must be provided.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class EC2Info implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Empty constructor for serialization. */
    public EC2Info () {}
    /**
     * The AWS-style instance type (e.g., t2.medium) to use for new EC2 machines. Defaults to
     * {@link EC2Utils#DEFAULT_INSTANCE_TYPE} if null during deployment.
     */
    public String instanceType;
    /** Number of instances to spin up and add to target group. If zero, defaults to 1. */
    public int instanceCount;
    /** The subnet ID associated with the target group. */
    public String subnetId;
    /** The security group ID associated with the target group. */
    public String securityGroupId;
    /**
     * The Amazon machine image (AMI) to be used for the OTP EC2 machines. Defaults to the app config value at
     * {@link EC2Utils#AMI_CONFIG_PATH} if null during deployment.
     */
    public String amiId;
    /**
     * The AWS-style instance type (e.g., t2.medium) to use for new EC2 machines used specifically for graph building.
     * Defaults to {@link com.conveyal.datatools.manager.models.EC2Info#instanceType} if null during deployment.
     */
    public String buildInstanceType;
    /**
     * The Amazon machine image (AMI) (e.g. ami-12345678) to be used for the OTP EC2 machine used specifically for
     * graph building. Defaults to {@link com.conveyal.datatools.manager.models.EC2Info#amiId} if null during deployment.
     */
    public String buildAmiId;
    /**
     * The IAM instance profile ARN that the OTP EC2 server should assume. For example,
     * arn:aws:iam::123456789012:instance-profile/otp-ec2-role
     */
    public String iamInstanceProfileArn;
    /** The AWS key file (.pem) that should be used to set up OTP EC2 servers (gives a way for admins to SSH into machine). */
    public String keyName;
    /** The target group to deploy new EC2 instances to. */
    public String targetGroupArn;
    /** An optional custom AWS region (e.g., us-east-1) */
    public String region;
    /**
     * Informs the DeployJob that a new Image of the graph building machine should be created after graph building has
     * finished. The previous Image associated with the buildAmiId will be deregistered. Then the buildAmiId will be
     * updated with AMI ID associated with the newly created Image.
     */
    public boolean recreateBuildImage;
    /** The name to give the newly created AMI if makeBuildImage is set to true */
    public String buildImageName;
    /** The description to give the newly created AMI if makeBuildImage is set to true */
    public String buildImageDescription;

    /**
     * Returns true if the instance type or ami ids are set and are different for a graph build.
     */
    public boolean hasSeparateGraphBuildConfig() {
        return (
            buildInstanceType != null && !buildInstanceType.equals(instanceType)
        ) || (
            buildAmiId != null && !buildAmiId.equals(amiId)
        );
    }

    /**
     * Returns the appropriate ami ID to use when creating a new ec2 instance during a deploy job.
     *
     * @param graphAlreadyBuilt whether or not a graph has already been built. If false, this means a build ami should
     *                          be used if available.
     */
    public String getAmiId(boolean graphAlreadyBuilt) {
        if (!graphAlreadyBuilt && buildAmiId != null) {
            return buildAmiId;
        } else if (amiId != null) {
            return amiId;
        } else {
            return EC2Utils.DEFAULT_AMI_ID;
        }
    }

    /**
     * Returns the appropriate instance type to use when creating a new ec2 instance during a deploy job.
     *
     * @param graphAlreadyBuilt whether or not a graph has already been built. If false, this means a build instance
     *                          type should be used if available.
     */
    public String getInstanceType(boolean graphAlreadyBuilt) {
        if (!graphAlreadyBuilt && buildInstanceType != null) {
            return buildInstanceType;
        } else if (instanceType != null) {
            return instanceType;
        } else {
            return EC2Utils.DEFAULT_INSTANCE_TYPE;
        }
    }
}