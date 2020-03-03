package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.manager.DataManager;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.List;

import static com.conveyal.datatools.manager.jobs.DeployJob.AMI_CONFIG_PATH;
import static com.conveyal.datatools.manager.jobs.DeployJob.DEFAULT_INSTANCE_TYPE;

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
     * {@link com.conveyal.datatools.manager.jobs.DeployJob#DEFAULT_INSTANCE_TYPE} if null during deployment.
     */
    public String instanceType;
    /** Number of instances to spin up and add to target group. If zero, defaults to 1. */
    public int instanceCount;
    /** The subnet ID associated with the target group. */
    public String subnetId;
    /** The security group ID associated with the target group. */
    public String securityGroupId;
    /** The Amazon machine image (AMI) to be used for the OTP EC2 machines. */
    public String amiId;
    /**
     * The AWS-style instance type (e.g., t2.medium) to use for new EC2 machines used specifically for graph building.
     * Defaults to {@link com.conveyal.datatools.manager.models.EC2Info#instanceType} if null during deployment.
     */
    public String buildInstanceType;
    /** The Amazon machine image (AMI) to be used for the OTP EC2 machine used specifically for graph building. */
    public String buildAmiId;
    /** The IAM instance profile ARN that the OTP EC2 server should assume. For example, arn:aws:iam::123456789012:instance-profile/otp-ec2-role */
    public String iamInstanceProfileArn;
    /** The AWS key file (.pem) that should be used to set up OTP EC2 servers (gives a way for admins to SSH into machine). */
    public String keyName;
    /** The target group to deploy new EC2 instances to. */
    public String targetGroupArn;
    /** An optional custom AWS region */
    public String region;

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
            return DataManager.getConfigPropertyAsText(AMI_CONFIG_PATH);
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
            return DEFAULT_INSTANCE_TYPE;
        }
    }
}