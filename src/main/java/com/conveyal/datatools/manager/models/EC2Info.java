package com.conveyal.datatools.manager.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.List;

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
     * Defaults to {@link com.conveyal.datatools.manager.jobs.DeployJob#DEFAULT_INSTANCE_TYPE} if null during deployment.
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
}