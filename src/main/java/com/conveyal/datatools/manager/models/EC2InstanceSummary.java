package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.common.utils.aws.InstanceStateAws1;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceState;
import software.amazon.awssdk.services.ec2.model.Tag;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

/**
 * Summarizes information derived from an EC2 instance for consumption by a user interface.
 */
public class EC2InstanceSummary implements Serializable {
    private static final long serialVersionUID = 1L;
    public String privateIpAddress;
    public String publicIpAddress;
    public String publicDnsName;
    public String instanceType;
    public String instanceId;
    public String imageId;
    public String projectId;
    public String jobId;
    public String deploymentId;
    public String name;
    public InstanceStateAws1 state;
    public String availabilityZone;
    public Date launchTime;
    public String stateTransitionReason;

    /** Empty constructor for serialization */
    public EC2InstanceSummary () { }

    public EC2InstanceSummary (Instance ec2Instance) {
        publicIpAddress = ec2Instance.publicIpAddress();
        privateIpAddress = ec2Instance.privateIpAddress();
        publicDnsName = ec2Instance.publicDnsName();
        instanceType = ec2Instance.instanceType().name();
        instanceId = ec2Instance.instanceId();
        imageId = ec2Instance.imageId();
        List<Tag> tags = ec2Instance.tags();
        // Set project and deployment ID if they exist.
        for (Tag tag : tags) {
            if (tag.key().equals("projectId")) projectId = tag.value();
            if (tag.key().equals("deploymentId")) deploymentId = tag.value();
            if (tag.key().equals("jobId")) jobId = tag.value();
            if (tag.key().equals("Name")) name = tag.value();
        }
        InstanceState instanceState = ec2Instance.state();
        state = new InstanceStateAws1()
            .withName(instanceState.nameAsString())
            .withCode(instanceState.code());
        availabilityZone = ec2Instance.placement().availabilityZone();
        launchTime = Date.from(ec2Instance.launchTime());
        stateTransitionReason = ec2Instance.stateTransitionReason();
    }
}
