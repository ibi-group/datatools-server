package com.conveyal.datatools.manager.models;

import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceState;
import com.amazonaws.services.ec2.model.Tag;

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
    public InstanceState state;
    public String availabilityZone;
    public Date launchTime;
    public String stateTransitionReason;

    /** Empty constructor for serialization */
    public EC2InstanceSummary () { }

    public EC2InstanceSummary (Instance ec2Instance) {
        publicIpAddress = ec2Instance.getPublicIpAddress();
        privateIpAddress = ec2Instance.getPrivateIpAddress();
        publicDnsName = ec2Instance.getPublicDnsName();
        instanceType = ec2Instance.getInstanceType();
        instanceId = ec2Instance.getInstanceId();
        imageId = ec2Instance.getImageId();
        List<Tag> tags = ec2Instance.getTags();
        // Set project and deployment ID if they exist.
        for (Tag tag : tags) {
            if (tag.getKey().equals("projectId")) projectId = tag.getValue();
            if (tag.getKey().equals("deploymentId")) deploymentId = tag.getValue();
            if (tag.getKey().equals("jobId")) jobId = tag.getValue();
            if (tag.getKey().equals("Name")) name = tag.getValue();
        }
        state = ec2Instance.getState();
        availabilityZone = ec2Instance.getPlacement().getAvailabilityZone();
        launchTime = ec2Instance.getLaunchTime();
        stateTransitionReason = ec2Instance.getStateTransitionReason();
    }
}
