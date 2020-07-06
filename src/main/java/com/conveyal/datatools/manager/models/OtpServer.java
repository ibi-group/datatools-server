package com.conveyal.datatools.manager.models;

import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.conveyal.datatools.common.utils.AWSUtils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.controllers.api.DeploymentController;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

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
    public List<EC2InstanceSummary> retrieveEC2InstanceSummaries() {
        // Prevent calling EC2 method on servers that do not have EC2 info defined because this is a JSON property.
        if (ec2Info == null) return Collections.EMPTY_LIST;
        Filter serverFilter = new Filter("tag:serverId", Collections.singletonList(id));
        return DeploymentController.fetchEC2InstanceSummaries(
            AWSUtils.getEC2ClientForRole(this.role, ec2Info.region),
            serverFilter
        );
    }

    public List<Instance> retrieveEC2Instances() {
        if (
            !"true".equals(DataManager.getConfigPropertyAsText("modules.deployment.ec2.enabled")) ||
                ec2Info == null
        ) return Collections.EMPTY_LIST;
        Filter serverFilter = new Filter("tag:serverId", Collections.singletonList(id));
        return DeploymentController.fetchEC2Instances(
            AWSUtils.getEC2ClientForRole(this.role, ec2Info.region),
            serverFilter
        );
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
}
