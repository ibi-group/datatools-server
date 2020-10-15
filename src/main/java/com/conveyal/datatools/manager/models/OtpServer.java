package com.conveyal.datatools.manager.models;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.conveyal.datatools.common.utils.aws.CheckedAWSException;
import com.conveyal.datatools.common.utils.aws.EC2Utils;
import com.conveyal.datatools.common.utils.aws.EC2ValidationResult;
import com.conveyal.datatools.common.utils.aws.IAMUtils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

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
     * URL location of the publicly-available user interface associated with either the {@link #internalUrl} or the
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
        if (ec2Info == null) return Collections.emptyList();
        Filter serverFilter = new Filter("tag:serverId", Collections.singletonList(id));
        return EC2Utils.fetchEC2InstanceSummaries(getEC2Client(), serverFilter);
    }

    public List<Instance> retrieveEC2Instances() throws CheckedAWSException {
        if (
            !"true".equals(DataManager.getConfigPropertyAsText("modules.deployment.ec2.enabled")) ||
                ec2Info == null
        ) return Collections.emptyList();
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
    public String getRegion() {
        return ec2Info != null && ec2Info.region != null
            ? ec2Info.region
            : null;
    }

    /**
     * Asynchronously validates all ec2 config of a the OtpServer instance.
     */
    @JsonIgnore
    @BsonIgnore
    public EC2ValidationResult validateEC2Config() throws ExecutionException, InterruptedException {

        List<Callable<EC2ValidationResult>> validationTasks = new ArrayList<>();
        validationTasks.add(() -> EC2Utils.validateInstanceType(ec2Info.instanceType));
        validationTasks.add(() -> EC2Utils.validateInstanceType(ec2Info.buildInstanceType));
        validationTasks.add(
            () -> IAMUtils.validateIamInstanceProfileArn(
                IAMUtils.getIAMClient(role, getRegion()),
                ec2Info.iamInstanceProfileArn
            )
        );
        validationTasks.add(() -> EC2Utils.validateKeyName(getEC2Client(), ec2Info.keyName));
        validationTasks.add(() -> EC2Utils.validateAmiId(getEC2Client(), ec2Info.amiId));
        validationTasks.add(() -> EC2Utils.validateAmiId(getEC2Client(), ec2Info.buildAmiId));
        validationTasks.add(() -> EC2Utils.validateGraphBuildReplacementAmiName(this));
        // add the load balancer task to the end since it can produce aggregate messages
        validationTasks.add(() -> EC2Utils.validateTargetGroupLoadBalancerSubnetIdAndSecurityGroup(this));

        return EC2ValidationResult.executeValidationTasks(
            validationTasks,
            "Invalid EC2 config for the following reasons!\n"
        );
    }

    /**
     * A MixIn to be applied to this OtpServer that will not include EC2InstanceSummaries when they are not needed in
     * the JSON output. This will avoid making unneeded AWS requests.
     *
     * Usually a mixin would be used on an external class, but since we are changing one thing about a single class, it
     * seemed unnecessary to define a new view.
     */
    public abstract static class OtpServerWithoutEc2Instances {

        @JsonIgnore
        public abstract List<EC2InstanceSummary> retrieveEC2InstanceSummaries();
    }
}
