package com.conveyal.datatools.manager.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Date;

/**
 * A summary of deployment items used to list available deployments without the need to return all deployment information.
 */
@JsonInclude()
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeploymentSummary {

    /**
     * Deployment Id.
     */
    public String id;

    /**
     * Name given to this deployment.
     */
    public String name;

    /**
     * Date when the deployment was created.
     */
    public Date created;

    /**
     * Date when the deployment was last deployed to a server.
     */
    public Date lastDeployed;

    /**
     * What server is this currently deployed to, if any?
     */
    public String deployedTo;

    /**
     * Number of feed versions associated with this project.
     */
    public int numberOfFeedVersions;

    /**
     * If not using the 'default' router, this value will be set to true.
     */
    public boolean test;

    /**
     * Deployment is pinned.
     */
    public boolean isPinned;

    /**
     * The name of the server this is currently deployed to, if any.
     */
    public String serverName;

    /**
     * Create an empty deployment summary, for use with dump/restore and testing.
     */
    public DeploymentSummary() {
    }

    /**
     * Extract from a deployment and parent project, deployment summary values.
     */
    public DeploymentSummary(Deployment deployment, Project project) {
        this.id = deployment.id;
        this.name = deployment.name;
        this.created = deployment.dateCreated;
        this.lastDeployed = deployment.retrieveLastDeployed();
        this.deployedTo = deployment.deployedTo;
        this.numberOfFeedVersions = deployment.feedVersionIds.size();
        this.test = deployment.routerId != null;
        this.isPinned = project.pinnedDeploymentId != null && project.pinnedDeploymentId.equals(deployment.id);
        OtpServer server = project.availableOtpServers()
            .stream()
            .filter(s -> s.id.equals(deployedTo))
            .findFirst()
            .orElse(null);
        this.serverName = (server != null) ? server.name : null;
    }
}
