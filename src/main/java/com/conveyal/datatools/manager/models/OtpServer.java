package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Created by landon on 5/20/16.
 */
public class OtpServer extends Model {
    private static final long serialVersionUID = 1L;
    public String name;
    public List<String> internalUrl;
    public List<String> instanceIds;
    public String instanceType;
    public int instanceCount;
    public String projectId;
    public String targetGroupArn;
    public String publicUrl;
    public boolean admin;
    public String s3Bucket;
    public String s3Credentials;
    public boolean createServer;

    /** Empty constructor for serialization. */
    public OtpServer () {}

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
