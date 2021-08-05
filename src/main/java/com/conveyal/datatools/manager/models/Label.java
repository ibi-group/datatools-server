package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Label extends Model implements Cloneable{
    public static final Logger LOG = LoggerFactory.getLogger(Label.class);

    /**
     * The collection of which this Label is a part
     */
    public String projectId;

    @JsonProperty("organizationId")
    @JsonIgnore
    public String organizationId () {
        Project project =  projectId != null ? Persistence.projects.getById(projectId) : null;
        return project == null ? null : project.organizationId;
    }

    /** The name of this label, e.g. "Complete" or "Broken" */
    public String name;

    /** A description for the label */
    public String description;

    /** The color of the label. Stored as a string because it will only ever hold a CSS hex value */
    public String color;

    /** Is this label only accessible to admin users? */
    public boolean adminOnly;

    /** Added by Mongo, should be ignored when deserializing */
    @JsonIgnore
    public Auth0UserProfile user;

    /**
     * Create a new label
     */
    public Label (String name, String description, String color, boolean adminOnly, String projectId) {
        super();
        this.name = name;
        this.description = description != null ? description : "";
        this.color = color != null ? color : "#000000";
        this.adminOnly = adminOnly;

        this.projectId = projectId;
    }

    /**
     * No-arg constructor to yield an uninitialized label, for dump/restore.
     * Should not be used in general code.
     */
    public Label () {
        this(null, null, null, false, null);
    }

    /**
     * Delete this label
     */
    public void delete() {
        FeedSource.removeLabelFromFeedSources(this);
        Persistence.labels.removeById(this.id);
    }

    public String toString () {
        return "<Label " + this.name + " (" + this.id + ")>";
    }
}
