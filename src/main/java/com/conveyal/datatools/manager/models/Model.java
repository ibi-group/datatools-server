package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.auth.Auth0Users;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.conveyal.datatools.manager.auth.Auth0UserProfile;

import javax.persistence.MappedSuperclass;

/**
 * The base class for all of the models used by GTFS Data Manager.
 * @author mattwigway
 */

@MappedSuperclass
public abstract class Model implements Serializable {
    private static final long serialVersionUID = 1L;

    public Model () {
        // This autogenerates an ID
        // this is OK for dump/restore, because the ID will simply be overridden
        this.id = UUID.randomUUID().toString();
        this.lastUpdated = new Date();
        this.dateCreated = new Date();
    }

    public String id;

    // FIXME: should this be stored here? Should we use lastUpdated as a nonce to protect against race conditions in DB
    // writes?
    public Date lastUpdated;
    public Date dateCreated;

    /**
     * The ID of the user who owns this object.
     * For accountability, every object is owned by a user.
     */
    @JsonView(JsonViews.DataDump.class)
    public String userId;

    @JsonView(JsonViews.DataDump.class)
    public String userEmail;

    /**
     * Notes on this object
     */
    @JsonView(JsonViews.DataDump.class)
    public List<String> noteIds;

    /**
     * Get the notes for this object
     */
    // notes are handled through a separate controller and in a separate DB
    @JsonIgnore
    public List<Note> retrieveNotes() {
        ArrayList<Note> ret = new ArrayList<>(noteIds != null ? noteIds.size() : 0);

        if (noteIds != null) {
            for (String id : noteIds) {
                ret.add(Persistence.notes.getById(id));
            }
        }

        // even if there were no notes, return an empty list
        return ret;
    }
    /**
     * Get the user who owns this object.
     * @return the String user_id
     */
    @JsonProperty("user")
    public String user () {
        return this.userEmail;
    }
    /**
     * Set the owner of this object
     */
    public void storeUser(Auth0UserProfile profile) {
        userId = profile.getUser_id();
        userEmail = profile.getEmail();
    }

    /**
     * Set the owner of this object by ID.
     */
    public void storeUser(String id) {
        userId = id;
        if (!Auth0Connection.authDisabled()) {
            Auth0UserProfile profile = Auth0Users.getUserById(userId);
            userEmail = profile != null ? profile.getEmail() : null;
        } else {
            userEmail = "no_auth@conveyal.com";
        }

    }

    public void addNote(Note n) {
        if (noteIds == null) {
            noteIds = new ArrayList<>();
        }

        noteIds.add(n.id);
        n.objectId = this.id;
    }
}
