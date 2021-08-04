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
import java.util.stream.Collectors;

import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.MappedSuperclass;

/**
 * The base class for all of the models used by GTFS Data Manager.
 * @author mattwigway
 */

@MappedSuperclass // applies mapping information to the subclassed entities FIXME remove?
public abstract class Model implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(Model.class);

    public Model () {
        // This autogenerates an ID
        // this is OK for dump/restore, because the ID will simply be overridden
        this.id = UUID.randomUUID().toString();
        this.lastUpdated = new Date();
        this.dateCreated = new Date();
    }

    public String id;

    public String retrieveId() { return this.id; }

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
    public List<String> noteIds = new ArrayList<>();

    /**
     * Get the notes for this object
     */
    @JsonIgnore
    public List<Note> retrieveNotes(boolean includeAdminNotes) {
        List<Note> ret = new ArrayList<>();
        if (noteIds != null) {
            List<Note> allNotes = Persistence.notes.getByIds(noteIds);
            if (includeAdminNotes) {
                ret = allNotes;
            } else {
                ret = allNotes.stream()
                    .filter(note -> !note.adminOnly)
                    .collect(Collectors.toList());
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
        if (!Auth0Connection.isAuthDisabled()) {
            Auth0UserProfile profile = null;
            // Try to fetch Auth0 user to store email address. This is surrounded by a try/catch because in the event of
            // a failure we do not want to cause issues from this low-level operation.
            try {
                profile = Auth0Users.getUserById(userId);
            } catch (Exception e) {
                LOG.warn(
                    "Could not find user profile {} from Auth0. This may be due to testing conditions or simply a bad user ID.",
                    id);
                e.printStackTrace();
            }
            userEmail = profile != null ? profile.getEmail() : null;
        } else {
            userEmail = "no_auth@conveyal.com";
        }
    }
}
