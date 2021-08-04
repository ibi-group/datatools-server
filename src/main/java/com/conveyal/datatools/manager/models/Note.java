package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import java.io.Serializable;
import java.util.Date;

/**
 * A note about a particular model.
 * @author mattwigway
 *
 */
@JsonInclude(Include.ALWAYS)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Note extends Model implements Serializable {
    private static final long serialVersionUID = 1L;

    /** The content of the note */
    public String body;

    /** What type of object it is recorded on */
    public NoteType type;

    public String userEmail;

    /** When was this comment made? */
    public Date date;

    /** Whether the note should be visible to project admins only */
    public boolean adminOnly;

    /**
     * The types of object that can have notes recorded on them.
     */
    public enum NoteType {
        FEED_VERSION, FEED_SOURCE
    }

    public void delete() {
        Persistence.feedSources.removeNoteFromCollection(id);
        Persistence.feedVersions.removeNoteFromCollection(id);
        Persistence.notes.removeById(this.id);
    }
}
