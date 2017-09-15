package com.conveyal.datatools.manager.models;

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

    /** What is the ID of the object it is recorded on */
    public String objectId;

    public String userEmail;

    /** When was this comment made? */
    public Date date;

    /**
     * The types of object that can have notes recorded on them.
     */
    public static enum NoteType {
        FEED_VERSION, FEED_SOURCE
    }
}
