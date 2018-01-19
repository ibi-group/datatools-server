package com.conveyal.datatools.manager.models;

import com.conveyal.gtfs.loader.FeedLoadResult;

/**
 * Represents a snapshot of an agency database.
 * @author mattwigway
 *
 */
public class Snapshot extends Model {
    public static final long serialVersionUID = 1L;

    /** Is this snapshot the current snapshot - the most recently created or restored (i.e. the most current view of what's in master */
    public boolean current;

    /** The version of this snapshot */
    public int version;

    /** The name of this snapshot */
    public String name;

    /** The comment of this snapshot */
    public String comment;

    /** The feed source associated with this */
    public String feedSourceId;

    /** The feed version this snapshot was generated from or published to, if any */
    public String feedVersionId;

    public FeedLoadResult feedLoadResult;

    /** the date/time this snapshot was taken (millis since epoch) */
    public long snapshotTime;

    /** Used for deserialization */
    public Snapshot() {}

    public Snapshot(String feedSourceId, int version, FeedLoadResult feedLoadResult) {
        this.feedSourceId = feedSourceId;
        this.version = version;
        this.feedLoadResult = feedLoadResult;
    }

    public String generateFileName () {
        return this.feedSourceId + "_" + this.snapshotTime + ".zip";
    }
}
