package com.conveyal.datatools.manager.models;

import com.conveyal.gtfs.loader.FeedLoadResult;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAlias;

import java.util.Date;

/**
 * Represents a snapshot of an agency database.
 * @author mattwigway
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Snapshot extends Model {
    public static final long serialVersionUID = 1L;
    public static final String FEED_SOURCE_REF = "feedSourceId";

    /** Is this snapshot the current snapshot - the most recently created or restored (i.e. the most current view of what's in master */
    public boolean current;

    /** The version of this snapshot */
    public int version;

    /** The name of this snapshot */
    public String name;

    /** The comment of this snapshot */
    public String comment;

    /** The feed source associated with this. NOTE: this field is NOT named feedSourceId to match legacy editor snapshots. */
    @JsonAlias({"feedId", "feedSourceId"})
    public String feedSourceId;

    /** The feed version this snapshot was generated from or published to, if any */
    public String feedVersionId;

    /** The namespace this snapshot is a copy of */
    public String snapshotOf;

    /** The namespace the snapshot copied tables to */
    public String namespace;

    public FeedLoadResult feedLoadResult;

    /** the date/time this snapshot was taken (millis since epoch) */
    public long snapshotTime;

    /** Used for deserialization */
    public Snapshot() {}

    public Snapshot(String feedSourceId, int version, String snapshotOf, FeedLoadResult feedLoadResult) {
        this.feedSourceId = feedSourceId;
        this.version = version;
        this.snapshotOf = snapshotOf;
        this.namespace = feedLoadResult.uniqueIdentifier;
        this.feedLoadResult = feedLoadResult;
        snapshotTime = System.currentTimeMillis();
    }

    public Snapshot(String name, String feedSourceId, String snapshotOf) {
        this.name = name;
        this.feedSourceId = feedSourceId;
        this.snapshotOf = snapshotOf;
        snapshotTime = System.currentTimeMillis();
    }

    public Snapshot(String feedSourceId, String snapshotOf) {
        this(null, feedSourceId, snapshotOf);
        generateName();
    }

    public void generateName() {
        this.name = "New snapshot " + new Date().toString();
    }
}
