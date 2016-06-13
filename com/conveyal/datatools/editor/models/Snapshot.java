package com.conveyal.datatools.editor.models;

import com.conveyal.datatools.editor.datastore.GlobalTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.LocalDate;

import org.mapdb.Fun;
import org.mapdb.Fun.Tuple2;
import com.conveyal.datatools.editor.utils.JacksonSerializers;

import java.io.Serializable;
import java.util.Collection;

/**
 * Represents a snapshot of an agency database.
 * @author mattwigway
 *
 */
public class Snapshot implements Cloneable, Serializable {
    public static final long serialVersionUID = -2450165077572197392L;

    /** Is this snapshot the current snapshot - the most recently created or restored (i.e. the most current view of what's in master */
    public boolean current;

    /** The version of this snapshot */
    public int version;

    /** The name of this snapshot */
    public String name;

    /** The comment of this snapshot */
    public String comment;

    /** ID: agency ID, version */
    @JsonSerialize(using=JacksonSerializers.Tuple2IntSerializer.class)
    @JsonDeserialize(using=JacksonSerializers.Tuple2IntDeserializer.class)
    public Tuple2<String, Integer> id;

    /** The feed associated with this */
    public String feedId;

    /** the date/time this snapshot was taken (millis since epoch) */
    public long snapshotTime;

    // TODO: these should become java.time.LocalDate
    /** When is the earliest date that schedule information contained in this snapshot is valid? */
    @JsonSerialize(using = JacksonSerializers.LocalDateIsoSerializer.class)
    @JsonDeserialize(using = JacksonSerializers.LocalDateIsoDeserializer.class)
    public LocalDate validFrom;

    /** When is the last date that schedule information contained in this snapshot is valid? */
    @JsonSerialize(using = JacksonSerializers.LocalDateIsoSerializer.class)
    @JsonDeserialize(using = JacksonSerializers.LocalDateIsoDeserializer.class)
    public LocalDate validTo;

    /** Used for Jackson deserialization */
    public Snapshot () {}

    public Snapshot (String feedId, int version) {
        this.feedId = feedId;
        this.version = version;
        this.computeId();
    }

    /** create an ID for this snapshot based on agency ID and version */
    public void computeId () {
        this.id = new Tuple2(feedId, version);
    }

    public Snapshot clone () {
        try {
            return (Snapshot) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    @JsonIgnore
    public static Collection<Snapshot> getSnapshots(String feedId) {
        GlobalTx gtx = VersionedDataStore.getGlobalTx();
        return gtx.snapshots.subMap(new Tuple2(feedId, null), new Tuple2(feedId, Fun.HI)).values();
    }
}
