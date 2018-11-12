package com.conveyal.datatools.editor.models;

import com.conveyal.datatools.editor.datastore.GlobalTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.File;
import java.io.IOException;
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

    /** The feed version this snapshot was generated from or published to, if any */
    public String feedVersionId;

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

    public String generateFileName () {
        return this.feedId + "_" + this.snapshotTime + ".zip";
    }

    @JsonIgnore
    public static Collection<Snapshot> getSnapshots (String feedId) {
        GlobalTx gtx = VersionedDataStore.getGlobalTx();
        return gtx.snapshots.subMap(new Tuple2(feedId, null), new Tuple2(feedId, Fun.HI)).values();
    }

    public static void deactivateSnapshots (String feedId, Snapshot ignore) {
        GlobalTx gtx = VersionedDataStore.getGlobalTx();
        Collection<Snapshot> snapshots = Snapshot.getSnapshots(feedId);
        try {
            for (Snapshot o : snapshots) {
                if (ignore != null && o.id.equals(ignore.id))
                    continue;

                Snapshot cloned = o.clone();
                cloned.current = false;
                gtx.snapshots.put(o.id, cloned);
            }
            gtx.commit();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            gtx.rollbackIfOpen();
        }
    }

    public static Snapshot get(String snapshotId) {
        Tuple2<String, Integer> decodedId;
        try {
            decodedId = JacksonSerializers.Tuple2IntDeserializer.deserialize(snapshotId);
        } catch (IOException e) {
            return null;
        }

        GlobalTx gtx = VersionedDataStore.getGlobalTx();
        if (!gtx.snapshots.containsKey(decodedId)) return null;
        return gtx.snapshots.get(decodedId);
    }

    public static Snapshot get(Tuple2<String, Integer> decodedId) {
        GlobalTx gtx = VersionedDataStore.getGlobalTx();
        if (!gtx.snapshots.containsKey(decodedId)) return null;
        return gtx.snapshots.get(decodedId);
    }
}
