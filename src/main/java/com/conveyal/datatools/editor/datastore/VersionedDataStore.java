package com.conveyal.datatools.editor.datastore;

import com.conveyal.datatools.manager.DataManager;
import com.google.common.collect.Maps;
import com.conveyal.datatools.editor.models.Snapshot;
import com.conveyal.datatools.editor.models.transit.Stop;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.TxMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.conveyal.datatools.editor.utils.ClassLoaderSerializer;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

/**
 * Create a new versioned com.conveyal.datatools.editor.datastore. A versioned data store handles multiple databases,
 * the global DB and the agency-specific DBs. It handles creating transactions, and saving and restoring
 * snapshots.
 * @author mattwigway
 *
 */
public class VersionedDataStore {
    public static final Logger LOG = LoggerFactory.getLogger(VersionedDataStore.class);
    private static File dataDirectory = new File(DataManager.getConfigPropertyAsText("application.data.editor_mapdb"));
    private static TxMaker globalTxMaker;

    private static Map<String, TxMaker> feedTxMakers = Maps.newConcurrentMap();

    static {
        File globalDataDirectory = new File(dataDirectory, "global");
        globalDataDirectory.mkdirs();

        // initialize the global database
        globalTxMaker = DBMaker.newFileDB(new File(globalDataDirectory, "global.db"))
                .mmapFileEnable()
                .asyncWriteEnable()
                .compressionEnable()
                .closeOnJvmShutdown()
                .makeTxMaker();
    }

    /** Start a transaction in the global database */
    public static GlobalTx getGlobalTx () {
        return new GlobalTx(globalTxMaker.makeTx());
    }

    /**
     * Start a transaction in an agency database. No checking is done to ensure the agency exists;
     * if it does not you will get a (hopefully) empty DB, unless you've done the same thing previously.
     */
    public static FeedTx getFeedTx(String feedId) {
        return new FeedTx(getRawFeedTx(feedId));
    }

    /**
     * Get a raw MapDB transaction for the given database. Use at your own risk - doesn't properly handle indexing, etc.
     * Intended for use primarily with database restore
     */
    static DB getRawFeedTx(String feedId) {
        if (!feedTxMakers.containsKey(feedId)) {
            synchronized (feedTxMakers) {
                if (!feedTxMakers.containsKey(feedId)) {
                    File path = new File(dataDirectory, feedId);
                    path.mkdirs();

                    TxMaker agencyTxm = DBMaker.newFileDB(new File(path, "master.db"))
                            .mmapFileEnable()
                            .compressionEnable()
                            .asyncWriteEnable()
                            .closeOnJvmShutdown()
                            .asyncWriteFlushDelay(5)
                            .makeTxMaker();

                    feedTxMakers.put(feedId, agencyTxm);
                }
            }
        }

        return feedTxMakers.get(feedId).makeTx();
    }

    public static Snapshot takeSnapshot (String feedId, String name, String comment) {
        return takeSnapshot(feedId, null, name, comment);
    }

    /** Take a snapshot of an agency database. The snapshot will be saved in the global database. */
    public static Snapshot takeSnapshot (String feedId, String feedVersionId, String name, String comment) {
        FeedTx tx = getFeedTx(feedId);
        Collection<Snapshot> snapshots = Snapshot.getSnapshots(feedId);
        GlobalTx gtx = getGlobalTx();
        int version = -1;
        DB snapshot = null;
        Snapshot ret;
        try {
            version = tx.getNextSnapshotId();
            LOG.info("Creating snapshot {} for feed {}", version, feedId);
            long startTime = System.currentTimeMillis();

            ret = new Snapshot(feedId, version);

            // if we encounter a duplicate snapshot ID, increment until there is a safe one
            if (gtx.snapshots.containsKey(ret.id)) {
                LOG.error("Duplicate snapshot IDs, incrementing until we have a fresh one.");
                while(gtx.snapshots.containsKey(ret.id)) {
                    version = tx.getNextSnapshotId();
                    LOG.info("Attempting to create snapshot {} for feed {}", version, feedId);
                    ret = new Snapshot(feedId, version);
                }
            }

            ret.snapshotTime = System.currentTimeMillis();
            ret.feedVersionId = feedVersionId;
            ret.name = name;
            ret.comment = comment;
            ret.current = true;

            snapshot = getSnapshotDb(feedId, version, false);

            // if snapshot contains maps, increment the version ID until we find a snapshot that is empty
            while (snapshot.getAll().size() != 0) {
                version = tx.getNextSnapshotId();
                LOG.info("Attempting to create snapshot {} for feed {}", version, feedId);
                ret = new Snapshot(feedId, version);
                snapshot = getSnapshotDb(feedId, version, false);
            }

            new SnapshotTx(snapshot).make(tx);
            // for good measure
            snapshot.commit();
            snapshot.close();

            gtx.snapshots.put(ret.id, ret);
            gtx.commit();
            tx.commit();
            String snapshotMessage = String.format("Saving snapshot took %.2f seconds", (System.currentTimeMillis() - startTime) / 1000D);
            LOG.info(snapshotMessage);


            return ret;
        } catch (Exception e) {
            // clean up
            if (snapshot != null && !snapshot.isClosed())
                snapshot.close();

            if (version >= 0) {
                File snapshotDir = getSnapshotDir(feedId, version);

                if (snapshotDir.exists()) {
                    for (File file : snapshotDir.listFiles()) {
                        file.delete();
                    }
                }
            }

            // re-throw
            throw new RuntimeException(e);
        } finally {
            tx.rollbackIfOpen();
            gtx.rollbackIfOpen();
        }
    }

    /**
     * restore a snapshot.
     * @return a list of stops that were restored from deletion to make this snapshot valid.
     */
    public static List<Stop> restore (Snapshot s) {
        SnapshotTx tx = new SnapshotTx(getSnapshotDb(s.feedId, s.version, true));
        try {
            LOG.info("Restoring snapshot {} of agency {}", s.version, s.feedId);
            long startTime = System.currentTimeMillis();
            List<Stop> ret = tx.restore(s.feedId);
            LOG.info(String.format("Restored snapshot in %.2f seconds", (System.currentTimeMillis() - startTime) / 1000D));
            return ret;
        } finally {
            tx.close();
        }
    }

    /** get the directory in which to store a snapshot */
    public static DB getSnapshotDb (String feedId, int version, boolean readOnly) {
        File thisSnapshotDir = getSnapshotDir(feedId, version);
        thisSnapshotDir.mkdirs();
        File snapshotFile = new File(thisSnapshotDir, "snapshot_" + version + ".db");

        // we don't use transactions for snapshots - makes them faster
        // and smaller.
        // at the end everything gets committed and flushed to disk, so this thread
        // will not complete until everything is done.
        // also, we compress the snapshot databases
        DBMaker maker = DBMaker.newFileDB(snapshotFile)
                .compressionEnable();

        if (readOnly)
            maker.readOnly();

        return maker.make();
    }

    /** get the directory in which a snapshot is stored */
    public static File getSnapshotDir (String feedId, int version) {
        File agencyDir = new File(dataDirectory, feedId);
        File snapshotsDir = new File(agencyDir, "snapshots");
        return new File(snapshotsDir, "" + version);
    }

    /** Convenience function to check if a feed exists */
    public static boolean feedExists(String feedId) {
        GlobalTx tx = getGlobalTx();
        boolean exists = tx.feeds.containsKey(feedId);
        tx.rollback();
        return exists;
    }

    /** Get a (read-only) agency TX into a particular snapshot version of an agency */
    public static FeedTx getFeedTx(String feedId, int version) {
        DB db = getSnapshotDb(feedId, version, true);
        return new FeedTx(db, false);
    }

    /** A wrapped transaction, so the database just looks like a POJO */
    public static class DatabaseTx {
        /** the database (transaction). subclasses must initialize. */
        protected final DB tx;

        /** has this transaction been closed? */
        boolean closed = false;

        /** Convenience function to get a map */
        protected final <T1, T2> BTreeMap<T1, T2> getMap (String name) {
            return tx.createTreeMap(name)
                    // use java serialization to allow for schema upgrades
                    .valueSerializer(new ClassLoaderSerializer())
                    .makeOrGet();
        }

        /**
         * Convenience function to get a set. These are used as indices so they use the default serialization;
         * if we make a schema change we drop and recreate them.
         */
        protected final <T> NavigableSet <T> getSet (String name) {
            return tx.createTreeSet(name)
                    .makeOrGet();
        }

        protected DatabaseTx (DB tx) {
            this.tx = tx;
        }

        public void commit() {
            tx.commit();
            closed = true;
        }

        public void rollback() {
            tx.rollback();
            closed = true;
        }

        /** roll this transaction back if it has not been committed or rolled back already */
        public void rollbackIfOpen () {
            if (!closed) rollback();
        }

        protected final void finalize () {
            if (!closed) {
                LOG.error("DB transaction left unclosed, this signifies a memory leak!");
                rollback();
            }
        }
    }
}
