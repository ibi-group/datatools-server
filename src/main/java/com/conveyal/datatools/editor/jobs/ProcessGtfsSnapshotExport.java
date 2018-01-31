package com.conveyal.datatools.editor.jobs;

import com.beust.jcommander.internal.Lists;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.datatools.editor.datastore.FeedTx;
import com.conveyal.datatools.editor.datastore.GlobalTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.Snapshot;

import java.time.LocalDate;

import org.mapdb.Fun.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

public class ProcessGtfsSnapshotExport extends MonitorableJob {
    public static final Logger LOG = LoggerFactory.getLogger(ProcessGtfsSnapshotExport.class);
    private Collection<Tuple2<String, Integer>> snapshots;
    private File output;
//    private LocalDate startDate;
//    private LocalDate endDate;

    /** Export the named snapshots to GTFS */
    public ProcessGtfsSnapshotExport(Collection<Tuple2<String, Integer>> snapshots, File output, LocalDate startDate, LocalDate endDate) {
        super("application", "Exporting snapshots to GTFS", JobType.PROCESS_SNAPSHOT_EXPORT);
        this.snapshots = snapshots;
        this.output = output;
//        this.startDate = startDate;
//        this.endDate = endDate;
    }

    /**
     * Export the master branch of the named feeds to GTFS. The boolean variable can be either true or false, it is only to make this
     * method have a different erasure from the other
     */
    public ProcessGtfsSnapshotExport(Collection<String> agencies, File output, LocalDate startDate, LocalDate endDate, boolean isagency) {
        super("application", "Exporting snapshots to GTFS", JobType.PROCESS_SNAPSHOT_EXPORT);
        this.snapshots = Lists.newArrayList(agencies.size());

        for (String agency : agencies) {
            // leaving version null will cause master to be used
            this.snapshots.add(new Tuple2<String, Integer>(agency, null));
        }

        this.output = output;
//        this.startDate = startDate;
//        this.endDate = endDate;
    }

    /**
     * Export this snapshot to GTFS, using the validity range in the snapshot.
     */
    public ProcessGtfsSnapshotExport (Snapshot snapshot, File output) {
        this(Arrays.asList(new Tuple2[] { snapshot.id }), output, snapshot.validFrom, snapshot.validTo);
    }

    @Override
    public void jobLogic() {
        GTFSFeed feed = null;

        GlobalTx gtx = VersionedDataStore.getGlobalTx();
        FeedTx feedTx = null;

        try {
            for (Tuple2<String, Integer> ssid : snapshots) {
                String feedId = ssid.a;

                // retrieveById present feed database if no snapshot version provided
                if (ssid.b == null) {
                    feedTx = VersionedDataStore.getFeedTx(feedId);
                }
                // else retrieveById snapshot version data
                else {
                    feedTx = VersionedDataStore.getFeedTx(feedId, ssid.b);
                }
                feed = feedTx.toGTFSFeed(false);
            }
            feed.toFile(output.getAbsolutePath());
        } finally {
            gtx.rollbackIfOpen();
            if (feedTx != null) feedTx.rollbackIfOpen();
        }
    }

    public static int toGtfsDate (LocalDate date) {
        return date.getYear() * 10000 + date.getMonthValue() * 100 + date.getDayOfMonth();
    }
}

