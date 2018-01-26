package com.conveyal.datatools.editor.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.Snapshot;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.loader.FeedLoadResult;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;

import static com.conveyal.gtfs.GTFS.makeSnapshot;

/**
 * Makes a snapshot of a feed in a SQL database. Effectively this is copying all of the tables for a given feed (which
 * all exist in a single schema, e.g., abcd_efghijklmnopqurstu.stops) into a new feed/schema namespace.
 *
 * As far as the logic for managing snapshots in the GTFS editor, here is the basic model we follow, along with some
 * ideas for future optimizations to make things quicker for large feeds (which can take a minute or two to copy all of
 * the tables -- unfortunately there is nothing in the world of RDBMS that allows for copying tables with records and
 * indexes on those records fully intact):
 *
 * 1. User uploads a GTFS zip file as a feed version.
 * 2. User loads that feed version into editor. This initializes a CreateSnapshotJob to create the first working buffer.
 *    Also, the editorNamespace field is updated to this first buffer namespace (abcd_).
 * 3. User makes edits, deletes a route, adds stops etc.
 *
 *  TODO: In the future, we could copy on write to add individual tables as they are edited and piece together a feed.
 *
 * 4. User makes a new snapshot to save her work. This initializes another CreateSnapshotJob to snapshot the buffer
 *    (abcd_). Operationally what happens is that the tables are copied to a new namespace (lmno_) and the new namespace
 *    becomes the working buffer.
 * 5. More edits... and, oops, we just deleted everything.
 * 6. User needs to restore abcd_, so we make a snapshot of abcd_ (as naamespace wxyz_), which becomes the new working
 *    buffer. If the user chooses, she can delete lmno_. Otherwise it just stays as a snapshot that the pointer has
 *    moved away from.
 *
 * I think how I had been thinking about this at one point was that the newly created tables were the "snapshot," but
 * actually in this model the namespace being copied becomes the immutable "snapshot" and the buffer is the new
 * namespace.
 *
 *        2.                                4.
 *  ____________                       ____________                                                       ____________
 * |            |                     |            |                                                     |            |
 * |            |                     |            |                                                     |            |
 * |            |          3.         |            |                5.                                   |            |
 * |   abcd_    | -->    edits    --> |    lmno_   | -->   more edits on lmno_  -->  restore abcd_  -->  |    wxyz_   |
 * |            |                     |            |                                                     |            |
 * |            |                     |            |                                                     |            |
 * |____________|                     |____________|                                                     |____________|
 */
public class CreateSnapshotJob extends MonitorableJob {
    private static final Logger LOG = LoggerFactory.getLogger(CreateSnapshotJob.class);
    private final FeedSource feedSource;
    private final String namespace;
    private final boolean updateBuffer;
    private Snapshot snapshot;

    public CreateSnapshotJob(FeedSource feedSource, String namespace, String owner, boolean updateBuffer) {
        super(owner, "Creating snapshot for " + feedSource.name, JobType.CREATE_SNAPSHOT);
        this.feedSource = feedSource;
        this.namespace = namespace;
        this.updateBuffer = updateBuffer;
        status.update(false,  "Creating snapshot...", 0);
    }

    @JsonProperty
    public String getFeedSourceId () {
        return feedSource.id;
    }

    @Override
    public void jobLogic() {
        // Get count of snapshots to set new version number.
        Collection<Snapshot> existingSnapshots = feedSource.retrieveSnapshots();
        int version = existingSnapshots.size();
        FeedLoadResult loadResult = makeSnapshot(namespace, DataManager.GTFS_DATA_SOURCE);
        this.snapshot = new Snapshot(feedSource.id, version, namespace, loadResult);
        snapshot.name = "New snapshot " + new Date().toString();
        snapshot.snapshotTime = loadResult.completionTime;
    }

    @Override
    public void jobFinished () {
        if (!status.error) {
            LOG.info("Storing snapshot {}", snapshot.id);
            Persistence.snapshots.create(snapshot);
            if (updateBuffer) {
                // Update working buffer to the newly created namespace. The "old" namespace will become the immutable
                // snapshot. Restoring the snapshot is a matter of making a new snapshot and updating the buffer pointer
                // to the new namespace.
                LOG.info("Updating active snapshot to {}", snapshot.id);
                // If creating the initial snapshot for the editor buffer, set the editor namespace field to
                // the new feed's namespace.
                Persistence.feedSources.updateField(
                        feedSource.id,
                        "editorNamespace",
                        snapshot.namespace
                );
                // FIXME Add original namespace as origin copy of data in list of snapshots field.
            } else {
                // Add to list of non-active snapshots.

            }
            status.update(false, "Created snapshot!", 100, true);
        }
    }
}
