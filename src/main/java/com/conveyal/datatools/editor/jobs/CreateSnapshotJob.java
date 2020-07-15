package com.conveyal.datatools.editor.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
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
    /** The namespace to snapshot. (Note: namespace resulting from snapshot can be found at {@link Snapshot#namespace} */
    private String namespace;
    /** Whether to update working buffer for the feed source to the newly created snapshot namespace. */
    private final boolean updateBuffer;
    /** Whether to persist the snapshot in the Snapshots collection. */
    private final boolean storeSnapshot;
    /**
     * Whether to preserve the existing editor buffer as its own snapshot. This is essentially a shorthand for creating
     * a snapshot and then separately loading something new into the buffer (if used with updateBuffer). It can also be
     * thought of as an autosave.
     */
    private final boolean preserveBuffer;
    private Snapshot snapshot;
    private FeedSource feedSource;

    public CreateSnapshotJob(Auth0UserProfile owner, Snapshot snapshot, boolean updateBufferNamespace, boolean storeSnapshot, boolean preserveBufferAsSnapshot) {
        super(owner, "Creating snapshot for " + snapshot.feedSourceId, JobType.CREATE_SNAPSHOT);
        this.namespace = snapshot.snapshotOf;
        this.snapshot = snapshot;
        this.updateBuffer = updateBufferNamespace;
        this.storeSnapshot = storeSnapshot;
        this.preserveBuffer = preserveBufferAsSnapshot;
        status.update( "Initializing...", 0);
    }

    public CreateSnapshotJob(Auth0UserProfile owner, Snapshot snapshot) {
        super(owner, "Creating snapshot for " + snapshot.feedSourceId, JobType.CREATE_SNAPSHOT);
        this.snapshot = snapshot;
        this.updateBuffer = false;
        this.storeSnapshot = true;
        this.preserveBuffer = false;
        status.update( "Initializing...", 0);
    }

    @JsonProperty
    public String getFeedSourceId () {
        return snapshot.feedSourceId;
    }

    @Override
    public void jobLogic() {
        // Special case where snapshot was created when a feed version was transformed by DbTransformations (the
        // snapshot contains the transformed feed). Because the jobs are queued up before the feed has been processed,
        // the namespace will not exist for the feed version until this jobLogic is actually run.
        if (namespace == null && snapshot.feedVersionId != null) {
            FeedVersion feedVersion = Persistence.feedVersions.getById(snapshot.feedVersionId);
            this.namespace = feedVersion.namespace;
        }
        // Get count of snapshots to set new version number.
        feedSource = Persistence.feedSources.getById(snapshot.feedSourceId);
        // Update job name to use feed source name (rather than ID).
        this.name = String.format("Creating snapshot for %s", feedSource.name);
        Collection<Snapshot> existingSnapshots = feedSource.retrieveSnapshots();
        int version = existingSnapshots.size();
        status.update("Creating snapshot...", 20);
        FeedLoadResult loadResult = makeSnapshot(namespace, DataManager.GTFS_DATA_SOURCE, !feedSource.preserveStopTimesSequence);
        snapshot.version = version;
        snapshot.namespace = loadResult.uniqueIdentifier;
        snapshot.feedLoadResult = loadResult;
        if (snapshot.name == null) {
            snapshot.generateName();
        }
        snapshot.snapshotTime = loadResult.completionTime;
        status.update("Database snapshot finished.", 80);
    }

    @Override
    public void jobFinished () {
        if (!status.error) {
            if (storeSnapshot) {
                LOG.info("Storing snapshot {} for feed source {}", snapshot.id, feedSource.id);
                Persistence.snapshots.create(snapshot);
            }
            if (preserveBuffer) {
                // Preserve the existing buffer as a snapshot if requested. This is essentially a shorthand for creating
                // a snapshot and then separately loading something new into the buffer. It can be thought of as an
                // autosave.
                // FIXME: the buffer would still exist even if not "preserved" here. Should it be deleted if
                //   requester opts to not preserve it?
                if (feedSource.editorNamespace == null) {
                    LOG.error("Cannot preserve snapshot with null namespace for feed source {}", feedSource.id);
                } else {
                    LOG.info("Autosaving feed source {} editor state {} as snapshot", feedSource.id, feedSource.editorNamespace);
                    Snapshot preservedBuffer = new Snapshot("Autosave " + new Date().toString(), feedSource.id, null);
                    preservedBuffer.namespace = feedSource.editorNamespace;
                    Persistence.snapshots.create(preservedBuffer);
                }
            }
            if (updateBuffer) {
                // Update working buffer to the newly created namespace. The "old" namespace will become the immutable
                // snapshot. Restoring the snapshot is a matter of making a new snapshot and updating the buffer pointer
                // to the new namespace, but we also will preserve the snapshot.
                LOG.info("Updating feed source {} active snapshot to {}", feedSource.id, snapshot.namespace);
                // If creating the initial snapshot for the editor buffer, set the editor namespace field to
                // the new feed's namespace.
                Persistence.feedSources.updateField(
                        feedSource.id,
                        "editorNamespace",
                        snapshot.namespace
                );
            }
            status.completeSuccessfully("Created snapshot!");
        }
    }
}
