package com.conveyal.datatools.manager.models.transform;

import java.io.File;

/**
 * This class collects the different targets that a {@link FeedTransformation} can apply to:
 * - zip file and
 * - snapshot ID (which is a proxy for the database namespace)
 *
 * Generally, only one of these fields should contain a value. This class exists so that we can keep
 * {@link FeedTransformation#transform} parameters constant for polymorphic subclasses.
 */
public class FeedTransformTarget {
    public File gtfsFile;
    public String snapshotId;

    public FeedTransformTarget(File gtfsFile) {
        this.gtfsFile = gtfsFile;
    }

    public FeedTransformTarget(String targetSnapshotId) {
        this.snapshotId = targetSnapshotId;
    }

    public String toString() {
        if (gtfsFile != null) {
            return String.format("Feed version: %s", gtfsFile.getAbsolutePath());
        } else {
            return String.format("Db namespace: %s", snapshotId);
        }
    }
}
