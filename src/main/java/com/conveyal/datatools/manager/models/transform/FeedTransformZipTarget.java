package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;

import java.io.File;

/**
 * Target for a zip transformation (e.g., {@link ReplaceFileFromVersionTransformation}).
 */
public class FeedTransformZipTarget extends FeedTransformTarget {
    public File gtfsFile;

    public FeedTransformZipTarget(File gtfsFile) {
        this.gtfsFile = gtfsFile;
    }

    public String toString() {
        return String.format("Feed version: %s", gtfsFile.getAbsolutePath());
    }

    @Override
    public void validate(MonitorableJob.Status status) {
        if (gtfsFile == null || !gtfsFile.exists()) {
            status.fail("Target version must not be null.");
        }
    }
}
