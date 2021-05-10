package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.gtfs.loader.Table;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

import java.io.IOException;
import java.util.zip.ZipFile;

/**
 * Helper class that collects the feed version and its zip file. Note: this class helps with sorting versions to
 * merge in a list collection.
 */
public class FeedToMerge {
    public FeedVersion version;
    public ZipFile zipFile;
    public SetMultimap<Table, String> idsForTable = HashMultimap.create();

    public FeedToMerge(FeedVersion version) throws IOException {
        this.version = version;
        this.zipFile = new ZipFile(version.retrieveGtfsFile());
    }
}