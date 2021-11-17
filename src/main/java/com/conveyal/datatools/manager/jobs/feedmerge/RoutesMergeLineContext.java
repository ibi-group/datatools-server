package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.datatools.manager.jobs.MergeFeedsJob;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.loader.Table;

import java.io.IOException;
import java.util.Set;
import java.util.zip.ZipOutputStream;

public class RoutesMergeLineContext extends MergeLineContext {
    public RoutesMergeLineContext(MergeFeedsJob job, Table table, ZipOutputStream out) throws IOException {
        super(job, table, out);
    }

    @Override
    public boolean checkFieldsForMergeConflicts(Set<NewGTFSError> idErrors, FieldContext fieldContext) throws IOException {
        return checkRoutesAndStopsIds(idErrors, fieldContext);
    }
}