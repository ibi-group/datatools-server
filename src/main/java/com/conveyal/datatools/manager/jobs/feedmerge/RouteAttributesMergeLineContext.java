package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.datatools.manager.jobs.MergeFeedsJob;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.loader.Table;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipOutputStream;

public class RouteAttributesMergeLineContext extends MergeLineContext {
    // Track route_ids found in future feed in order to check for conflicts with active feed (MTC only).
    private final Set<String> routeIdsInFutureFeed = new HashSet<>();

    public RouteAttributesMergeLineContext(MergeFeedsJob job, Table table, ZipOutputStream out) throws IOException {
        super(job, table, out);
    }

    @Override
    public boolean checkFieldsForMergeConflicts(Set<NewGTFSError> idErrors, FieldContext fieldContext) {
        return checkRouteIds(fieldContext);
    }

    private boolean checkRouteIds(FieldContext fieldContext) {
        boolean shouldSkipRecord = false;
        String mergedRouteId = keyValue;
        if (fieldContext.nameEquals(ROUTE_ID)) {
            if (isHandlingFutureFeed()) {
                routeIdsInFutureFeed.add(keyValue);
            } else {
                // In the active feed, if a route of a given short name
                // has been assigned a new route_id, then the route_id of
                // the active record in route_attributes.txt should be remapped.
                String scopedRouteId = getTableScopedValue(Table.ROUTES, keyValue);
                String remappedRouteId = mergeFeedsResult.remappedIds.get(scopedRouteId);
                // Modify route_id and ensure that referencing trips
                // have route_id updated.
                if (remappedRouteId != null) {
                    mergedRouteId = remappedRouteId;
                    fieldContext.setValueToWrite(mergedRouteId);
                }

                // If any route_id in the active feed matches with the future
                // feed, skip it.
                if (routeIdsInFutureFeed.contains(mergedRouteId)) {
                    shouldSkipRecord = true;
                }
            }

            // Drop unused route ids.
            if (!mergeFeedsResult.routeIds.contains(mergedRouteId)) {
                shouldSkipRecord = true;
            }
        }

        return !shouldSkipRecord;
    }
}