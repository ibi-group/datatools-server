package com.conveyal.datatools.manager.gtfsplus;

import org.apache.logging.log4j.util.Strings;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Helper class to collect listed routes in directions.txt
 */
public class DirectionsScanner implements TableScanner {
    private final Set<String> routeIds = new HashSet<>();
    private int idIndex;

    public void setFields(List<String> fields) {
        // Find the route_id and realtime_enabled index from the headers.
        idIndex = fields.indexOf("route_id");
    }

    public boolean canUseHeaders() {
        return idIndex != -1;
    }

    public void scanRecord(String[] rowValues) {
        if (canUseHeaders()) {
            String idValue = rowValues[idIndex];

            if (!Strings.isBlank(idValue)) {
                routeIds.add(idValue);
            }
        }
    }

    public Set<String> getRouteIds() {
        return routeIds;
    }
}
