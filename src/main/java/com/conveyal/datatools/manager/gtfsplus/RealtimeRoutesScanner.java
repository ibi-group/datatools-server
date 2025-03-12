package com.conveyal.datatools.manager.gtfsplus;

import org.apache.logging.log4j.util.Strings;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Helper class to collect enabled routes from realtime_routes.txt
 */
public class RealtimeRoutesScanner extends TableScanner {
    private final Set<String> enabledRouteIds = new HashSet<>();
    private int idIndex;
    private int enabledIndex;

    public void setFields(List<String> fields) {
        // Find the route_id and realtime_enabled index from the headers.
        idIndex = fields.indexOf("route_id");
        enabledIndex = fields.indexOf("realtime_enabled");
    }

    private boolean canUseHeaders() {
        return idIndex != -1 && enabledIndex != -1;
    }

    public void scanRecord(String[] rowValues) {
        if (canUseHeaders()) {
            String idValue = rowValues[idIndex];
            String enabledValue = rowValues[enabledIndex];

            if ("1".equals(enabledValue) && !Strings.isBlank(idValue)) {
                enabledRouteIds.add(idValue);
            }
        }
    }

    public Set<String> getEnabledRouteIds() {
        return enabledRouteIds;
    }
}
