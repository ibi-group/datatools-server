package com.conveyal.datatools.manager.jobs.validation;

import java.util.Map;

public enum SharedStopsHeader {

    STOP_GROUP_ID_INDEX("stop_group_id"),

    STOP_ID_INDEX("stop_id"),

    IS_PRIMARY_INDEX("is_primary"),

    FEED_ID_INDEX("feed_id");

    public final String headerName;

    SharedStopsHeader(String name) {
        this.headerName = name;
    }

    public String getHeaderName() {
        return headerName;
    }

    public static SharedStopsHeader fromValue(String headerName) {
        for (SharedStopsHeader sharedStopsHeader : SharedStopsHeader.values()) {
            if (sharedStopsHeader.getHeaderName().equalsIgnoreCase(headerName)) {
                return sharedStopsHeader;
            }
        }
        throw new UnsupportedOperationException(String.format("Unknown shared stops header: %s", headerName));
    }

    public static boolean hasRequiredHeaders(Map<SharedStopsHeader, Integer> headerIndices) {
        return headerIndices.size() == SharedStopsHeader.values().length;
    }
}
