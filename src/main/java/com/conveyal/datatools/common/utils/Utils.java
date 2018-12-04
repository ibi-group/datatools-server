package com.conveyal.datatools.common.utils;

import java.time.ZoneId;

public class Utils {

    /**
     * Get the ZoneId of a String that could be a valid tzid.
     *
     * @param tzid  The timezone identifier
     * @return  The ZoneId of the parsed timezone identifier, or "America/New_York" if tzid is invalid.
     */
    public static ZoneId getTimezone(String tzid) {
        try {
            return ZoneId.of(tzid);
        } catch(Exception e) {
            return ZoneId.of("America/New_York");
        }
    }
}
