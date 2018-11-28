package com.conveyal.datatools.common.utils;

import java.time.ZoneId;

public class Utils {
    public static ZoneId getTimezone(String tzid) {
        try {
            return ZoneId.of(tzid);
        } catch(Exception e) {
            return ZoneId.of("America/New_York");
        }
    }
}
