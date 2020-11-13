package com.conveyal.datatools.manager.models;

import java.util.concurrent.TimeUnit;

/**
 * Contains options for how frequent a {@link FeedSource} should be fetched if auto-fetch is enabled. The enum values
 * MUST map to possible values of {@link TimeUnit} and are limited to our own subset to avoid ultra high frequencies. In
 * other words, we don't want to allow users to set auto-fetch to every 5 milliseconds!
 */
public enum FetchFrequency {
    DAYS, HOURS, MINUTES;

    public TimeUnit toTimeUnit() {
        return TimeUnit.valueOf(this.toString());
    }

    public static FetchFrequency fromValue(String value) {
        for (FetchFrequency f: FetchFrequency.values()) {
            if (f.toString().equals(value)) {
                return f;
            }
        }
        throw new IllegalArgumentException(value);
    }
}
