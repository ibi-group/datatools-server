package com.conveyal.datatools.manager.utils;

import java.util.concurrent.TimeUnit;

/**
 * A helper class to track whether a task has timed out.
 */
public class TimeTracker {
    private final long maxEndTime;
    private final long startTime = System.currentTimeMillis();

    /**
     * @param maxDuration The maximum duration that this task should take.
     * @param timeUnit the unit of time that the duration is in
     */
    public TimeTracker(long maxDuration, TimeUnit timeUnit) {
        maxEndTime = startTime + timeUnit.toMillis(maxDuration);
    }


    public boolean hasTimedOut() {
        return System.currentTimeMillis() > maxEndTime;
    }

    public long elapsedSeconds() {
        return (System.currentTimeMillis() - startTime) / 1000;
    }
}
