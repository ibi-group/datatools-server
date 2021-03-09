package com.conveyal.datatools.common.utils;

import java.util.concurrent.ScheduledFuture;

/**
 * Utility class that associates a {@link Runnable} with its {@link ScheduledFuture} for easy storage and recall.
 */
public class ScheduledJob {
    public final ScheduledFuture<?> scheduledFuture;
    public final Runnable job;

    public ScheduledJob (Runnable job, ScheduledFuture<?> scheduledFuture) {
        this.job = job;
        this.scheduledFuture = scheduledFuture;
    }
}
