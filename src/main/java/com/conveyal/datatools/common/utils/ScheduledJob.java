package com.conveyal.datatools.common.utils;

import java.util.concurrent.Future;

/**
 * Utility class that associates a {@link Runnable} with its {@link Future} for easy storage and recall.
 */
public class ScheduledJob {
    public final Future scheduledFuture;
    public final Runnable job;

    public ScheduledJob (Runnable job, Future scheduledFuture) {
        this.job = job;
        this.scheduledFuture = scheduledFuture;
    }
}
