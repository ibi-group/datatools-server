package com.conveyal.datatools.common.utils;

import com.conveyal.datatools.common.status.MonitorableJob;

import java.util.concurrent.ScheduledFuture;

/**
 * Utility class that associates a {@link MonitorableJob} with its {@link ScheduledFuture} for easy storage and recall.
 */
public class ScheduledJob {
    public final ScheduledFuture scheduledFuture;
    public final MonitorableJob job;

    public ScheduledJob (MonitorableJob job, ScheduledFuture scheduledFuture) {
        this.job = job;
        this.scheduledFuture = scheduledFuture;
    }
}
