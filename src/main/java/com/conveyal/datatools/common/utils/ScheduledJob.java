package com.conveyal.datatools.common.utils;

import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.Project;

import java.util.concurrent.ScheduledFuture;

/**
 * Utility class that associates a {@link Runnable} with its {@link ScheduledFuture} for easy storage and recall.
 */
public class ScheduledJob {
    public final String id;
    public final ScheduledFuture scheduledFuture;
    public final Runnable job;

    ScheduledJob (FeedSource feedSource, Runnable job, ScheduledFuture scheduledFuture) {
        this.id = feedSource.id;
        this.job = job;
        this.scheduledFuture = scheduledFuture;
    }

    ScheduledJob (Project project, Runnable job, ScheduledFuture scheduledFuture) {
        this.id = project.id;
        this.job = job;
        this.scheduledFuture = scheduledFuture;
    }
}
