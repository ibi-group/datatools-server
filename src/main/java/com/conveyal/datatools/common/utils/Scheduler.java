package com.conveyal.datatools.common.utils;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.controllers.api.ProjectController;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

public class Scheduler {
    private static final Logger LOG = LoggerFactory.getLogger(DataManager.class);

    // Stores ScheduledFuture objects that kick off runnable tasks (e.g., fetch project feeds at 2:00 AM).
    public static Map<String, ScheduledFuture> autoFetchMap = new HashMap<>();
    // Scheduled executor that handles running scheduled jobs.
    public final static ScheduledExecutorService schedulerService = Executors.newScheduledThreadPool(1);
    // multimap for keeping track of notification expirations
    public final static ListMultimap<String, Future> scheduledNotificationExpirations = ArrayListMultimap.create();

    /**
     * A method to initialize all scheduled tasks upon server startup.
     */
    public static void initialize() {
        scheduleProjectAutoFetches();

        scheduleFeedNotificationExpirations();
    }

    /**
     * Schedule recurring auto-fetches for all projects as needed
     */
    private static void scheduleProjectAutoFetches() {
        for (Project project : Persistence.projects.getAll()) {
            if (project.autoFetchFeeds) {
                ScheduledFuture scheduledFuture = ProjectController.scheduleAutoFeedFetch(project, 1);
                autoFetchMap.put(project.id, scheduledFuture);
            }
        }
    }

    /**
     * Schedule all feed version expiration notification jobs
     */
    private static void scheduleFeedNotificationExpirations() {
        LOG.info("Scheduling feed expiration notifications");

        // get all active feed sources
        for (FeedSource feedSource : Persistence.feedSources.getAll()) {
            // schedule expiration notification jobs for the latest feed version
            feedSource.scheduleExpirationNotifications();
        }
    }
}
