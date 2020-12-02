package com.conveyal.datatools.common.utils;

import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.FeedExpirationNotificationJob;
import com.conveyal.datatools.manager.jobs.FetchSingleFeedJob;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.conveyal.datatools.common.utils.Utils.getTimezone;
import static com.conveyal.datatools.manager.models.FeedRetrievalMethod.FETCHED_AUTOMATICALLY;
import static com.google.common.collect.Multimaps.synchronizedListMultimap;

/**
 * This class centralizes the logic associated with scheduling and cancelling tasks (organized as a {@link ScheduledJob})
 * for the Data Tools application. These tasks can be auto-scheduled according to application data (e.g., feed expiration
 * notifications based on the latest feed version's last date of service) or enabled by users (e.g., scheduling a project
 * auto feed fetch nightly at 2AM). The jobs are tracked in {@link #scheduledJobsForFeedSources} so that they can be
 * cancelled at a later point in time should the associated feeds/projects be deleted or if the user changes the fetch
 * behavior.
 */
public class Scheduler {
    private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);
    private static final int DEFAULT_FETCH_INTERVAL_DAYS = 1;

    // Scheduled executor that handles running scheduled jobs.
    public final static ScheduledExecutorService schedulerService = Executors.newScheduledThreadPool(1);
    /** Stores {@link ScheduledJob} objects containing scheduled tasks keyed on the tasks's associated {@link FeedSource} ID. */
    public final static ListMultimap<String, ScheduledJob> scheduledJobsForFeedSources =
        synchronizedListMultimap(ArrayListMultimap.create());

    /**
     * A method to initialize all scheduled tasks upon server startup.
     */
    public static void initialize() {
        LOG.info("Scheduling recurring feed auto fetches for all projects.");
        for (Project project : Persistence.projects.getAll()) {
            handleAutoFeedFetch(project);
        }
        LOG.info("Scheduling feed expiration notifications for all feed sources.");
        // Get all active feed sources
        for (FeedSource feedSource : Persistence.feedSources.getAll()) {
            // Schedule expiration notification jobs for the latest feed version
            scheduleExpirationNotifications(feedSource);
        }
    }

    /**
     * Convenience method for scheduling one-off jobs for a feed source.
     */
    public static ScheduledJob scheduleFeedSourceJob (FeedSource feedSource, Runnable job, long delay, TimeUnit timeUnit) {
        ScheduledFuture<?> scheduledFuture = schedulerService.schedule(job, delay, timeUnit);
        ScheduledJob scheduledJob = new ScheduledJob(job, scheduledFuture);
        scheduledJobsForFeedSources.put(feedSource.id, scheduledJob);
        return scheduledJob;
    }

    /**
     * Convenience method for scheduling auto fetch job for a feed source. Expects delay/interval values in minutes.
     */
    public static ScheduledJob scheduleAutoFeedFetch(FeedSource feedSource, Runnable job, long delayMinutes, long intervalMinutes) {
        long delayHours = TimeUnit.MINUTES.toHours(delayMinutes);
        long intervalHours = TimeUnit.MINUTES.toHours(intervalMinutes);
        LOG.info("Auto fetch for feed {} runs every {} hours. Beginning in {} hours.", feedSource.id, intervalHours, delayHours);
        ScheduledFuture<?> scheduledFuture = schedulerService.scheduleAtFixedRate(job, delayMinutes, intervalMinutes, TimeUnit.MINUTES);
        ScheduledJob scheduledJob = new ScheduledJob(job, scheduledFuture);
        scheduledJobsForFeedSources.put(feedSource.id, scheduledJob);
        return scheduledJob;
    }

    /**
     * Cancels and removes all scheduled jobs for a given entity id and job class. NOTE: This is intended as an internal
     * method that should operate on one of the scheduledJobsForXYZ fields of this class. A wrapper method (such as
     * {@link #removeFeedSourceJobsOfType(String, Class, boolean)} should be provided for any new entity types with
     * scheduled jobs (e.g., if feed version-specific scheduled jobs are needed).
     */
    private static int removeJobsOfType(ListMultimap<String, ScheduledJob> scheduledJobs, String id, Class<?> clazz, boolean mayInterruptIfRunning) {
        int jobsCancelled = 0;
        // First get the list of jobs belonging to the id (e.g., all jobs related to a feed source).
        List<ScheduledJob> jobs = scheduledJobs.get(id);
        // Iterate over jobs, cancelling and removing only those matching the job class.
        // Use an iterator because elements may be removed and if removed in a regular loop it could
        // throw a java.util.ConcurrentModificationException
        // See https://stackoverflow.com/q/8104692/269834
        for (Iterator<ScheduledJob> iterator = jobs.iterator(); iterator.hasNext(); ) {
            ScheduledJob scheduledJob = iterator.next();
            // If clazz is null, remove all job types. Or, just remove the job if it matches the input type.
            if (clazz == null || clazz.isInstance(scheduledJob.job)) {
                scheduledJob.scheduledFuture.cancel(mayInterruptIfRunning);
                iterator.remove();
                jobsCancelled++;
            }
        }
        return jobsCancelled;
    }

    /**
     * Convenience wrapper around {@link #removeJobsOfType} that removes all job types for the provided id.
     */
    private static int removeAllJobs(ListMultimap<String, ScheduledJob> scheduledJobs, String id, boolean mayInterruptIfRunning) {
        return removeJobsOfType(scheduledJobs, id, null, mayInterruptIfRunning);
    }

    /**
     * Cancels and removes all scheduled jobs for a given feed source id and job class.
     */
    public static void removeFeedSourceJobsOfType(String id, Class<?> clazz, boolean mayInterruptIfRunning) {
        int cancelled = removeJobsOfType(scheduledJobsForFeedSources, id, clazz, mayInterruptIfRunning);
        if (cancelled > 0) LOG.info("Cancelled/removed {} {} jobs for feed source {}", cancelled, clazz.getSimpleName(), id);
    }

    /**
     * Cancels and removes all scheduled jobs for a given feed source id (of any job type).
     */
    public static void removeAllFeedSourceJobs(String id, boolean mayInterruptIfRunning) {
        int cancelled = removeAllJobs(scheduledJobsForFeedSources, id, mayInterruptIfRunning);
        if (cancelled > 0) LOG.info("Cancelled/removed {} jobs for feed source {}", cancelled, id);
    }

    /**
     * Schedule or cancel auto feed fetch for a project's feeds as needed.  This should be called whenever a
     * project is created or updated.  If a feed source is deleted, the auto feed fetch jobs will
     * automatically cancel itself.
     */
    public static void handleAutoFeedFetch(Project project) {
        long defaultDelay = getDefaultDelayMinutes(project);
        for (FeedSource feedSource : project.retrieveProjectFeedSources()) {
            scheduleAutoFeedFetch(feedSource, defaultDelay);
        }
    }

    /**
     * Get the default project delay in minutes corrected to the project's timezone.
     */
    private static long getDefaultDelayMinutes(Project project) {
        ZoneId timezone = getTimezone(project.defaultTimeZone);
        // NOW in project's timezone.
        ZonedDateTime now = ZonedDateTime.ofInstant(Instant.now(), timezone);

        // Scheduled start time for fetch (in project timezone)
        ZonedDateTime startTime = LocalDateTime.of(
            LocalDate.now(),
            LocalTime.of(project.autoFetchHour, project.autoFetchMinute)
        ).atZone(timezone);
        LOG.debug("Now: {}", now.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
        LOG.debug("Scheduled start time: {}", startTime.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));

        // Get diff between start time and current time
        long diffInMinutes = (startTime.toEpochSecond() - now.toEpochSecond()) / 60;
        // Delay is equivalent to diff or (if negative) one day plus (negative) diff.
        long projectDelayInMinutes = diffInMinutes >= 0
            ? diffInMinutes
            : 24 * 60 + diffInMinutes;
        LOG.debug(
            "Default auto fetch for feeds begins in {} hours and runs every {} hours",
            (projectDelayInMinutes / 60.0),
            TimeUnit.DAYS.toHours(DEFAULT_FETCH_INTERVAL_DAYS)
        );
        return projectDelayInMinutes;
    }

    /**
     * Convenience wrapper for calling scheduling a feed source auto fetch with the parent project's
     * default delay minutes.
     */
    public static void handleAutoFeedFetch(FeedSource feedSource) {
        long defaultDelayMinutes = getDefaultDelayMinutes(feedSource.retrieveProject());
        scheduleAutoFeedFetch(feedSource, defaultDelayMinutes);
    }

    /**
     * Internal method for scheduling an auto fetch for a {@link FeedSource}. This method's internals handle checking
     * that the auto fetch fields are filled correctly (at the project and feed source level).
     * @param feedSource          feed source for which to schedule auto fetch
     * @param defaultDelayMinutes default delay in minutes for scheduling the first fetch
     */
    private static void scheduleAutoFeedFetch(FeedSource feedSource, long defaultDelayMinutes) {
        try {
            // First, remove any scheduled fetch jobs for the current feed source.
            removeFeedSourceJobsOfType(feedSource.id, FetchSingleFeedJob.class, true);
            Project project = feedSource.retrieveProject();
            // Do not schedule fetch job if missing URL, not fetched automatically, or auto fetch disabled for project.
            if (feedSource.url == null || !FETCHED_AUTOMATICALLY.equals(feedSource.retrievalMethod) || !project.autoFetchFeeds) {
                return;
            }
            LOG.info("Scheduling auto fetch for feed source {}", feedSource.id);
            // Default fetch frequency to daily if null/missing.
            TimeUnit frequency = feedSource.fetchFrequency == null
                ? TimeUnit.DAYS
                : feedSource.fetchFrequency.toTimeUnit();
            // Convert interval to minutes. Note: Min interval is one (i.e., we cannot have zero fetches per day).
            // TODO: should this be higher if frequency is in minutes?
            long intervalMinutes = frequency.toMinutes(Math.max(feedSource.fetchInterval, 1));
            // Use system user as owner of job.
            Auth0UserProfile systemUser = Auth0UserProfile.createSystemUser();
            // Set delay to default delay for daily fetch (usually derived from project fetch time, e.g. 2am) OR zero
            // (begin checks immediately).
            long delayMinutes = TimeUnit.DAYS.equals(frequency) ? defaultDelayMinutes : 0;
            FetchSingleFeedJob fetchSingleFeedJob = new FetchSingleFeedJob(feedSource, systemUser, false);
            scheduleAutoFeedFetch(feedSource, fetchSingleFeedJob, delayMinutes, intervalMinutes);
        } catch (Exception e) {
            LOG.error("Error scheduling feed source {} auto fetch.", feedSource.id);
            e.printStackTrace();
        }
    }

    /**
     * Schedules feed expiration notifications.  This method will find the latest feed version and
     * then schedule a 1 week expiration warning notification and also notification the day that the
     * feed version expires.  It also cancels any existing notifications for this feed source.
     */
    public static void scheduleExpirationNotifications (FeedSource feedSource) {
        // Cancel existing expiration notifications
        removeFeedSourceJobsOfType(feedSource.id, FeedExpirationNotificationJob.class, true);

        FeedVersion latest = feedSource.retrieveLatest();

        if (
            latest != null &&
                latest.validationResult != null &&
                latest.validationResult.lastCalendarDate != null &&
                latest.validationResult.lastCalendarDate.isAfter(LocalDate.now())
        ) {
            // get parent project
            Project parentProject = feedSource.retrieveProject();

            if (parentProject == null) {
                // parent project has been deleted, but feed source/version have not
                // abort the setting up of the notification and figure out why the database has been
                // allowed to devolve to this state
                LOG.warn("The parent project for feed source {} does not exist in the database.", feedSource.id);
                return;
            }

            // get the timezone from the parent project
            ZoneId timezone = getTimezone(parentProject.defaultTimeZone);

            // calculate feed expiration time from last service date
            long expirationEpochSeconds = latest
                .validationResult
                .lastCalendarDate
                .atTime(4, 0)
                .atZone(timezone)
                .toEpochSecond();
            long curSeconds = System.currentTimeMillis() / 1000;
            long timeUntilExpiration = expirationEpochSeconds - curSeconds;
            long timeUntilOneWeekBeforeExpiration = timeUntilExpiration - 86400 * 7;

            // schedule notification jobs and record them in the scheduled notifications

            // one week warning
            if (timeUntilOneWeekBeforeExpiration > 0) {
                scheduleFeedSourceJob(
                    feedSource,
                    new FeedExpirationNotificationJob(feedSource.id, true),
                    timeUntilOneWeekBeforeExpiration,
                    TimeUnit.SECONDS
                );
            }

            // actual expiration
            scheduleFeedSourceJob(
                feedSource,
                new FeedExpirationNotificationJob(feedSource.id, false),
                timeUntilExpiration,
                TimeUnit.SECONDS
            );

            LOG.info("Scheduled feed expiration notifications for feed {}", feedSource.id);
        }
    }
}
