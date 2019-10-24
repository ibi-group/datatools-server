package com.conveyal.datatools.common.utils;

import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.FeedExpirationNotificationJob;
import com.conveyal.datatools.manager.jobs.FetchProjectFeedsJob;
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
import static com.google.common.collect.Multimaps.synchronizedListMultimap;

/**
 * This class centralizes the logic associated with scheduling and cancelling tasks (organized as a {@link ScheduledJob})
 * for the Data Tools application. These tasks can be auto-scheduled according to application data (e.g., feed expiration
 * notifications based on the latest feed version's last date of service) or enabled by users (e.g., scheduling a project
 * auto feed fetch nightly at 2AM). The jobs are tracked in {@link #scheduledJobsForFeedSources} and
 * {@link #scheduledJobsForProjects} so that they can be cancelled at a later point in time should the associated
 * feeds/projects be deleted or if the user changes the fetch behavior.
 */
public class Scheduler {
    private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);

    // Scheduled executor that handles running scheduled jobs.
    public final static ScheduledExecutorService schedulerService = Executors.newScheduledThreadPool(1);
    /** Stores {@link ScheduledJob} objects containing scheduled tasks keyed on the tasks's associated {@link FeedSource} ID. */
    public final static ListMultimap<String, ScheduledJob> scheduledJobsForFeedSources =
        synchronizedListMultimap(ArrayListMultimap.create());
    /** Stores {@link ScheduledJob} objects containing scheduled tasks keyed on the tasks's associated {@link Project} ID. */
    public final static ListMultimap<String, ScheduledJob> scheduledJobsForProjects =
        synchronizedListMultimap(ArrayListMultimap.create());

    /**
     * A method to initialize all scheduled tasks upon server startup.
     */
    public static void initialize() {
        LOG.info("Scheduling recurring project auto fetches");
        for (Project project : Persistence.projects.getAll()) {
            if (project.autoFetchFeeds) {
                scheduleAutoFeedFetch(project, 1);
            }
        }
        LOG.info("Scheduling feed expiration notifications");
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
        ScheduledFuture scheduledFuture = schedulerService.schedule(job, delay, timeUnit);
        ScheduledJob scheduledJob = new ScheduledJob(job, scheduledFuture);
        scheduledJobsForFeedSources.put(feedSource.id, scheduledJob);
        return scheduledJob;
    }

    /**
     * Cancels and removes all scheduled jobs for a given entity id and job class. NOTE: This is intended as an internal
     * method that should operate on one of the scheduledJobsForXYZ fields of this class. A wrapper method (such as
     * {@link #removeProjectJobsOfType(String, Class, boolean)}) should be provided for any new entity types with
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
            if (clazz.isInstance(scheduledJob.job)) {
                scheduledJob.scheduledFuture.cancel(mayInterruptIfRunning);
                iterator.remove();
                jobsCancelled++;
            }
        }
        return jobsCancelled;
    }

    /**
     * Cancels and removes all scheduled jobs for a given feed source id and job class.
     */
    public static void removeFeedSourceJobsOfType(String id, Class<?> clazz, boolean mayInterruptIfRunning) {
        int cancelled = removeJobsOfType(scheduledJobsForFeedSources, id, clazz, mayInterruptIfRunning);
        if (cancelled > 0) LOG.info("Cancelled/removed {} {} jobs for feed source {}", cancelled, clazz.getSimpleName(), id);
    }

    /**
     * Cancels and removes all scheduled jobs for a given project id and job class.
     */
    public static void removeProjectJobsOfType(String id, Class<?> clazz, boolean mayInterruptIfRunning) {
        int cancelled = removeJobsOfType(scheduledJobsForProjects, id, clazz, mayInterruptIfRunning);
        if (cancelled > 0) LOG.info("Cancelled/removed {} {} jobs for project {}", cancelled, clazz.getSimpleName(), id);
    }

    /**
     * Schedule or cancel auto feed fetch for a project as needed.  This should be called whenever a
     * project is created or updated.  If a project is deleted, the auto feed fetch jobs will
     * automatically cancel itself.
     */
    public static void scheduleAutoFeedFetch(Project project) {
        // If auto fetch flag is turned on, schedule auto fetch.
        if (project.autoFetchFeeds) Scheduler.scheduleAutoFeedFetch(project, 1);
        // Otherwise, cancel any existing task for this id.
        else Scheduler.removeProjectJobsOfType(project.id, FetchProjectFeedsJob.class, true);
    }

    /**
     * Schedule an action that fetches all the feeds in the given project according to the autoFetch fields of that project.
     * Currently feeds are not auto-fetched independently, they must be all fetched together as part of a project.
     * This method is called when a Project's auto-fetch settings are updated, and when the system starts up to populate
     * the auto-fetch scheduler.
     */
    public static void scheduleAutoFeedFetch (Project project, int intervalInDays) {
        try {
            // First cancel any already scheduled auto fetch task for this project id.
            removeProjectJobsOfType(project.id, FetchProjectFeedsJob.class, true);

            ZoneId timezone = getTimezone(project.defaultTimeZone);
            LOG.info("Scheduling auto-fetch for projectID: {}", project.id);

            // NOW in default timezone
            ZonedDateTime now = ZonedDateTime.ofInstant(Instant.now(), timezone);

            // Scheduled start time
            ZonedDateTime startTime = LocalDateTime.of(
                LocalDate.now(),
                LocalTime.of(project.autoFetchHour, project.autoFetchMinute)
            ).atZone(timezone);
            LOG.info("Now: {}", now.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
            LOG.info("Scheduled start time: {}", startTime.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));

            // Get diff between start time and current time
            long diffInMinutes = (startTime.toEpochSecond() - now.toEpochSecond()) / 60;
            // Delay is equivalent to diff or (if negative) one day plus (negative) diff.
            long delayInMinutes = diffInMinutes >= 0
                ? diffInMinutes
                : 24 * 60 + diffInMinutes;

            LOG.info("Auto fetch begins in {} hours and runs every {} hours", String.valueOf(delayInMinutes / 60.0), TimeUnit.DAYS.toHours(intervalInDays));
            long intervalInMinutes = TimeUnit.DAYS.toMinutes(intervalInDays);
            // system is defined as owner because owner field must not be null
            FetchProjectFeedsJob fetchProjectFeedsJob = new FetchProjectFeedsJob(project, Auth0UserProfile.createSystemUser());
            ScheduledFuture scheduledFuture = schedulerService.scheduleAtFixedRate(
                fetchProjectFeedsJob,
                delayInMinutes,
                intervalInMinutes,
                TimeUnit.MINUTES
            );
            ScheduledJob scheduledJob = new ScheduledJob(fetchProjectFeedsJob, scheduledFuture);
            scheduledJobsForProjects.put(project.id, scheduledJob);
        } catch (Exception e) {
            LOG.error("Error scheduling project {} feed fetch.", project.id);
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
