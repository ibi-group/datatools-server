package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.utils.Scheduler;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static com.conveyal.datatools.manager.auth.Auth0Users.getVerifiedEmailsBySubscription;
import static com.conveyal.datatools.manager.utils.NotificationsUtils.sendNotification;

public class FeedExpirationNotificationJob implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(FeedExpirationNotificationJob.class);
    private static final String APPLICATION_URL = DataManager.getConfigPropertyAsText("application.public_url");

    String feedSourceId;
    boolean isWarningNotification;

    public FeedExpirationNotificationJob(String feedSourceId, boolean isWarningNotification) {
        this.feedSourceId = feedSourceId;
        this.isWarningNotification = isWarningNotification;
    }

    public void run() {
        FeedSource source = Persistence.feedSources.getById(feedSourceId);
        Project project = source.retrieveProject();
        if (source == null) {
            Scheduler.removeAllFeedSourceJobs(feedSourceId, true);
        }
        if (project == null) {
            // parent project has already been deleted, this notification should've been canceled
            // but it's still around for some reason.  Return as nothing further should be done.
            return;
        }

        // build up list of emails to send expiration notifications to
        Set<String> emails = new HashSet<>();

        // get each user subscriber for feed
        emails.addAll(getVerifiedEmailsBySubscription("feed-updated", source.id));

        // get each user subscriber for feed's project
        emails.addAll(getVerifiedEmailsBySubscription("project-updated", project.id));

        if (emails.size() > 0) {
            LOG.info(
                String.format(
                    "Sending feed %s for feed source %s notification to %d users",
                    isWarningNotification
                        ? "expiration in one week" :
                        "final expiration",
                    source.id,
                    emails.size()
                )
            );

            String message = String.format(
                "The latest feed version for %s %s",
                source.name,
                isWarningNotification ? "expires in one week!" : "has expired!"
            );

            String feedSourceUrl = String.format("%s/feed/%s", APPLICATION_URL, source.id);
            String text = String.format(
                "%s\n\nView the %s feedsource here: %s.",
                message,
                source.name,
                feedSourceUrl
            );
            String html = String.format(
                "<p>%s</p><p>View the <a href='%s'>%s feedsource here<a>.",
                message,
                feedSourceUrl,
                source.name
            );

            for (String email : emails) {
                sendNotification(email, message, text, html);
            }
        }
    }
}
