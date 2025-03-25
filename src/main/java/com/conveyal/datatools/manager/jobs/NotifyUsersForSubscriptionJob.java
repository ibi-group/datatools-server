package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.conveyal.datatools.manager.auth.Auth0Users.getVerifiedEmailsBySubscription;
import static com.conveyal.datatools.manager.utils.NotificationsUtils.sendNotification;

public class NotifyUsersForSubscriptionJob extends NotifyUsersJob {

    public static final Logger LOG = LoggerFactory.getLogger(NotifyUsersForSubscriptionJob.class);
    private final String subscriptionType;

    private NotifyUsersForSubscriptionJob(String subscriptionType, String target, String message) {
        super(target, message);
        this.subscriptionType = subscriptionType;
    }

    /**
     * Convenience method to create and schedule a notification job to notify subscribed users.
     */
    public static void createNotification(String subscriptionType, String target, String message) {
        createNotification(new NotifyUsersForSubscriptionJob(subscriptionType, target, message));
    }

    public void notifyUsers() {
        String subject;
        String subjectTemplate = "%s Notification: %s (%s)";
        String html = String.format("<p>%s</p>", this.message);
        String subscriptionToString = subscriptionType.replace("-", " ");
        String[] subType = subscriptionType.split("-");
        switch (subType[0]) {
            case "feed":
                FeedSource fs = Persistence.feedSources.getById(target);
                // Format subject header
                subject = String.format(subjectTemplate, applicationName, subscriptionToString, fs.name);
                // Add action text.
                html += String.format("<p>View <a href='%s/feed/%s'>this feed</a>.</p>", APPLICATION_URL, fs.id);
                break;
            case "project":
                Project p = Persistence.projects.getById(target);
                // Format subject header
                subject = String.format(subjectTemplate, applicationName, subscriptionToString, p.name);
                // Add action text.
                html += String.format("<p>View <a href='%s/project/%s'>this project</a>.</p>", APPLICATION_URL, p.id);
                break;
            case "deployment":
                Deployment deployment = Persistence.deployments.getById(target);
                // Format subject header
                subject = String.format(
                    subjectTemplate,
                    applicationName,
                    subscriptionToString,
                    deployment.name)
                ;
                // Add action text.
                html += String.format(
                    "<p>View <a href='%s/project/%s/deployments/%s'>this deployment</a>.</p>",
                    APPLICATION_URL,
                    deployment.projectId,
                    deployment.id
                );
                break;
            default:
                LOG.warn("Notifications not supported for subscription type {}", subType[0]);
                return;
        }
        // Add manage subscriptions blurb.
        html += String.format(
            "<p><small>Manage subscriptions <a href='%s/settings/notifications'>here</a>.</small></p>",
            APPLICATION_URL
        );
        LOG.info("Checking for subscribed users to notify type={} target={}", subscriptionType, target);
        for (String email : getVerifiedEmailsBySubscription(subscriptionType, target)) {
            LOG.info("Sending notification to {}", email);
            sendNotification(email, subject, "Body", html);
        }
    }
}
