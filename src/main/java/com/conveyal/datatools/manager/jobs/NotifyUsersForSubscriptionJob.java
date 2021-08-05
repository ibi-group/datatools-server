package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.JobUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.conveyal.datatools.manager.auth.Auth0Users.getVerifiedEmailsBySubscription;
import static com.conveyal.datatools.manager.controllers.api.UserController.inTestingEnvironment;
import static com.conveyal.datatools.manager.utils.NotificationsUtils.sendNotification;

/**
 * Created by landon on 6/6/16.
 */
public class NotifyUsersForSubscriptionJob implements Runnable {

    public static final Logger LOG = LoggerFactory.getLogger(NotifyUsersForSubscriptionJob.class);
    private String subscriptionType;
    private String target;
    private String message;
    private static final String DEFAULT_NAME = "Data Tools";
    private static final String APPLICATION_NAME = DataManager.getConfigPropertyAsText("application.title");
    private static final String APPLICATION_URL = DataManager.getConfigPropertyAsText("application.public_url");

    private NotifyUsersForSubscriptionJob(String subscriptionType, String target, String message) {
        this.subscriptionType = subscriptionType;
        this.target = target;
        this.message = message;
    }

    /**
     * Convenience method to create and schedule a notification job to notify subscribed users.
     */
    public static void createNotification(String subscriptionType, String target, String message) {
        if (APPLICATION_URL == null || !(APPLICATION_URL.startsWith("https://") || APPLICATION_URL.startsWith("http://"))) {
            LOG.error("application.public_url (value={}) property must be set to a valid URL in order to send notifications to users.", APPLICATION_URL);
            return;
        }
        NotifyUsersForSubscriptionJob notifyJob = new NotifyUsersForSubscriptionJob(subscriptionType, target, message);
        JobUtils.lightExecutor.execute(notifyJob);
        LOG.info("Notification job scheduled in light executor");
    }

    @Override
    public void run() {
        notifyUsersForSubscription();
    }

    // TODO: modify method so that it receives both a feed param and a updateFor param?
    private void notifyUsersForSubscription() {
        String subject;
        String html = String.format("<p>%s</p>", this.message);
        String applicationName;
        String subscriptionToString = this.subscriptionType.replace("-", " ");
        if (APPLICATION_NAME == null) {
            LOG.warn("Configuration property \"application.title\" must be set to customize notifications.");
            applicationName = DEFAULT_NAME;
        } else {
            applicationName = APPLICATION_NAME;
        }
        String[] subType = this.subscriptionType.split("-");
        switch (subType[0]) {
            case "feed":
                FeedSource fs = Persistence.feedSources.getById(this.target);
                // Format subject header
                subject = String.format("%s Notification: %s (%s)", applicationName, subscriptionToString, fs.name);
                // Add action text.
                html += String.format("<p>View <a href='%s/feed/%s'>this feed</a>.</p>", APPLICATION_URL, fs.id);
                break;
            case "project":
                Project p = Persistence.projects.getById(this.target);
                // Format subject header
                subject = String.format("%s Notification: %s (%s)", applicationName, subscriptionToString, p.name);
                // Add action text.
                html += String.format("<p>View <a href='%s/project/%s'>this project</a>.</p>", APPLICATION_URL, p.id);
                break;
            case "deployment":
                Deployment deployment = Persistence.deployments.getById(this.target);
                // Format subject header
                subject = String.format(
                    "%s Notification: %s (%s)",
                    applicationName,
                    subscriptionToString,
                    deployment.name);
                // Add action text.
                html += String.format(
                    "<p>View <a href='%s/project/%s/deployments/%s'>this deployment</a>.</p>",
                    APPLICATION_URL,
                    deployment.projectId,
                    deployment.id);
                break;
            default:
                LOG.warn("Notifications not supported for subscription type {}", subType[0]);
                return;
        }
        // Add manage subscriptions blurb.
        html += String.format(
            "<p><small>Manage subscriptions <a href='%s/settings/notifications'>here</a>.</small></p>",
            APPLICATION_URL);
        // Only notify subscribed users if not in testing environment.
        if (inTestingEnvironment()) {
            LOG.info("Skipping check for subscribed users");
        } else {
            LOG.info("Checking for subscribed users to notify type={} target={}", subscriptionType, target);
            for (String email : getVerifiedEmailsBySubscription(subscriptionType, target)) {
                LOG.info("Sending notification to {}", email);
                sendNotification(email, subject, "Body", html);
            }
        }
    }
}
