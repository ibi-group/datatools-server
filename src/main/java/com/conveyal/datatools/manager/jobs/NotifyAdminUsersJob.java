package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.JobUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.conveyal.datatools.manager.auth.Auth0Users.getVerifiedEmailsForAdmins;
import static com.conveyal.datatools.manager.utils.NotificationsUtils.sendNotification;

public class NotifyAdminUsersJob extends NotifyUsersJob {

    public static final Logger LOG = LoggerFactory.getLogger(NotifyAdminUsersJob.class);
    private final String target;
    private final String message;

    private NotifyAdminUsersJob(String target, String message) {
        this.target = target;
        this.message = message;
    }

    /**
     * Convenience method to create and schedule a notification job to notify admin users.
     */
    public static void createNotification(String target, String message) {
        if (hasInvalidApplicationURL()) {
            return;
        }
        NotifyAdminUsersJob notifyJob = new NotifyAdminUsersJob(target, message);
        JobUtils.lightExecutor.execute(notifyJob);
        LOG.info("Notification job scheduled in light executor");
    }

    protected void notifyUsers() {
        Project project = Persistence.projects.getById(target);
        String subject = String.format("%s Notification: Project updated (%s)", applicationName, project.name);
        final String html = String.format(
            "<p>%s</p><p>View <a href='%s/project/%s'>this project</a>.</p>",
            message,
            APPLICATION_URL,
            project.id
        );
        getVerifiedEmailsForAdmins().forEach(email -> {
            LOG.info("Sending notification to {}", email);
            sendNotification(email, subject, "Body", html);
        });
    }
}
