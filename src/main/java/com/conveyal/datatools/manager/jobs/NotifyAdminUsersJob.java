package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.conveyal.datatools.manager.auth.Auth0Users.getVerifiedEmailsForAdmins;
import static com.conveyal.datatools.manager.utils.NotificationsUtils.sendNotification;

public class NotifyAdminUsersJob extends NotifyUsersJob {

    public static final Logger LOG = LoggerFactory.getLogger(NotifyAdminUsersJob.class);

    private NotifyAdminUsersJob(String target, String message) {
        super(target, message);
    }

    /**
     * Convenience method to create and schedule a notification job to notify admin users.
     */
    public static void createNotification(String target, String message) {
        createNotification(new NotifyAdminUsersJob(target, message));
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
