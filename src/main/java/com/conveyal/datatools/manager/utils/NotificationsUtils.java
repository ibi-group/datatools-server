package com.conveyal.datatools.manager.utils;

import com.conveyal.datatools.manager.DataManager;
import com.sparkpost.Client;
import com.sparkpost.model.responses.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by landon on 4/26/16.
 */
public class NotificationsUtils {
    private static final Logger LOG = LoggerFactory.getLogger(NotificationsUtils.class);
    private static final String NOTIFY_CONFIG = "application.notifications_enabled";
    private static final boolean notificationsDisabled = DataManager.hasConfigProperty(NOTIFY_CONFIG) &&
        !DataManager.getConfigProperty(NOTIFY_CONFIG).asBoolean();

    public static void sendNotification(String to_email, String subject, String text, String html) {
        if (notificationsDisabled) {
            // Skip sending notification message if notifications are not enabled.
            LOG.warn("Notifications disabled. Skipping notification to {} SUBJECT: {}", to_email, subject);
            return;
        }
        String API_KEY = DataManager.getConfigPropertyAsText("SPARKPOST_KEY");
        Client client = new Client(API_KEY);

        try {
            Response response = client.sendMessage(
                    DataManager.getConfigPropertyAsText("SPARKPOST_EMAIL"), // from
                    to_email, // to
                    subject,
                    text,
                    html);
            LOG.info(response.getResponseMessage());
        } catch (Exception e) {
            LOG.error("Could not send notification to {}", to_email);
            e.printStackTrace();
        }
    }
}
