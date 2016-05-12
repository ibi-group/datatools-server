package com.conveyal.datatools.manager.utils;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sparkpost.Client;
import com.sparkpost.exception.SparkPostException;
import com.sparkpost.model.responses.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.Data;
import java.io.IOException;

import static com.conveyal.datatools.manager.auth.Auth0Users.getUsersBySubscription;

/**
 * Created by landon on 4/26/16.
 */
public class NotificationsUtils {
    private static final Logger LOG = LoggerFactory.getLogger(NotificationsUtils.class);
    public static ObjectMapper mapper = new ObjectMapper();

    public static void sendNotification(String to_email, String subject, String text, String html) {
        String API_KEY = DataManager.serverConfig.get("sparkpost").get("key").asText();
        Client client = new Client(API_KEY);

        try {
            Response response = client.sendMessage(
                    DataManager.serverConfig.get("sparkpost").get("from_email").asText(), // from
                    to_email, // to
                    subject,
                    text,
                    html);
            System.out.println(response.getResponseMessage());
        } catch (SparkPostException e) {
            e.printStackTrace();
        }
    }

    public static void notifyUsersForSubscription(String subscriptionType, String target, String message) {
        if (!DataManager.config.get("application").get("notifications_enabled").asBoolean()) {
            return;
        }
        String userString = getUsersBySubscription(subscriptionType, target);
        JsonNode subscribedUsers = null;
        try {
            subscribedUsers = mapper.readTree(userString);
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (JsonNode user : subscribedUsers) {
            String email = user.get("email").asText();
            Boolean emailVerified = user.get("email_verified").asBoolean();
            System.out.println(email);

            // only send email if address has been verified
            if (emailVerified) {
                try {
                    FeedSource fs = FeedSource.get(target);
                    String subject = "Datatools Notification: " + subscriptionType.replace("-", " ") + " (" + fs.name + ")";
                    String url = DataManager.config.get("application").get("url").asText();
                    sendNotification(email, subject, "Body", "<p>" + message + "</p><p>View <a href='" + url + "/feed/" + fs.id + "'>this feed</a>.</p>");
                } catch (Exception e) {
                    LOG.error(e.getMessage());
                }
            }
        }
    }
}
