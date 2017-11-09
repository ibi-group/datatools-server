package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.conveyal.datatools.manager.auth.Auth0Users.getUsersBySubscription;
import static com.conveyal.datatools.manager.utils.NotificationsUtils.sendNotification;

/**
 * Created by landon on 6/6/16.
 */
public class NotifyUsersForSubscriptionJob implements Runnable {
    private ObjectMapper mapper = new ObjectMapper();
    public static final Logger LOG = LoggerFactory.getLogger(NotifyUsersForSubscriptionJob.class);
    private String subscriptionType;
    private String target;
    private String message;

    public NotifyUsersForSubscriptionJob(String subscriptionType, String target, String message) {
        this.subscriptionType = subscriptionType;
        this.target = target;
        this.message = message;
    }

    @Override
    public void run() {
        notifyUsersForSubscription();
    }

    // TODO: modify method so that it receives both a feed param and a updateFor param?
    public void notifyUsersForSubscription() {
        if (DataManager.hasConfigProperty("application.notifications_enabled") && !DataManager.getConfigProperty("application.notifications_enabled").asBoolean()) {
            return;
        }
        String userString = getUsersBySubscription(this.subscriptionType, this.target);
        JsonNode subscribedUsers = null;
        try {
            subscribedUsers = this.mapper.readTree(userString);
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (JsonNode user : subscribedUsers) {
            if (!user.has("email")) {
                continue;
            }
            String email = user.get("email").asText();
            Boolean emailVerified = user.get("email_verified").asBoolean();
            LOG.info("sending notification to {}", email);

            // only send email if address has been verified
            if (emailVerified) {
                try {
                    String subject;
                    String url;
                    String bodyAction;
                    String[] subType = this.subscriptionType.split("-");
                    switch (subType[0]) {
                        case "feed":
                            FeedSource fs = Persistence.feedSources.getById(this.target);
                            subject = DataManager.getConfigPropertyAsText("application.title")+ " Notification: " + this.subscriptionType.replace("-", " ") + " (" + fs.name + ")";
                            url = DataManager.getConfigPropertyAsText("application.public_url");
                            bodyAction = "</p><p>View <a href='" + url + "/feed/" + fs.id + "'>this feed</a>.</p>";
                            sendNotification(email, subject, "Body", "<p>" + this.message + bodyAction);
                            break;
                        case "project":
                            Project p = Persistence.projects.getById(this.target);
                            subject = "Datatools Notification: " + this.subscriptionType.replace("-", " ") + " (" + p.name + ")";
                            url = DataManager.getConfigPropertyAsText("application.public_url");
                            bodyAction = "</p><p>View <a href='" + url + "/project/" + p.id + "'>this project</a>.</p>";
                            sendNotification(email, subject, "Body", "<p>" + this.message + bodyAction);
                            break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
