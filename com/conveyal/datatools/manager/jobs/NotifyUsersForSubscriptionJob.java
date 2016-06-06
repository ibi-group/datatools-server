package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.Project;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

import static com.conveyal.datatools.manager.auth.Auth0Users.getUsersBySubscription;
import static com.conveyal.datatools.manager.utils.NotificationsUtils.sendNotification;

/**
 * Created by landon on 6/6/16.
 */
public class NotifyUsersForSubscriptionJob implements Runnable {
    private ObjectMapper mapper = new ObjectMapper();
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
        if (!DataManager.config.get("application").get("notifications_enabled").asBoolean()) {
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
            String email = user.get("email").asText();
            Boolean emailVerified = user.get("email_verified").asBoolean();
            System.out.println(email);

            // only send email if address has been verified
            if (emailVerified) {
                try {
                    String subject = "subject";
                    String url = null;
                    String bodyAction = null;
                    String[] subType = this.subscriptionType.split("-");
                    if (subType[0] == "feed"){
                        FeedSource fs = FeedSource.get(this.target);
                        subject = "Datatools Notification: " + this.subscriptionType.replace("-", " ") + " (" + fs.name + ")";
                        url = DataManager.config.get("application").get("url").asText();
                        bodyAction = "</p><p>View <a href='" + url + "/feed/" + fs.id + "'>this feed</a>.</p>";

                    }
                    else if (subType[0] == "project") {
                        Project p = Project.get(this.target);
                        subject = "Datatools Notification: " + this.subscriptionType.replace("-", " ") + " (" + p.name + ")";
                        url = DataManager.config.get("application").get("url").asText();
                        bodyAction = "</p><p>View <a href='" + url + "/project/" + p.id + "'>this project</a>.</p>";
                    }

                    sendNotification(email, subject, "Body", "<p>" + this.message + bodyAction);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
