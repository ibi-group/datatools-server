package com.conveyal.datatools.manager.utils;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.Project;
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
}
