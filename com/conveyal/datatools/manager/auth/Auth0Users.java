package com.conveyal.datatools.manager.auth;

import com.conveyal.datatools.manager.DataManager;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * Created by landon on 4/26/16.
 */
public class Auth0Users {
    private static String AUTH0_DOMAIN = DataManager.config.get("auth0").get("domain").asText();
    private static String AUTH0_API_TOKEN = DataManager.serverConfig.get("auth0").get("api_token").asText();

    public static String getAuth0Users(String searchQuery) {
        String url = "https://" + AUTH0_DOMAIN + "/api/v2/users";
        if (searchQuery != null){
            try {
                url += "?search_engine=v2&q=" + URLEncoder.encode(searchQuery, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        System.out.println(url);
        String charset = "UTF-8";
        HttpClient client = new DefaultHttpClient();
        HttpGet request = new HttpGet(url);
        request.addHeader("Authorization", "Bearer " + AUTH0_API_TOKEN);
        request.setHeader("Accept-Charset", charset);
        HttpResponse response = null;
        try {
            response = client.execute(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String result = null;
        try {
            result = EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(result);
        return result;
    }
    public static String getUsersBySubscription(String subscriptionType, String target) {
        return getAuth0Users("app_metadata.datatools.subscriptions.type:" + subscriptionType + " AND app_metadata.datatools.subscriptions.target:" + target);
    }
    public static String getAuth0Users() throws IOException {
        return getAuth0Users(null);
    }
}
