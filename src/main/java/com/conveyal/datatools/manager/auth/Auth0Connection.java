package com.conveyal.datatools.manager.auth;

import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;

import javax.net.ssl.HttpsURLConnection;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.URL;

import static spark.Spark.halt;

/**
 * Created by demory on 3/22/16.
 */

public class Auth0Connection {
    private static final Logger LOG = LoggerFactory.getLogger(Auth0Connection.class);

    /**
     * Check API request for user token.
     * @param req Spark request object
     */
    public static void checkUser(Request req) {
        // if in a development environment, assign a mock profile to request attribute
        if (authDisabled()) {
            req.attribute("user", new Auth0UserProfile("mock@example.com", "user_id:string"));
            return;
        }
        String token = getToken(req);

        if(token == null) {
            halt(401, SparkUtils.formatJSON("Could not find authorization token", 401));
        }
        Auth0UserProfile profile;
        try {
            profile = getUserProfile(token);
            req.attribute("user", profile);
        }
        catch(Exception e) {
            LOG.warn("Could not verify user", e);
            halt(401, SparkUtils.formatJSON("Could not verify user", 401));
        }
    }

    public static String getToken(Request req) {
        String token = null;

        final String authorizationHeader = req.headers("Authorization");
        if (authorizationHeader == null) return null;

        // check format (Authorization: Bearer [token])
        String[] parts = authorizationHeader.split(" ");
        if (parts.length != 2) return null;

        String scheme = parts[0];
        String credentials = parts[1];

        if (scheme.equals("Bearer")) token = credentials;
        return token;
    }

    public static Auth0UserProfile getUserProfile(String token) throws Exception {

        URL url = new URL("https://" + DataManager.getConfigPropertyAsText("AUTH0_DOMAIN") + "/tokeninfo");
        HttpsURLConnection con = (HttpsURLConnection) url.openConnection();

        //add request header
        con.setRequestMethod("POST");

        String urlParameters = "id_token=" + token;

        // Send post request
        con.setDoOutput(true);
        DataOutputStream wr = new DataOutputStream(con.getOutputStream());
        wr.writeBytes(urlParameters);
        wr.flush();
        wr.close();

        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        ObjectMapper m = new ObjectMapper();
        Auth0UserProfile profile = null;
        String userString = response.toString();
        try {
            profile = m.readValue(userString, Auth0UserProfile.class);
        }
        catch(Exception e) {
            Object json = m.readValue(userString, Object.class);
            System.out.println(m.writerWithDefaultPrettyPrinter().writeValueAsString(json));
            LOG.warn("Could not verify user", e);
            halt(401, SparkUtils.formatJSON("Could not verify user", 401));
        }
        return profile;
    }

    public static void checkEditPrivileges(Request request) {

        Auth0UserProfile userProfile = request.attribute("user");
        String feedId = request.queryParams("feedId");
        if (feedId == null) {
            String[] parts = request.pathInfo().split("/");
            feedId = parts[parts.length - 1];
        }
        FeedSource feedSource = feedId != null ? FeedSource.get(feedId) : null;
        if (feedSource == null) {
            LOG.warn("feedId {} not found", feedId);
            halt(400, SparkUtils.formatJSON("Must provide feedId parameter", 400));
        }

        if (!request.requestMethod().equals("GET")) {
            if (!userProfile.canEditGTFS(feedSource.getOrganizationId(), feedSource.projectId, feedSource.id)) {
                LOG.warn("User {} cannot edit GTFS for {}", userProfile.email, feedId);
                halt(403, SparkUtils.formatJSON("User does not have permission to edit GTFS for feedId", 403));
            }
        }
    }

    public static boolean authDisabled() {
        return DataManager.hasConfigProperty("DISABLE_AUTH") && "true".equals(DataManager.getConfigPropertyAsText("DISABLE_AUTH"));
    }
}
