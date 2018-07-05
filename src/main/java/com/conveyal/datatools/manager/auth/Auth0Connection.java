package com.conveyal.datatools.manager.auth;

import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.net.ssl.HttpsURLConnection;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import static com.conveyal.datatools.common.utils.SparkUtils.haltWithMessage;
import static com.conveyal.datatools.manager.DataManager.getConfigPropertyAsText;
import static spark.Spark.halt;

/**
 * Created by demory on 3/22/16.
 */

public class Auth0Connection {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(Auth0Connection.class);
    private static final String BASE_URL = getConfigPropertyAsText("application.public_url");

    /**
     * Check API request for user token and assign as the "user" attribute on the incoming request object for use in
     * downstream controllers.
     * @param req Spark request object
     */
    public static void checkUser(Request req) {
        // If in a development environment, assign a mock profile to request attribute
        if (authDisabled()) {
            req.attribute("user", new Auth0UserProfile("mock@example.com", "user_id:string"));
            return;
        }
        String token = getToken(req);

        if(token == null) {
            haltWithMessage(401, "Could not find authorization token");
        }
        Auth0UserProfile profile;
        try {
            profile = getUserProfile(token);
            req.attribute("user", profile);
        }
        catch(Exception e) {
            LOG.warn("Could not verify user", e);
            haltWithMessage(401, "Could not verify user");
        }
    }

    private static String getToken(Request req) {
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

    /**
     * Gets the Auth0 user profile for the provided token.
     */
    private static Auth0UserProfile getUserProfile(String token) throws Exception {

        URL url = new URL("https://" + getConfigPropertyAsText("AUTH0_DOMAIN") + "/tokeninfo");
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
            LOG.warn(m.writerWithDefaultPrettyPrinter().writeValueAsString(json));
            LOG.warn("Could not verify user", e);
            halt(401, SparkUtils.formatJSON("Could not verify user", 401));
        }
        return profile;
    }

    /**
     * Check that the user has edit privileges for the feed ID specified.
     * FIXME: Needs an update for SQL editor.
     */
    public static void checkEditPrivileges(Request request) {
        // If in a development environment, assign a mock profile to request attribute
        if (authDisabled()) {
            request.attribute("user", new Auth0UserProfile("mock@example.com", "user_id:string"));
            return;
        }
        Auth0UserProfile userProfile = request.attribute("user");
        String feedId = request.queryParams("feedId");
        if (feedId == null) {
            String[] parts = request.pathInfo().split("/");
            feedId = parts[parts.length - 1];
        }
        FeedSource feedSource = feedId != null ? Persistence.feedSources.getById(feedId) : null;
        if (feedSource == null) {
            LOG.warn("feedId {} not found", feedId);
            halt(400, SparkUtils.formatJSON("Must provide valid feedId parameter", 400));
        }

        if (!request.requestMethod().equals("GET")) {
            if (!userProfile.canEditGTFS(feedSource.organizationId(), feedSource.projectId, feedSource.id)) {
                LOG.warn("User {} cannot edit GTFS for {}", userProfile.email, feedId);
                halt(403, SparkUtils.formatJSON("User does not have permission to edit GTFS for feedId", 403));
            }
        }
    }

    /**
     * Check whether authentication has been disabled via the DISABLE_AUTH config variable.
     */
    public static boolean authDisabled() {
        return DataManager.hasConfigProperty("DISABLE_AUTH") && "true".equals(getConfigPropertyAsText("DISABLE_AUTH"));
    }

    /**
     * Log Spark requests in the format user@email.com: GET http://localhost/path/info. This will also attempt to parse and log
     * the request body if the content type is JSON.
     */
    public static void logRequest(Request request, Response response) {
        Auth0UserProfile userProfile = request.attribute("user");
        String userEmail = userProfile != null ? userProfile.email : "no-auth";
        // Log all API requests to the application's Spark server.
        String bodyString = "";
        try {
            if ("application/json".equals(request.contentType()) && !"".equals(request.body())) {
                // Only construct body string if ContentType is JSON and body is not empty
                JsonNode jsonNode = MAPPER.readTree(request.body());
                // Add new line for legibility when printing to system.out
                bodyString = "\n" + MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(jsonNode);
            }
        } catch (IOException e) {
            LOG.warn("Could not parse JSON", e);
            bodyString = "\nBad JSON:\n" + request.body();
            // FIXME: Should a halt be applied here to warn about malformed JSON? Or are there special cases where
            // request body should not be JSON?
        }

        String queryString = request.queryParams().size() > 0 ? "?" + request.queryString() : "";
        if (!request.pathInfo().contains("secure/status/") && !request.pathInfo().contains("secure/lock/")) {
            // Do not log requests made to lock or status controller (requests are made many times in succession).
            LOG.info("{}: {} {}{}{}{}", userEmail, request.requestMethod(), BASE_URL, request.pathInfo(), queryString, bodyString);
        }
    }

    /**
     * TODO: Check that user has access to query namespace provided in GraphQL query (see https://github.com/catalogueglobal/datatools-server/issues/94).
     */
    public static void checkGTFSPrivileges(Request request) {
        Auth0UserProfile userProfile = request.attribute("user");
        String feedId = request.queryParams("feedId");
        if (feedId == null) {
            String[] parts = request.pathInfo().split("/");
            feedId = parts[parts.length - 1];
        }
        FeedSource feedSource = feedId != null ? Persistence.feedSources.getById(feedId) : null;
        if (feedSource == null) {
            LOG.warn("feedId {} not found", feedId);
            halt(400, SparkUtils.formatJSON("Must provide valid feedId parameter", 400));
        }

        if (!request.requestMethod().equals("GET")) {
            if (!userProfile.canEditGTFS(feedSource.organizationId(), feedSource.projectId, feedSource.id)) {
                LOG.warn("User {} cannot edit GTFS for {}", userProfile.email, feedId);
                halt(403, SparkUtils.formatJSON("User does not have permission to edit GTFS for feedId", 403));
            }
        }
    }
}
