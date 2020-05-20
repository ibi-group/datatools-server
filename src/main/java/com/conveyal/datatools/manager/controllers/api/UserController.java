package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.auth.Auth0Users;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.Note;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;
import static com.conveyal.datatools.manager.auth.Auth0Users.API_PATH;
import static com.conveyal.datatools.manager.auth.Auth0Connection.authDisabled;
import static com.conveyal.datatools.manager.auth.Auth0Users.USERS_API_PATH;
import static com.conveyal.datatools.manager.auth.Auth0Users.getUserById;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;
import static spark.Spark.put;

/**
 * Handles the HTTP endpoints related to CRUD operations for Auth0 users.
 */
public class UserController {

    private static final String AUTH0_DOMAIN = DataManager.getConfigPropertyAsText("AUTH0_DOMAIN");
    private static final String AUTH0_CLIENT_ID = DataManager.getConfigPropertyAsText("AUTH0_CLIENT_ID");
    public static final int TEST_AUTH0_PORT = 8089;
    public static final String TEST_AUTH0_DOMAIN = String.format("localhost:%d", TEST_AUTH0_PORT);
    private static Logger LOG = LoggerFactory.getLogger(UserController.class);
    private static ObjectMapper mapper = new ObjectMapper();
    private static final String UTF_8 = "UTF-8";
    public static final String DEFAULT_BASE_USERS_URL = "https://" + AUTH0_DOMAIN  + USERS_API_PATH;
    /** Users URL uses Auth0 domain by default, but can be overridden with {@link #setBaseUsersUrl(String)} for testing. */
    private static String baseUsersUrl = DEFAULT_BASE_USERS_URL;
    private static final JsonManager<Project> json = new JsonManager<>(Project.class, JsonViews.UserInterface.class);

    /**
     * HTTP endpoint to get a single Auth0 user for the application (by specified ID param). Note, this uses a different
     * Auth0 API (get user) than the other get methods (user search query).
     */
    private static String getUser(Request req, Response res) {
        HttpGet getUserRequest = new HttpGet(getUserIdUrl(req));
        setHeaders(req, getUserRequest);
        return executeRequestAndGetResult(getUserRequest, req);
    }

    /**
     * Determines whether the user controller is being run in a testing environment by checking if the users URL contains
     * the {@link #TEST_AUTH0_DOMAIN}.
     */
    public static boolean inTestingEnvironment() {
        return baseUsersUrl.contains(TEST_AUTH0_DOMAIN);
    }

    /**
     * HTTP endpoint to get all users for the application (using a filtered search on all users for the Auth0 tenant).
     */
    private static String getAllUsers(Request req, Response res) {
        res.type("application/json");
        int page = Integer.parseInt(req.queryParams("page"));
        String queryString = filterUserSearchQuery(req);
        String users = Auth0Users.getAuth0Users(queryString, page);
        return users;
    }

    /**
     * Filters a search query for users by the query string and the requesting user's permissions. For example, an
     * organization admin is only permitted to view the users assigned to that organization, whereas an application
     * admin can view all users for all organizations.
     */
    private static String filterUserSearchQuery(Request req) {
        Auth0UserProfile userProfile = req.attribute("user");
        String queryString = req.queryParams("queryString");
        if(queryString != null) queryString = "email:" + queryString + "*";

        if (userProfile.canAdministerApplication()) {
            // do not filter further based on permissions, proceed with search
            return queryString;
        } else if (userProfile.canAdministerOrganization()) {
            String organizationId = userProfile.getOrganizationId();
            // filter by organization_id
            if (queryString == null) {
                queryString = "app_metadata.datatools.organizations.organization_id:" + organizationId;
            } else {
                queryString += " AND app_metadata.datatools.organizations.organization_id:" + organizationId;
            }
            return queryString;
        } else {
            logMessageAndHalt(req, 401, "Must be application or organization admin to view users");
            // Return statement cannot be reached due to halt.
            return null;
        }
    }

    /**
     * Gets the total count of users that match the filtered user search query.
     */
    private static int getUserCount(Request req, Response res) {
        res.type("application/json");
        String queryString = filterUserSearchQuery(req);
        try {
            return Auth0Users.getAuth0UserCount(queryString);
        } catch (IOException e) {
            logMessageAndHalt(req, 500, "Failed to get user count", e);
            return 0;
        }
    }

    /**
     * HTTP endpoint to create a "public user" that has no permissions to access projects in the application.
     *
     * Note, this passes a "blank" app_metadata object to the newly created user, so there is no risk of someone
     * injecting permissions somehow into the create user request.
     */
    private static String createPublicUser(Request req, Response res) {
        HttpPost createUserRequest = new HttpPost(baseUsersUrl);
        setHeaders(req, createUserRequest);

        JsonNode jsonNode = parseJsonFromBody(req);
        String json = String.format("{" +
                "\"connection\": \"Username-Password-Authentication\"," +
                "\"email\": %s," +
                "\"password\": %s," +
                "\"app_metadata\": {\"datatools\": [{\"permissions\": [], \"projects\": [], \"subscriptions\": [], \"client_id\": \"%s\" }] } }",
                jsonNode.get("email"), jsonNode.get("password"), AUTH0_CLIENT_ID);
        setRequestEntityUsingJson(createUserRequest, json, req);

        return executeRequestAndGetResult(createUserRequest, req);
    }

    /**
     * HTTP endpoint to create new Auth0 user for the application.
     */
    private static String createUser(Request req, Response res) {
        HttpPost createUserRequest = new HttpPost(baseUsersUrl);
        setHeaders(req, createUserRequest);

        JsonNode jsonNode = parseJsonFromBody(req);
        String json = String.format("{" +
                "\"connection\": \"Username-Password-Authentication\"," +
                "\"email\": %s," +
                "\"password\": %s," +
                "\"app_metadata\": {\"datatools\": [%s] } }"
                , jsonNode.get("email"), jsonNode.get("password"), jsonNode.get("permissions"));
        setRequestEntityUsingJson(createUserRequest, json, req);

        return executeRequestAndGetResult(createUserRequest, req);
    }

    private static String updateUser(Request req, Response res) {
        String userId = req.params("id");
        Auth0UserProfile user = getUserById(userId);

        if (user == null) {
            logMessageAndHalt(
                req,
                404,
                String.format("Could not update user: User with id %s not found (or there are issues with the Auth0 configuration)", userId)
            );
        }

        LOG.info("Updating user {}", user.getEmail());

        HttpPatch updateUserRequest = new HttpPatch(getUserIdUrl(req));
        setHeaders(req, updateUserRequest);

        JsonNode jsonNode = parseJsonFromBody(req);

//        JsonNode data = mapper.readValue(jsonNode.retrieveById("data"), Auth0UserProfile.DatatoolsInfo.class); //jsonNode.retrieveById("data");
        JsonNode data = jsonNode.get("data");

        Iterator<Map.Entry<String, JsonNode>> fieldsIter = data.fields();
        while (fieldsIter.hasNext()) {
            Map.Entry<String, JsonNode> entry = fieldsIter.next();
        }
//        if (!data.has("client_id")) {
//            ((ObjectNode)data).put("client_id", DataManager.config.retrieveById("auth0").retrieveById("client_id").asText());
//        }
        String json = "{ \"app_metadata\": { \"datatools\" : " + data + " }}";

        setRequestEntityUsingJson(updateUserRequest, json, req);

        return executeRequestAndGetResult(updateUserRequest, req);
    }

    private static Object deleteUser(Request req, Response res) {
        HttpDelete deleteUserRequest = new HttpDelete(getUserIdUrl(req));
        setHeaders(req, deleteUserRequest);
        executeRequestAndGetResult(deleteUserRequest, req);
        return true;
    }

    /** HTTP endpoint to get recent activity summary based on requesting user's subscriptions/notifications. */
    private static List<Activity> getRecentActivity(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        // Default range: past 7 days, TODO: Allow custom from/to range based on query params?
        ZonedDateTime from = ZonedDateTime.now(ZoneOffset.UTC).minusDays(7);
        ZonedDateTime to = ZonedDateTime.now(ZoneOffset.UTC);

        List<Activity> activityList = new ArrayList<>();
        Auth0UserProfile.DatatoolsInfo datatools = userProfile.getApp_metadata().getDatatoolsInfo();
        // NOTE: this condition will also occur if DISABLE_AUTH is set to true
        if (datatools == null) {
            if (authDisabled()) return Collections.emptyList();
            logMessageAndHalt(req, 403, "User does not have permission to access to this application");
        }

        Auth0UserProfile.Subscription[] subscriptions = datatools.getSubscriptions();
        if (subscriptions == null) return activityList;

        /* NOTE: as of May-08-2018 we decided to limit subscriptions to two types:
         * 'feed-updated' and 'project-updated'. Comment subscriptions are now always
         * assumed if the containing 'feed-updated' subscription is active
         */
        for (Auth0UserProfile.Subscription sub : subscriptions) {
            switch (sub.getType()) {
                case "feed-updated":
                    for (String targetId : sub.getTarget()) {
                        FeedSource fs = Persistence.feedSources.getById(targetId);
                        if (fs == null) continue;

                        // FeedSource comments
                        for (Note note : fs.retrieveNotes()) {
                            ZonedDateTime datePosted = toZonedDateTime(note.date);
                            if (datePosted.isBefore(from) || datePosted.isAfter(to)) continue;
                            activityList.add(new FeedSourceCommentActivity(note, fs));
                        }

                        // Iterate through this Feed's FeedVersions
                        for(FeedVersion version : fs.retrieveFeedVersions()) {
                            // FeedVersion creation event
                            ZonedDateTime dateCreated = toZonedDateTime(fs.dateCreated);
                            if (dateCreated.isAfter(from) && dateCreated.isBefore(to)) {
                                activityList.add(new FeedVersionCreationActivity(version, fs));
                            }

                            // FeedVersion comments
                            for (Note note : version.retrieveNotes()) {
                                ZonedDateTime datePosted = toZonedDateTime(note.date);
                                if (datePosted.isBefore(from) || datePosted.isAfter(to)) continue;
                                activityList.add(new FeedVersionCommentActivity(note, fs, version));
                            }
                        }
                    }
                    break;

                case "project-updated":
                    // Iterate through Project IDs, skipping any that don't resolve to actual projects
                    for (String targetId : sub.getTarget()) {
                        Project proj = Persistence.projects.getById(targetId);
                        if (proj == null) continue;

                        // Iterate through Project's FeedSources, creating "Feed created" items as needed
                        for (FeedSource fs : proj.retrieveProjectFeedSources()) {
                            ZonedDateTime dateCreated = toZonedDateTime(fs.dateCreated);
                            if (dateCreated.isBefore(from) || dateCreated.isAfter(to)) continue;
                            activityList.add(new FeedSourceCreationActivity(fs, proj));
                        }
                    }
                    break;
            }
        }

        return activityList;
    }

    /**
     * Resends the user confirmation email for a given user
     */
    private static ObjectNode resendEmailConfirmation(Request req, Response res) {
        // verify if the request is legit. Should only come from the user for which the account belongs to.
        Auth0UserProfile userProfile = req.attribute("user");
        if (!userProfile.getUser_id().equals(req.params("id"))) {
            // user is not authorized because they're not the user that email verification is being requested for
            logMessageAndHalt(
                req,
                401,
                "Not authorized to resend confirmation email"
            );
            // doesn't actually return null since above line sends a 401 response
            return null;
        }
        // authorized. Create request to resend email verification
        HttpPost resendEmailVerificationRequest = new HttpPost(
            "https://" + AUTH0_DOMAIN + API_PATH + "/jobs/verification-email"
        );
        setHeaders(req, resendEmailVerificationRequest);
        setRequestEntityUsingJson(
            resendEmailVerificationRequest,
            JsonUtil.objectMapper.createObjectNode()
                .put("user_id", userProfile.getUser_id())
                .put("client_id", AUTH0_CLIENT_ID)
                .toString(),
            req
        );

        // Execute request. If a HTTP response other than 200 occurs, an error will be returned.
        executeRequestAndGetResult(resendEmailVerificationRequest, req);

        // Email verification successfully sent! Return successful response.
        return JsonUtil.objectMapper.createObjectNode()
            .put("emailSent", true);
    }

    /**
     * Set some common headers on the request, including the API access token, which must be obtained via token request
     * to Auth0.
     */
    private static void setHeaders(Request sparkRequest, HttpRequestBase auth0Request) {
        String apiToken = Auth0Users.getApiToken();
        if (apiToken == null) {
            logMessageAndHalt(
                sparkRequest,
                400,
                "Failed to obtain Auth0 API token for request"
            );
        }
        auth0Request.addHeader("Authorization", "Bearer " + apiToken);
        auth0Request.setHeader("Accept-Charset", UTF_8);
        auth0Request.setHeader("Content-Type", "application/json");
    }

    /**
     * Safely parse the userId and create an Auth0 url.
     *
     * @param req The initating request that came into datatools-server
     */
    private static String getUserIdUrl(Request req) {
        try {
            return String.format(
                "%s/%s",
                baseUsersUrl,
                URLEncoder.encode(req.params("id"), "UTF-8")
            );
        } catch (UnsupportedEncodingException e) {
            logMessageAndHalt(
                req,
                400,
                "Failed to encode user id",
                e
            );
        }
        return null;
    }

    /**
     * Safely parse the request body into a JsonNode.
     *
     * @param req The initating request that came into datatools-server
     */
    private static JsonNode parseJsonFromBody(Request req) {
        try {
            return mapper.readTree(req.body());
        } catch (IOException e) {
            logMessageAndHalt(req, 400, "Failed to parse request body", e);
            return null;
        }
    }

    /**
     * Safely set the HTTP request body with a json string.
     *
     * @param request the outgoing HTTP post request
     * @param json The json to set in the request body
     * @param req The initating request that came into datatools-server
     */
    private static void setRequestEntityUsingJson(HttpEntityEnclosingRequestBase request, String json, Request req) {
        HttpEntity entity = null;
        try {
            entity = new ByteArrayEntity(json.getBytes(UTF_8));
        } catch (UnsupportedEncodingException e) {
            logMessageAndHalt(
                req,
                500,
                "Failed to set entity body due to encoding issue.",
                e
            );
        }
        request.setEntity(entity);
    }

    /**
     * Executes and logs an outgoing HTTP request, makes sure it worked and then returns the
     * stringified response body.
     *
     * @param httpRequest The outgoing HTTP request
     * @param req The initating request that came into datatools-server
     */
    private static String executeRequestAndGetResult(HttpRequestBase httpRequest, Request req) {
        // execute outside http request
        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = null;
        try {
            LOG.info("Making request: ({})", httpRequest.toString());
            response = client.execute(httpRequest);
        } catch (IOException e) {
            LOG.error("HTTP request failed: ({})", httpRequest.toString());
            logMessageAndHalt(
                req,
                500,
                "Failed to make external HTTP request.",
                e
            );
        }

        // parse response body if there is one
        HttpEntity entity = response.getEntity();
        String result = null;
        if (entity != null) {
            try {
                result = EntityUtils.toString(entity);
            } catch (IOException e) {
                logMessageAndHalt(
                    req,
                    500,
                    String.format(
                        "Failed to parse result of http request (%s).",
                        httpRequest.toString()
                    ),
                    e
                );
            }
        }

        int statusCode = response.getStatusLine().getStatusCode();
        if(statusCode >= 300) {
            LOG.error(
                "HTTP request returned error code >= 300: ({}). Body: {}",
                httpRequest.toString(),
                result != null ? result : ""
            );
            // attempt to parse auth0 response to respond with an error message
            String auth0Message = "An Auth0 error occurred";
            JsonNode jsonResponse = null;
            try {
                jsonResponse = mapper.readTree(result);
            } catch (IOException e) {
                LOG.warn("Could not parse json from auth0 error message. Body: {}", result != null ? result : "");
                e.printStackTrace();
            }

            if (jsonResponse != null && jsonResponse.has("message")) {
                auth0Message = String.format("%s: %s", auth0Message, jsonResponse.get("message").asText());
            }

            logMessageAndHalt(req, statusCode, auth0Message);
        }

        LOG.info("Successfully made request: ({})", httpRequest.toString());

        return result;
    }

    private static ZonedDateTime toZonedDateTime (Date date) {
        return ZonedDateTime.ofInstant(date.toInstant(), ZoneOffset.UTC);
    }

    static abstract class Activity implements Serializable {
        private static final long serialVersionUID = 1L;
        public String type;
        public String userId;
        public String userName;
        public Date date;
    }

    static class FeedSourceCreationActivity extends Activity {
        private static final long serialVersionUID = 1L;
        public String feedSourceId;
        public String feedSourceName;
        public String projectId;
        public String projectName;

        public FeedSourceCreationActivity(FeedSource fs, Project proj) {
            this.type = "feed-created";
            this.date = fs.dateCreated;
            this.userId = fs.userId;
            this.userName = fs.userEmail;
            this.feedSourceId = fs.id;
            this.feedSourceName = fs.name;
            this.projectId = proj.id;
            this.projectName = proj.name;
        }
    }

    static class FeedVersionCreationActivity extends Activity {
        private static final long serialVersionUID = 1L;
        public Integer feedVersionIndex;
        public String feedVersionName;
        public String feedSourceId;
        public String feedSourceName;

        public FeedVersionCreationActivity(FeedVersion version, FeedSource fs) {
            this.type = "version-created";
            this.date = version.dateCreated;
            this.userId = version.userId;
            this.userName = version.userEmail;
            this.feedVersionIndex = version.version;
            this.feedVersionName = version.name;
            this.feedSourceId = fs.id;
            this.feedSourceName = fs.name;
        }
    }

    static abstract class CommentActivity extends Activity {
        private static final long serialVersionUID = 1L;
        public String body;

        public CommentActivity (Note note) {
            this.date = note.date;
            this.userId = note.userId;
            this.userName = note.userEmail;
            this.body = note.body;
        }
    }

    static class FeedSourceCommentActivity extends CommentActivity {
        private static final long serialVersionUID = 1L;
        public String feedSourceId;
        public String feedSourceName;

        public FeedSourceCommentActivity(Note note, FeedSource feedSource) {
            super(note);
            this.type = "feed-commented-on";
            this.feedSourceId = feedSource.id;
            this.feedSourceName = feedSource.name;
        }
    }

    static class FeedVersionCommentActivity extends FeedSourceCommentActivity {
        private static final long serialVersionUID = 1L;
        public Integer feedVersionIndex;
        public String feedVersionName;

        public FeedVersionCommentActivity(Note note, FeedSource feedSource, FeedVersion version) {
            super(note, feedSource);
            this.type = "version-commented-on";
            this.feedVersionIndex = version.version;
            this.feedVersionName = version.name;
        }
    }

    /**
     * Used to override the base url for making requests to Auth0. This is primarily used for testing purposes to set
     * the url to something that is stubbed with WireMock.
     */
    public static void setBaseUsersUrl (String url) {
        baseUsersUrl = url;
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/user/:id", UserController::getUser, json::write);
        get(apiPrefix + "secure/user/:id/recentactivity", UserController::getRecentActivity, json::write);
        get(apiPrefix + "secure/user/:id/resendEmailConfirmation", UserController::resendEmailConfirmation, json::write);
        get(apiPrefix + "secure/user", UserController::getAllUsers, json::write);
        get(apiPrefix + "secure/usercount", UserController::getUserCount, json::write);
        post(apiPrefix + "secure/user", UserController::createUser, json::write);
        put(apiPrefix + "secure/user/:id", UserController::updateUser, json::write);
        delete(apiPrefix + "secure/user/:id", UserController::deleteUser, json::write);

        post(apiPrefix + "public/user", UserController::createPublicUser, json::write);
    }
}
