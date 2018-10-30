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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.io.Serializable;
import java.net.URLEncoder;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.conveyal.datatools.common.utils.SparkUtils.haltWithMessage;
import static com.conveyal.datatools.manager.auth.Auth0Users.getUserById;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;
import static spark.Spark.put;

/**
 * Handles the HTTP endpoints related to CRUD operations for Auth0 users.
 */
public class UserController {

    private static String AUTH0_DOMAIN = DataManager.getConfigPropertyAsText("AUTH0_DOMAIN");
    private static String AUTH0_CLIENT_ID = DataManager.getConfigPropertyAsText("AUTH0_CLIENT_ID");
    private static String AUTH0_API_TOKEN = DataManager.getConfigPropertyAsText("AUTH0_TOKEN");
    private static Logger LOG = LoggerFactory.getLogger(UserController.class);
    private static ObjectMapper mapper = new ObjectMapper();
    public static JsonManager<Project> json =
            new JsonManager<>(Project.class, JsonViews.UserInterface.class);

    /**
     * HTTP endpoint to get a single Auth0 user for the application (by specified ID param). Note, this uses a different
     * Auth0 API (get user) than the other get methods (user search query).
     */
    private static String getUser(Request req, Response res) throws IOException {
        String url = "https://" + AUTH0_DOMAIN + "/api/v2/users/" + URLEncoder.encode(req.params("id"), "UTF-8");
        String charset = "UTF-8";

        HttpGet request = new HttpGet(url);
        request.addHeader("Authorization", "Bearer " + AUTH0_API_TOKEN);
        request.setHeader("Accept-Charset", charset);

        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(request);
        String result = EntityUtils.toString(response.getEntity());

        return result;
    }

    /**
     * HTTP endpoint to get all users for the application (using a filtered search on all users for the Auth0 tenant).
     */
    private static String getAllUsers(Request req, Response res) throws IOException {
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
            haltWithMessage(req, 401, "Must be application or organization admin to view users");
            // Return statement cannot be reached due to halt.
            return null;
        }
    }

    /**
     * Gets the total count of users that match the filtered user search query.
     */
    private static int getUserCount(Request req, Response res) throws IOException {
        res.type("application/json");
        String queryString = filterUserSearchQuery(req);
        return Auth0Users.getAuth0UserCount(queryString);
    }

    /**
     * HTTP endpoint to create a "public user" that has no permissions to access projects in the application.
     *
     * Note, this passes a "blank" app_metadata object to the newly created user, so there is no risk of someone
     * injecting permissions somehow into the create user request.
     */
    private static String createPublicUser(Request req, Response res) throws IOException {
        String url = "https://" + AUTH0_DOMAIN + "/api/v2/users";
        String charset = "UTF-8";

        HttpPost request = new HttpPost(url);
        request.addHeader("Authorization", "Bearer " + AUTH0_API_TOKEN);
        request.setHeader("Accept-Charset", charset);
        request.setHeader("Content-Type", "application/json");
        JsonNode jsonNode = mapper.readTree(req.body());
        String json = String.format("{" +
                "\"connection\": \"Username-Password-Authentication\"," +
                "\"email\": %s," +
                "\"password\": %s," +
                "\"app_metadata\": {\"datatools\": [{\"permissions\": [], \"projects\": [], \"subscriptions\": [], \"client_id\": \"%s\" }] } }",
                jsonNode.get("email"), jsonNode.get("password"), AUTH0_CLIENT_ID);
        HttpEntity entity = new ByteArrayEntity(json.getBytes(charset));
        request.setEntity(entity);

        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(request);
        String result = EntityUtils.toString(response.getEntity());
        int statusCode = response.getStatusLine().getStatusCode();
        if(statusCode >= 300) haltWithMessage(req, statusCode, response.toString());

        return result;
    }

    /**
     * HTTP endpoint to create new Auth0 user for the application.
     *
     * FIXME: This endpoint fails if the user's email already exists in the Auth0 tenant.
     */
    private static String createUser(Request req, Response res) throws IOException {
        String url = "https://" + AUTH0_DOMAIN + "/api/v2/users";
        String charset = "UTF-8";

        HttpPost request = new HttpPost(url);
        request.addHeader("Authorization", "Bearer " + AUTH0_API_TOKEN);
        request.setHeader("Accept-Charset", charset);
        request.setHeader("Content-Type", "application/json");
        JsonNode jsonNode = mapper.readTree(req.body());
        String json = String.format("{" +
                "\"connection\": \"Username-Password-Authentication\"," +
                "\"email\": %s," +
                "\"password\": %s," +
                "\"app_metadata\": {\"datatools\": [%s] } }"
                , jsonNode.get("email"), jsonNode.get("password"), jsonNode.get("permissions"));
        HttpEntity entity = new ByteArrayEntity(json.getBytes(charset));
        request.setEntity(entity);

        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(request);
        String result = EntityUtils.toString(response.getEntity());

        int statusCode = response.getStatusLine().getStatusCode();
        if(statusCode >= 300) haltWithMessage(req, statusCode, response.toString());

        System.out.println(result);

        return result;
    }

    private static Object updateUser(Request req, Response res) throws IOException {
        String userId = req.params("id");
        Auth0UserProfile user = getUserById(userId);

        LOG.info("Updating user {}", user.getEmail());

        String url = "https://" + AUTH0_DOMAIN + "/api/v2/users/" + URLEncoder.encode(userId, "UTF-8");
        String charset = "UTF-8";


        HttpPatch request = new HttpPatch(url);

        request.addHeader("Authorization", "Bearer " + AUTH0_API_TOKEN);
        request.setHeader("Accept-Charset", charset);
        request.setHeader("Content-Type", "application/json");

        JsonNode jsonNode = mapper.readTree(req.body());
//        JsonNode data = mapper.readValue(jsonNode.retrieveById("data"), Auth0UserProfile.DatatoolsInfo.class); //jsonNode.retrieveById("data");
        JsonNode data = jsonNode.get("data");
        System.out.println(data.asText());
        Iterator<Map.Entry<String, JsonNode>> fieldsIter = data.fields();
        while (fieldsIter.hasNext()) {
            Map.Entry<String, JsonNode> entry = fieldsIter.next();
            System.out.println(entry.getValue());
        }
//        if (!data.has("client_id")) {
//            ((ObjectNode)data).put("client_id", DataManager.config.retrieveById("auth0").retrieveById("client_id").asText());
//        }
        String json = "{ \"app_metadata\": { \"datatools\" : " + data + " }}";
        System.out.println(json);
        HttpEntity entity = new ByteArrayEntity(json.getBytes(charset));
        request.setEntity(entity);

        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(request);
        String result = EntityUtils.toString(response.getEntity());

        return mapper.readTree(result);
    }

    private static Object deleteUser(Request req, Response res) throws IOException {
        String url = "https://" + AUTH0_DOMAIN + "/api/v2/users/" + URLEncoder.encode(req.params("id"), "UTF-8");
        String charset = "UTF-8";

        HttpDelete request = new HttpDelete(url);
        request.addHeader("Authorization", "Bearer " + AUTH0_API_TOKEN);
        request.setHeader("Accept-Charset", charset);

        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(request);
        int statusCode = response.getStatusLine().getStatusCode();
        if(statusCode >= 300) haltWithMessage(req, statusCode, response.getStatusLine().getReasonPhrase());

        return true;
    }

    private static Object getRecentActivity(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");

        /* TODO: Allow custom from/to range
        String fromStr = req.queryParams("from");
        String toStr = req.queryParams("to"); */

        // Default range: past 7 days
        ZonedDateTime from = ZonedDateTime.now(ZoneOffset.UTC).minusDays(7);
        ZonedDateTime to = ZonedDateTime.now(ZoneOffset.UTC);

        List<Activity> activityList = new ArrayList<>();
        Auth0UserProfile.DatatoolsInfo datatools = userProfile.getApp_metadata().getDatatoolsInfo();
        if (datatools == null) {
            // NOTE: this condition will also occur if DISABLE_AUTH is set to true
            haltWithMessage(req, 403, "User does not have permission to access to this application");
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

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/user/:id", UserController::getUser, json::write);
        get(apiPrefix + "secure/user/:id/recentactivity", UserController::getRecentActivity, json::write);
        get(apiPrefix + "secure/user", UserController::getAllUsers, json::write);
        get(apiPrefix + "secure/usercount", UserController::getUserCount, json::write);
        post(apiPrefix + "secure/user", UserController::createUser, json::write);
        put(apiPrefix + "secure/user/:id", UserController::updateUser, json::write);
        delete(apiPrefix + "secure/user/:id", UserController::deleteUser, json::write);

        post(apiPrefix + "public/user", UserController::createPublicUser, json::write);
    }
}
