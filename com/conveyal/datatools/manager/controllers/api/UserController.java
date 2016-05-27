package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.conveyal.datatools.manager.DataManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.*;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.Map;

import com.conveyal.datatools.manager.auth.Auth0Users;

import static com.conveyal.datatools.manager.auth.Auth0Users.getUserById;
import static spark.Spark.*;

/**
 * Created by landon on 3/29/16.
 */
public class UserController {

    private static String AUTH0_DOMAIN = DataManager.config.get("auth0").get("domain").asText();
    private static String AUTH0_API_TOKEN = DataManager.serverConfig.get("auth0").get("api_token").asText();
    private static Logger LOG = LoggerFactory.getLogger(UserController.class);
    private static ObjectMapper mapper = new ObjectMapper();
    public static JsonManager<Project> json =
            new JsonManager<>(Project.class, JsonViews.UserInterface.class);

    public static Object getUser(Request req, Response res) throws IOException {
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

    public static Object getAllUsers(Request req, Response res) throws IOException {
        res.type("application/json");
        int page = Integer.parseInt(req.queryParams("page"));
        String queryString = req.queryParams("queryString");
        if(queryString != null) queryString = "email:" + queryString + "*";

        return mapper.readTree(Auth0Users.getAuth0Users(queryString, page));
    }

    public static Object getUserCount(Request req, Response res) throws IOException {
        res.type("application/json");
        String queryString = req.queryParams("queryString");
        if(queryString != null) queryString = "email:" + queryString + "*";
        return Auth0Users.getAuth0UserCount(queryString);
    }

    public static Object createPublicUser(Request req, Response res) throws IOException {
        String url = "https://" + AUTH0_DOMAIN + "/api/v2/users";
        String charset = "UTF-8";

        HttpPost request = new HttpPost(url);
        request.addHeader("Authorization", "Bearer " + AUTH0_API_TOKEN);
        request.setHeader("Accept-Charset", charset);
        request.setHeader("Content-Type", "application/json");
        JsonNode jsonNode = mapper.readTree(req.body());
        String json = String.format("{ \"connection\": \"Username-Password-Authentication\", \"email\": %s, \"password\": %s, \"app_metadata\": {\"datatools\": {\"permissions\": [], \"projects\": [] } } }", jsonNode.get("email"), jsonNode.get("password"));
        HttpEntity entity = new ByteArrayEntity(json.getBytes(charset));
        request.setEntity(entity);

        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(request);
        String result = EntityUtils.toString(response.getEntity());

        return result;
    }

    public static Object createUser(Request req, Response res) throws IOException {
        String url = "https://" + AUTH0_DOMAIN + "/api/v2/users";
        String charset = "UTF-8";

        HttpPost request = new HttpPost(url);
        request.addHeader("Authorization", "Bearer " + AUTH0_API_TOKEN);
        request.setHeader("Accept-Charset", charset);
        request.setHeader("Content-Type", "application/json");
        JsonNode jsonNode = mapper.readTree(req.body());
        String json = String.format("{ \"connection\": \"Username-Password-Authentication\", \"email\": %s, \"password\": %s, \"app_metadata\": {\"datatools\": [%s] } }", jsonNode.get("email"), jsonNode.get("password"), jsonNode.get("permissions"));
        HttpEntity entity = new ByteArrayEntity(json.getBytes(charset));
        request.setEntity(entity);

        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(request);
        String result = EntityUtils.toString(response.getEntity());

        System.out.println(result);

        return result;
    }

    public static Object updateUser(Request req, Response res) throws IOException {
        String userId = req.params("id");
        Auth0UserProfile user = mapper.readValue(getUserById(userId), Auth0UserProfile.class);

        LOG.info("Updating user {}", user.getEmail());

        String url = "https://" + AUTH0_DOMAIN + "/api/v2/users/" + URLEncoder.encode(userId, "UTF-8");
        String charset = "UTF-8";


        HttpPatch request = new HttpPatch(url);

        request.addHeader("Authorization", "Bearer " + AUTH0_API_TOKEN);
        request.setHeader("Accept-Charset", charset);
        request.setHeader("Content-Type", "application/json");

        JsonNode jsonNode = mapper.readTree(req.body());
//        JsonNode data = mapper.readValue(jsonNode.get("data"), Auth0UserProfile.DatatoolsInfo.class); //jsonNode.get("data");
        JsonNode data = jsonNode.get("data");
        System.out.println(data.asText());
        Iterator<Map.Entry<String, JsonNode>> fieldsIter = data.fields();
        while (fieldsIter.hasNext()) {
            Map.Entry<String, JsonNode> entry = fieldsIter.next();
            System.out.println(entry.getValue());
        }
//        if (!data.has("client_id")) {
//            ((ObjectNode)data).put("client_id", DataManager.config.get("auth0").get("client_id").asText());
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

    public static Object deleteUser(Request req, Response res) throws IOException {
        String url = "https://" + AUTH0_DOMAIN + "/api/v2/users/" + URLEncoder.encode(req.params("id"), "UTF-8");
        String charset = "UTF-8";

        HttpDelete request = new HttpDelete(url);
        request.addHeader("Authorization", "Bearer " + AUTH0_API_TOKEN);
        request.setHeader("Accept-Charset", charset);

        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(request);
        int statusCode = response.getStatusLine().getStatusCode();
        if(statusCode >= 300) halt(statusCode);

        return true;
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/user/:id", UserController::getUser, json::write);
        get(apiPrefix + "secure/user", UserController::getAllUsers, json::write);
        get(apiPrefix + "secure/usercount", UserController::getUserCount, json::write);
        post(apiPrefix + "secure/user", UserController::createUser, json::write);
        put(apiPrefix + "secure/user/:id", UserController::updateUser, json::write);
        delete(apiPrefix + "secure/user/:id", UserController::deleteUser, json::write);

        post(apiPrefix + "public/user", UserController::createPublicUser, json::write);
    }
}
