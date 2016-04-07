package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.conveyal.datatools.manager.DataManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import spark.Request;
import spark.Response;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;

import static spark.Spark.*;

/**
 * Created by landon on 3/29/16.
 */
public class UserController {
    private static String AUTH0_DOMAIN = DataManager.config.getProperty("application.auth0.domain");
    private static String AUTH0_API_TOKEN = DataManager.config.getProperty("application.auth0.api_token");
    public static JsonManager<Project> json =
            new JsonManager<>(Project.class, JsonViews.UserInterface.class);

    public static Object getUser(Request req, Response res) throws IOException {
        String url = "https://" + AUTH0_DOMAIN + "/api/v2/users/" + URLEncoder.encode(req.params("id"), "UTF-8");
        System.out.println(url);
        String charset = "UTF-8";
        HttpClient client = new DefaultHttpClient();
        HttpGet request = new HttpGet(url);
        request.addHeader("Authorization", "Bearer " + AUTH0_API_TOKEN);
        request.setHeader("Accept-Charset", charset);
        HttpResponse response = client.execute(request);
        String result = EntityUtils.toString(response.getEntity());
//        System.out.println(result);

        return result;
    }

    public static Object getAllUsers(Request req, Response res) throws IOException {
        String url = "https://" + AUTH0_DOMAIN + "/api/v2/users";
        System.out.println(url);
        String charset = "UTF-8";
        HttpClient client = new DefaultHttpClient();
        HttpGet request = new HttpGet(url);
        request.addHeader("Authorization", "Bearer " + AUTH0_API_TOKEN);
        request.setHeader("Accept-Charset", charset);
        HttpResponse response = client.execute(request);
        String result = EntityUtils.toString(response.getEntity());
        System.out.println(result);
        res.type("application/json");
        return result;
    }

    public static Object createPublicUser(Request req, Response res) throws IOException {
        String url = "https://" + AUTH0_DOMAIN + "/api/v2/users";
        System.out.println(url);
        String charset = "UTF-8";
        JsonNode jsonNode = new ObjectMapper().readTree(req.body());
        HttpClient client = new DefaultHttpClient();
        HttpPost request = new HttpPost(url);
        request.addHeader("Authorization", "Bearer " + AUTH0_API_TOKEN);
        request.setHeader("Accept-Charset", charset);
        request.setHeader("Content-Type", "application/json");
//        System.out.println(jsonNode.get("data"));
        String json = String.format("{ \"connection\": \"Username-Password-Authentication\", \"email\": %s, \"password\": %s, \"app_metadata\": {\"datatools\": {\"permissions\": [], \"projects\": [] } } }", jsonNode.get("email"), jsonNode.get("password"));
//        System.out.println(json);
        HttpEntity entity = new ByteArrayEntity(json.getBytes(charset));
        request.setEntity(entity);
        HttpResponse response = client.execute(request);
        String result = EntityUtils.toString(response.getEntity());

        System.out.println(result);

        return result;
    }

    public static Object createUser(Request req, Response res) throws IOException {
        String url = "https://" + AUTH0_DOMAIN + "/api/v2/users";
        System.out.println(url);
        String charset = "UTF-8";
        JsonNode jsonNode = new ObjectMapper().readTree(req.body());
        HttpClient client = new DefaultHttpClient();
        HttpPost request = new HttpPost(url);
        request.addHeader("Authorization", "Bearer " + AUTH0_API_TOKEN);
        request.setHeader("Accept-Charset", charset);
        request.setHeader("Content-Type", "application/json");
//        System.out.println(jsonNode.get("data"));
        String json = String.format("{ \"connection\": \"Username-Password-Authentication\", \"email\": %s, \"password\": %s, \"app_metadata\": {\"datatools\": %s } }", jsonNode.get("email"), jsonNode.get("password"), jsonNode.get("permissions"));
//        System.out.println(json);
        HttpEntity entity = new ByteArrayEntity(json.getBytes(charset));
        request.setEntity(entity);
        HttpResponse response = client.execute(request);
        String result = EntityUtils.toString(response.getEntity());

        System.out.println(result);

        return result;
    }

    public static Object updateUser(Request req, Response res) throws IOException {
        String url = "https://" + AUTH0_DOMAIN + "/api/v2/users/" + URLEncoder.encode(req.params("id"), "UTF-8");
        System.out.println(url);
        String charset = "UTF-8";
        JsonNode jsonNode = new ObjectMapper().readTree(req.body());
        HttpClient client = new DefaultHttpClient();
        HttpPatch request = new HttpPatch(url);
        request.addHeader("Authorization", "Bearer " + AUTH0_API_TOKEN);
        request.setHeader("Accept-Charset", charset);
        request.setHeader("Content-Type", "application/json");
//        System.out.println(jsonNode.get("data"));
        String json = "{ \"app_metadata\": { \"datatools\" : " + jsonNode.get("data") + " }}";
//        System.out.println(json);
        HttpEntity entity = new ByteArrayEntity(json.getBytes(charset));
        request.setEntity(entity);
        HttpResponse response = client.execute(request);
        String result = EntityUtils.toString(response.getEntity());
//        res.type("application/json");
//        System.out.println(result);

        return result;
    }

    public static Object deleteUser(Request req, Response res){
        return true;
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/user/:id", UserController::getUser, json::write);
        get(apiPrefix + "secure/user", UserController::getAllUsers, json::write);
        post(apiPrefix + "secure/user", UserController::createUser, json::write);
        put(apiPrefix + "secure/user/:id", UserController::updateUser, json::write);
        delete(apiPrefix + "secure/user/:id", UserController::deleteUser, json::write);

        post(apiPrefix + "public/user", UserController::createPublicUser, json::write);
    }
}
