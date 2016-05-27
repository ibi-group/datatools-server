package com.conveyal.datatools.manager.auth;

import com.conveyal.datatools.manager.DataManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;

/**
 * Created by landon on 4/26/16.
 */
public class Auth0Users {
    private static String AUTH0_DOMAIN = DataManager.config.get("auth0").get("domain").asText();
    private static String AUTH0_API_TOKEN = DataManager.serverConfig.get("auth0").get("api_token").asText();

    private static URI getUrl(String searchQuery, int page, int perPage, boolean includeTotals) {
        String clientId = DataManager.config.get("auth0").get("client_id").asText();
        String defaultQuery = "app_metadata.datatools.client_id:" + clientId;
        URIBuilder builder = new URIBuilder();
        builder.setScheme("https").setHost(AUTH0_DOMAIN).setPath("/api/v2/users");
        builder.setParameter("sort", "email:1");
        builder.setParameter("per_page", Integer.toString(perPage));
        builder.setParameter("page", Integer.toString(page));
        builder.setParameter("include_totals", Boolean.toString(includeTotals));
        if (searchQuery != null) {
            builder.setParameter("search_engine", "v2");
            builder.setParameter("q", searchQuery + " AND " + defaultQuery);
        }
        else {
            builder.setParameter("search_engine", "v2");
            builder.setParameter("q", defaultQuery);
        }

        URI uri = null;

        try {
            uri = builder.build();

        } catch (URISyntaxException e) {
            e.printStackTrace();
            return null;
        }

        return uri;
    }

    private static String doRequest(URI uri) {
        System.out.println("Auth0 getUsers URL=" + uri);
        String charset = "UTF-8";

        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(uri);

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

        return result;
    }

    public static String getAuth0Users(String searchQuery, int page) {

        URI uri = getUrl(searchQuery, page, 10, false);
        return doRequest(uri);
    }

    public static String getUserById(String id) {

        URIBuilder builder = new URIBuilder();
        builder.setScheme("https").setHost(AUTH0_DOMAIN).setPath("/api/v2/users/" + id);
        URI uri = null;
        try {
            uri = builder.build();

        } catch (URISyntaxException e) {
            e.printStackTrace();
            return null;
        }
        return doRequest(uri);
    }

    public static String getUsersBySubscription(String subscriptionType, String target) {
        return getAuth0Users("app_metadata.datatools.subscriptions.type:" + subscriptionType + " AND app_metadata.datatools.subscriptions.target:" + target);
    }

    public static String getAuth0Users(String queryString) {
        return getAuth0Users(queryString, 0);
    }

    public static JsonNode getAuth0UserCount(String searchQuery) throws IOException {
        URI uri = getUrl(searchQuery, 0, 1, true);
        String result = doRequest(uri);
        JsonNode jsonNode = new ObjectMapper().readTree(result);
        return jsonNode.get("total");
    }

}
