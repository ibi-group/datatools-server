package com.conveyal.datatools.manager.auth;

import com.conveyal.datatools.manager.DataManager;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.HashSet;

/**
 * This class contains methods for querying Auth0 users using the Auth0 User Management API. Auth0 docs describing the
 * searchable fields and query syntax are here: https://auth0.com/docs/api/management/v2/user-search
 */
public class Auth0Users {
    private static final String AUTH0_DOMAIN = DataManager.getConfigPropertyAsText("AUTH0_DOMAIN");
    private static final String AUTH0_API_TOKEN = DataManager.getConfigPropertyAsText("AUTH0_TOKEN");
    private static final String clientId = DataManager.getConfigPropertyAsText("AUTH0_CLIENT_ID");
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(Auth0Users.class);

    /**
     * Constructs a user search query URL.
     * @param searchQuery   search query to perform (null value implies default query)
     * @param page          which page of users to return
     * @param perPage       number of users to return per page
     * @param includeTotals whether to include the total number of users in search results
     * @return              URI to perform the search query
     */
    private static URI getUrl(String searchQuery, int page, int perPage, boolean includeTotals) {
        // always filter users by datatools client_id
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

    /**
     * Perform user search query, returning results as a JSON string.
     */
    private static String doRequest(URI uri) {
        LOG.info("Auth0 getUsers URL=" + uri);
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

    /**
     * Wrapper method for performing user search with default per page count.
     * @return JSON string of users matching search query
     */
    public static String getAuth0Users(String searchQuery, int page) {

        URI uri = getUrl(searchQuery, page, 10, false);
        return doRequest(uri);
    }

    /**
     * Wrapper method for performing user search with default per page count and page number = 0.
     */
    public static String getAuth0Users(String queryString) {
        return getAuth0Users(queryString, 0);
    }

    /**
     * Get all users for this application (using the default search).
     */
    public static Collection<Auth0UserProfile> getAll () {
        Collection<Auth0UserProfile> users = new HashSet<>();

        // limited to the first 100
        URI uri = getUrl(null, 0, 100, false);
        String response = doRequest(uri);
        try {
            users = mapper.readValue(response, new TypeReference<Collection<Auth0UserProfile>>(){});
        } catch (IOException e) {
            e.printStackTrace();
        }
        return users;
    }

    /**
     * Get a single Auth0 user for the specified ID.
     */
    public static Auth0UserProfile getUserById(String id) {

        URIBuilder builder = new URIBuilder();
        builder.setScheme("https").setHost(AUTH0_DOMAIN).setPath("/api/v2/users/" + id);
        URI uri = null;
        try {
            uri = builder.build();

        } catch (URISyntaxException e) {
            e.printStackTrace();
            return null;
        }
        String response = doRequest(uri);
        Auth0UserProfile user = null;
        try {
            user = mapper.readValue(response, Auth0UserProfile.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return user;
    }

    /**
     * Get users subscribed to a given target ID.
     */
    public static String getUsersBySubscription(String subscriptionType, String target) {
        return getAuth0Users("app_metadata.datatools.subscriptions.type:" + subscriptionType + " AND app_metadata.datatools.subscriptions.target:" + target);
    }

    /**
     * Get users belong to a specified organization.
     */
    public static String getUsersForOrganization(String organizationId) {
        return getAuth0Users("app_metadata.datatools.organizations.organization_id:" + organizationId);
    }

    /**
     * Get number of users for the application.
     */
    public static int getAuth0UserCount(String searchQuery) throws IOException {
        URI uri = getUrl(searchQuery, 0, 1, true);
        String result = doRequest(uri);
        JsonNode jsonNode = new ObjectMapper().readTree(result);
        return jsonNode.get("total").asInt();
    }

}
