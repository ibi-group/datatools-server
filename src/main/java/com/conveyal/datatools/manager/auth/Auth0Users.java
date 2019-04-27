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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

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
        URIBuilder builder = getURIBuilder();
        builder.setPath("/api/v2/users");
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
        HttpResponse response;

        LOG.info("Making request: ({})", request.toString());

        try {
            response = client.execute(request);
        } catch (IOException e) {
            LOG.error("An exception occurred while making a request to Auth0");
            e.printStackTrace();
            return null;
        }

        String result = null;

        if (response.getEntity() != null) {
            try {
                result = EntityUtils.toString(response.getEntity());
            } catch (IOException e) {
                LOG.error("An exception occurred while parsing a response from Auth0");
                e.printStackTrace();
            }
        } else {
            LOG.warn("No response body available to parse from Auth0 request");
        }

        int statusCode = response.getStatusLine().getStatusCode();
        if(statusCode >= 300) {
            LOG.warn(
                "HTTP request to Auth0 returned error code >= 300: ({}). Body: {}",
                request.toString(),
                result != null ? result : ""
            );
        } else {
            LOG.info("Successfully made request: ({})", request.toString());
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
     * Get a single Auth0 user for the specified ID.
     */
    public static Auth0UserProfile getUserById(String id) {
        URIBuilder builder = getURIBuilder();
        builder.setPath("/api/v2/users/" + id);
        URI uri = null;
        try {
            uri = builder.build();
        } catch (URISyntaxException e) {
            LOG.error("Unable to build URI to getUserById");
            e.printStackTrace();
            return null;
        }
        String response = doRequest(uri);
        Auth0UserProfile user = null;
        try {
            user = mapper.readValue(response, Auth0UserProfile.class);
        } catch (IOException e) {
            LOG.error("Unable to parse user profile response from Auth0! Response: {}", response);
            e.printStackTrace();
        }
        return user;
    }

    /**
     * Creates a new uri builder and sets the scheme, port and host according to whether a test environment is in effect
     */
    private static URIBuilder getURIBuilder() {
        URIBuilder builder = new URIBuilder();
        if (AUTH0_DOMAIN.equals("your-auth0-domain")) {
            // set items for testing purposes assuming use of a Wiremock server
            builder.setScheme("http");
            builder.setPort(8089);
            builder.setHost("localhost");
        } else {
            // use live Auth0 domain
            builder.setScheme("https");
            builder.setHost(AUTH0_DOMAIN);
        }
        return builder;
    }

    /**
     * Get users subscribed to a given target ID.
     */
    public static String getUsersBySubscription(String subscriptionType, String target) {
        return getAuth0Users("app_metadata.datatools.subscriptions.type:" + subscriptionType + " AND app_metadata.datatools.subscriptions.target:" + target);
    }

    public static Set<String> getVerifiedEmailsBySubscription(String subscriptionType, String target) {
        String json = getUsersBySubscription(subscriptionType, target);
        JsonNode firstNode = null;
        Set<String> emails = new HashSet<>();
        try {
            firstNode = mapper.readTree(json);
        } catch (IOException e) {
            LOG.error("Subscribed users list for type={}, target={} is null or unparseable.", subscriptionType, target);
            return emails;
        }
        for (JsonNode user : firstNode) {
            if (!user.has("email")) {
                continue;
            }
            String email = user.get("email").asText();
            Boolean emailVerified = user.get("email_verified").asBoolean();
            // only send email if address has been verified
            if (!emailVerified) {
                LOG.warn("Skipping user {}. User's email address has not been verified.", email);
            } else {
                emails.add(email);
            }
        }

        return emails;
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
