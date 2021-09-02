package com.conveyal.datatools.manager.auth;

import com.conveyal.datatools.manager.DataManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class contains methods for querying Auth0 users using the Auth0 User Management API. Auth0 docs describing the
 * searchable fields and query syntax are here: https://auth0.com/docs/api/management/v2/user-search
 */
public class Auth0Users {
    private static final String AUTH0_DOMAIN = DataManager.getConfigPropertyAsText("AUTH0_DOMAIN");
    // This client/secret pair is for making requests for an API access token used with the Management API.
    private static final String AUTH0_API_CLIENT = DataManager.getConfigPropertyAsText("AUTH0_API_CLIENT");
    private static final String AUTH0_API_SECRET = DataManager.getConfigPropertyAsText("AUTH0_API_SECRET");
    // This is the UI client ID which is currently used to synchronize the user permissions object between server and UI.
    private static final String clientId = DataManager.getConfigPropertyAsText("AUTH0_CLIENT_ID");
    private static final String MANAGEMENT_API_VERSION = "v2";
    private static final String SEARCH_API_VERSION = "v3";
    public static final String API_PATH = "/api/" + MANAGEMENT_API_VERSION;
    public static final String USERS_API_PATH = API_PATH + "/users";
    public static final int DEFAULT_ITEMS_PER_PAGE = 10;
    // Cached API token so that we do not have to request a new one each time a Management API request is made.
    private static Auth0AccessToken cachedToken = null;
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(Auth0Users.class);

    /**
     * Overload of user search query URL to restrict search to current users only.
     */
    private static URI getSearchUrl(String searchQuery, int page, int perPage, boolean includeTotals) {
        return getSearchUrl(searchQuery, page, perPage, includeTotals, true);
    }

    /**
     * Constructs a user search query URL.
     * @param searchQuery   search query to perform (null value implies default query)
     * @param page          which page of users to return
     * @param perPage       number of users to return per page. Max value is 1000 per Auth0 docs:
     *                      https://auth0.com/docs/users/user-search/view-search-results-by-page#limitation
     * @param includeTotals whether to include the total number of users in search results
     * @param limitToCurrentUsers whether to restrict the search to current users only
     * @return              URI to perform the search query
     */
    private static URI getSearchUrl(String searchQuery, int page, int perPage, boolean includeTotals, boolean limitToCurrentUsers) {
        // Filter users by datatools client_id, unless excluded (by limitToCurrentUsers) and a search query is provided.
        // This allows for a less restricted, wider search to be carried out on tenant users.
        String searchCurrentUsersOnly = "app_metadata.datatools.client_id:" + clientId;
        URIBuilder builder = getURIBuilder();
        builder.setPath(USERS_API_PATH);
        builder.setParameter("sort", "email:1");
        builder.setParameter("per_page", Integer.toString(perPage));
        builder.setParameter("page", Integer.toString(page));
        builder.setParameter("include_totals", Boolean.toString(includeTotals));
        if (searchQuery != null) {
            builder.setParameter("search_engine", SEARCH_API_VERSION);
            builder.setParameter("q", limitToCurrentUsers ? searchQuery + " AND " + searchCurrentUsersOnly : searchQuery);
        } else {
            builder.setParameter("search_engine", SEARCH_API_VERSION);
            builder.setParameter("q", searchCurrentUsersOnly);
        }

        URI uri;

        try {
            uri = builder.build();
            return uri;
        } catch (URISyntaxException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Perform user search query, returning results as a JSON string.
     */
    private static String doRequest(URI uri) {
        LOG.info("Auth0 getUsers URL=" + uri);
        String charset = "UTF-8";

        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(uri);
        String apiToken = getApiToken();
        if (apiToken == null) {
            LOG.error("API access token is null, aborting Auth0 request");
            return null;
        }
        request.addHeader("Authorization", "Bearer " + apiToken);
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
     * Gets an Auth0 API access token for authenticating requests to the Auth0 Management API. This will either create
     * a new token using the oauth token endpoint or grab a cached token that it has already created (if it has not
     * expired). More information on setting this up is here: https://auth0.com/docs/api/management/v2/get-access-tokens-for-production
     */
    public static String getApiToken() {
        long nowInMillis = new Date().getTime();
        // If cached token has not expired, use it instead of requesting a new one.
        if (cachedToken != null && cachedToken.getExpirationTime() > nowInMillis) {
            long minutesToExpiration = (cachedToken.getExpirationTime() - nowInMillis) / 1000 / 60;
            LOG.info("Using cached token (expires in {} minutes)", minutesToExpiration);
            return cachedToken.access_token;
        }
        LOG.info("Getting new Auth0 API access token (cached token does not exist or has expired).");
        // Create client and build URL.
        HttpClient client = HttpClientBuilder.create().build();
        URIBuilder builder = getURIBuilder();
        String responseString;
        try {
            // First get base url for use in audience URL param. (Trailing slash required.)
            final String audienceUrl = builder.setPath(API_PATH + "/").build().toString();
            URI uri = builder.setPath("/oauth/token").build();
            // Make POST request to Auth0 for new token.
            HttpPost post = new HttpPost(uri);
            post.setHeader("content-type", "application/x-www-form-urlencoded");
            List<NameValuePair> urlParameters = new ArrayList<>();
            urlParameters.add(new BasicNameValuePair("grant_type", "client_credentials"));
            urlParameters.add(new BasicNameValuePair("client_id", AUTH0_API_CLIENT));
            urlParameters.add(new BasicNameValuePair("client_secret", AUTH0_API_SECRET));
            urlParameters.add(new BasicNameValuePair("audience", audienceUrl));
            post.setEntity(new UrlEncodedFormEntity(urlParameters));
            HttpResponse response = client.execute(post);
            // Read response code/entity.
            int code = response.getStatusLine().getStatusCode();
            responseString = EntityUtils.toString(response.getEntity());
            if (code >= 300) {
                LOG.error("Could not get Auth0 API token {}", responseString);
                throw new IllegalStateException("Bad response for Auth0 token");
            }
        } catch (IllegalStateException | URISyntaxException | IOException e) {
            e.printStackTrace();
            return null;
        }
        // Parse API Token.
        Auth0AccessToken auth0AccessToken;
        try {
            auth0AccessToken = mapper.readValue(responseString, Auth0AccessToken.class);
        } catch (IOException e) {
            LOG.error("Error parsing Auth0 API access token.", e);
            return null;
        }
        if (auth0AccessToken.scope == null) {
            // TODO: Somehow verify that the scope of the token supports the original request's operation? Right now
            //  we expect that the scope covers fully all of the operations handled by this application (i.e., update
            //  user, delete user, etc.), which is something that must be configured in the Auth0 dashboard.
            LOG.error("API access token has invalid scope.");
            return null;
        }
        // Cache token for later use and return token string.
        setCachedApiToken(auth0AccessToken);
        return getCachedApiToken().access_token;
    }

    /** Set the cached API token to the input parameter. */
    public static void setCachedApiToken(Auth0AccessToken accessToken) {
        cachedToken = accessToken;
    }

    /** Set the cached API token to the input parameter. */
    public static Auth0AccessToken getCachedApiToken() {
        return cachedToken;
    }

    /**
     * Wrapper method for performing user search with default per page count.
     * @return JSON string of users matching search query
     */
    public static String getAuth0Users(String searchQuery, int page) {
        return getAuth0Users(searchQuery, page, DEFAULT_ITEMS_PER_PAGE);
    }

    /**
     * Wrapper method for performing user search.
     * @return JSON string of users matching search query
     */
    public static String getAuth0Users(String searchQuery, int page, int perPage) {
        URI uri = getSearchUrl(searchQuery, page, perPage, false);
        return doRequest(uri);
    }

    /**
     * Wrapper method for performing user search with default per page count. It also opens the search to all tenant
     * users by excluding the restriction to current datatool users.
     * @param searchQuery The search criteria.
     * @return JSON string of users matching search query.
     */
    public static String getUnrestrictedAuth0Users(String searchQuery) {
        URI uri = getSearchUrl(searchQuery, 0, DEFAULT_ITEMS_PER_PAGE, false, false);
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
        builder.setPath(String.join("/", USERS_API_PATH, id));
        URI uri = null;
        try {
            uri = builder.build();
        } catch (URISyntaxException e) {
            LOG.error("Unable to build URI to getUserById");
            e.printStackTrace();
            return null;
        }
        String response = doRequest(uri);
        if (response == null) {
            LOG.error("Auth0 request aborted due to issues during request.");
            return null;
        }
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
        if ("your-auth0-domain".equals(AUTH0_DOMAIN)) {
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
        if (Auth0Connection.isAuthDisabled()) {
            LOG.warn("Auth is disabled. Skipping Auth0 request for subscribed users.");
            return "";
        }
        return getAuth0Users("app_metadata.datatools.subscriptions.type:" + subscriptionType + " AND app_metadata.datatools.subscriptions.target:" + target);
    }

    public static Set<String> getVerifiedEmailsBySubscription(String subscriptionType, String target) {
        String json = getUsersBySubscription(subscriptionType, target);
        JsonNode firstNode;
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
            boolean emailVerified = user.get("email_verified").asBoolean();
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
        URI uri = getSearchUrl(searchQuery, 0, 1, true);
        String result = doRequest(uri);
        JsonNode jsonNode = new ObjectMapper().readTree(result);
        return jsonNode.get("total").asInt();
    }

}
