package com.conveyal.datatools.manager.auth;

import com.auth0.jwt.JWTExpiredException;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.pem.PemReader;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.util.Map;

import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;
import static com.conveyal.datatools.manager.DataManager.getConfigPropertyAsText;
import static com.conveyal.datatools.manager.DataManager.hasConfigProperty;
import static com.conveyal.datatools.manager.controllers.api.UserController.inTestingEnvironment;

/**
 * This handles verifying the Auth0 token passed in the Auth header of Spark HTTP requests.
 *
 * Created by demory on 3/22/16.
 */

public class Auth0Connection {
    public static final String APP_METADATA = "app_metadata";
    public static final String USER_METADATA = "user_metadata";
    public static final String SCOPE = "http://datatools";
    public static final String SCOPED_APP_METADATA = String.join("/", SCOPE, APP_METADATA);
    public static final String SCOPED_USER_METADATA = String.join("/", SCOPE, USER_METADATA);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(Auth0Connection.class);
    private static JWTVerifier verifier;

    /**
     * Whether authentication is disabled for the HTTP endpoints. This defaults to the value in the config file, but can
     * be overridden (e.g., in tests) with {@link #setAuthDisabled(boolean)}.
     */
    private static boolean authDisabled = getDefaultAuthDisabled();
    private static Auth0UserProfile testUser = getDefaultTestUser();


    /**
     * Check the incoming API request for the user token (and verify it) and assign as the "user" attribute on the
     * incoming request object for use in downstream controllers.
     * @param req Spark request object
     */
    public static void checkUser(Request req) {
        if (isAuthDisabled() || inTestingEnvironment()) {
            // If in a development or testing environment, assign a test user (defaults to an application admin) to the
            // request attribute and skip authentication.
            req.attribute("user", getTestUser());
            return;
        }
        // Check that auth header is present and formatted correctly (Authorization: Bearer [token]).
        final String authHeader = req.headers("Authorization");
        if (authHeader == null) {
            logMessageAndHalt(req, 401, "Authorization header is missing.");
        }
        String[] parts = authHeader.split(" ");
        if (parts.length != 2 || !"bearer".equals(parts[0].toLowerCase())) {
            logMessageAndHalt(req, 401, String.format("Authorization header is malformed: %s", authHeader));
        }
        // Retrieve token from auth header.
        String token = parts[1];
        if (token == null) {
            logMessageAndHalt(req, 401, "Could not find authorization token");
        }
        // Handle getting the verifier outside of the below verification try/catch, which is intended to catch issues
        // with the client request. (getVerifier has its own exception/halt handling).
        verifier = getVerifier(req);
        // Validate the JWT and cast into the user profile, which will be attached as an attribute on the request object
        // for downstream controllers to check permissions.
        try {
            Map<String, Object> jwt = verifier.verify(token);
            remapTokenValues(jwt);
            Auth0UserProfile profile = MAPPER.convertValue(jwt, Auth0UserProfile.class);
            // The user attribute is used on the server side to check user permissions and does not have all of the
            // fields that the raw Auth0 profile string does.
            req.attribute("user", profile);
        } catch (JWTExpiredException e) {
            LOG.warn("JWT token has expired for user.");
            logMessageAndHalt(req, 401, "User's authentication token has expired. Please re-login.");
        } catch (Exception e) {
            LOG.warn("Login failed to verify with our authorization provider.", e);
            logMessageAndHalt(req, 401, "Could not verify user's token");
        }
    }

    /**
     * @return the actively applied test user when running the application in a test environment.
     */
    private static Auth0UserProfile getTestUser() {
        return testUser;
    }

    /**
     * @return the default test user (an application admin) when running the application in a test environment.
     */
    public static Auth0UserProfile getDefaultTestUser() {
        return Auth0UserProfile.createTestAdminUser();
    }

    /**
     * This method allows test classes to override the default test user when running the application in a test
     * environment.
     *
     * NOTE: Following the conclusion of a test where this method is called, it is generally recommended that you call
     * this method again (e.g., in an @afterEach method) with {@link #getDefaultTestUser()} to reset things.
     */
    public static void setTestUser(Auth0UserProfile updatedUser) {
        testUser = updatedUser;
    }

    /**
     * Choose the correct JWT verification algorithm (based on the values present in env.yml config) and get the
     * respective verifier.
     */
    private static JWTVerifier getVerifier(Request req) {
        if (verifier == null) {
            try {
                if (hasConfigProperty("AUTH0_SECRET")) {
                    // Use HS256 algorithm to verify token (uses client secret).
                    byte[] decodedSecret = new org.apache.commons.codec.binary.Base64().decode(getConfigPropertyAsText("AUTH0_SECRET"));
                    verifier = new JWTVerifier(decodedSecret);
                } else if (hasConfigProperty("AUTH0_PUBLIC_KEY")) {
                    // Use RS256 algorithm to verify token (uses public key/.pem file).
                    PublicKey publicKey = PemReader.readPublicKey(getConfigPropertyAsText("AUTH0_PUBLIC_KEY"));
                    verifier = new JWTVerifier(publicKey);
                } else throw new IllegalStateException("Auth0 public key or secret token must be defined in config (env.yml).");
            } catch (IllegalStateException | NullPointerException | NoSuchAlgorithmException | IOException | NoSuchProviderException | InvalidKeySpecException e) {
                LOG.error("Auth0 verifier configured incorrectly.");
                logMessageAndHalt(req, 500, "Server authentication configured incorrectly.", e);
            }
        }
        return verifier;
    }

    /**
     * Handle mapping token values to the expected keys. This accounts for app_metadata and user_metadata that have been
     * scoped to conform with OIDC (i.e., how newer Auth0 accounts structure the user profile) as well as the user_id ->
     * sub mapping.
     */
    private static void remapTokenValues(Map<String, Object> jwt) {
        // If token did not contain app_metadata or user_metadata, add the scoped values to the decoded token object.
        if (!jwt.containsKey(APP_METADATA) && jwt.containsKey(SCOPED_APP_METADATA)) {
            jwt.put(APP_METADATA, jwt.get(SCOPED_APP_METADATA));
        }
        if (!jwt.containsKey(USER_METADATA) && jwt.containsKey(SCOPED_USER_METADATA)) {
            jwt.put(USER_METADATA, jwt.get(SCOPED_USER_METADATA));
        }
        // Do the same for user_id -> sub
        if (!jwt.containsKey("user_id") && jwt.containsKey("sub")) {
            jwt.put("user_id", jwt.get("sub"));
        }
        // Remove scoped metadata objects to clean up user profile object.
        jwt.remove(SCOPED_APP_METADATA);
        jwt.remove(SCOPED_USER_METADATA);
    }

    /**
     * Check that the user has edit privileges for the feed ID specified. NOTE: the feed ID provided in the request will
     * represent a feed source, not a specific SQL namespace that corresponds to a feed version or specific set of GTFS
     * tables in the database.
     */
    public static void checkEditPrivileges(Request request) {
        if (isAuthDisabled() || inTestingEnvironment()) {
            // If in a development or testing environment, skip privileges check. This is done so that basically any API
            // endpoint can function.
            // TODO: make unit tests of the below items or do some more stuff as mentioned in PR review here:
            // https://github.com/conveyal/datatools-server/pull/187#discussion_r262714708
            return;
        }
        Auth0UserProfile userProfile = request.attribute("user");
        String feedId = request.queryParams("feedId");
        if (feedId == null) {
            // Some editor requests (e.g., update snapshot) specify the feedId as a parameters in the request (not a
            // query parameter).
            String[] parts = request.pathInfo().split("/");
            feedId = parts[parts.length - 1];
        }
        FeedSource feedSource = feedId != null ? Persistence.feedSources.getById(feedId) : null;
        if (feedSource == null) {
            LOG.warn("feedId {} not found", feedId);
            logMessageAndHalt(request, 400, "Must provide valid feedId parameter");
        }

        if (!request.requestMethod().equals("GET")) {
            if (!userProfile.canEditGTFS(feedSource)) {
                LOG.warn("User {} cannot edit GTFS for {}", userProfile.email, feedId);
                logMessageAndHalt(request, 403, "User does not have permission to edit GTFS for feedId");
            }
        }
    }

    /**
     * Check whether authentication has been disabled via the DISABLE_AUTH config variable.
     */
    public static boolean getDefaultAuthDisabled() {
        return DataManager.hasConfigProperty("DISABLE_AUTH") && "true".equals(getConfigPropertyAsText("DISABLE_AUTH"));
    }

    /**
     * Whether authentication is disabled.
     */
    public static boolean isAuthDisabled() {
        return authDisabled;
    }

    /**
     * Override the current {@link #authDisabled} value. This is used principally for setting up test environments that
     * require auth to be disabled.
     */
    public static void setAuthDisabled(boolean newAuthDisabled) {
        Auth0Connection.authDisabled = newAuthDisabled;
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
            logMessageAndHalt(request, 400, "Must provide valid feedId parameter");
        }

        if (!request.requestMethod().equals("GET")) {
            if (!userProfile.canEditGTFS(feedSource)) {
                LOG.warn("User {} cannot edit GTFS for {}", userProfile.email, feedId);
                logMessageAndHalt(request, 403, "User does not have permission to edit GTFS for feedId");
            }
        }
    }
}
