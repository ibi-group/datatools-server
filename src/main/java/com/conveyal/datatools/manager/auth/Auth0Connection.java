package com.conveyal.datatools.manager.auth;

import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.JWTVerifyException;
import com.auth0.jwt.pem.PemReader;
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

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static com.conveyal.datatools.common.utils.SparkUtils.haltWithMessage;
import static com.conveyal.datatools.manager.DataManager.getConfigPropertyAsText;
import static com.conveyal.datatools.manager.DataManager.hasConfigProperty;
import static spark.Spark.halt;

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
    private static final String BASE_URL = getConfigPropertyAsText("application.public_url");
    private static final int DEFAULT_LINES_TO_PRINT = 10;
    private static JWTVerifier verifier;

    /**
     * Check the incoming API request for the user token (and verify it) and assign as the "user" attribute on the
     * incoming request object for use in downstream controllers.
     * @param req Spark request object
     */
    public static void checkUser(Request req) {
        if (authDisabled()) {
            // If in a development environment, assign a mock profile to request attribute and skip authentication.
            req.attribute("user", new Auth0UserProfile("mock@example.com", "user_id:string"));
            return;
        }
        // Check that auth header is present and formatted correctly (Authorization: Bearer [token]).
        final String authHeader = req.headers("Authorization");
        if (authHeader == null) {
            haltWithMessage(401, "Authorization header is missing.");
        }
        String[] parts = authHeader.split(" ");
        if (parts.length != 2 || !"bearer".equals(parts[0].toLowerCase())) {
            haltWithMessage(401, String.format("Authorization header is malformed: %s", authHeader));
        }
        // Retrieve token from auth header.
        String token = parts[1];
        if (token == null) {
            haltWithMessage(401, "Could not find authorization token");
        }
        // Validate the JWT and cast into the user profile, which will be attached as an attribute on the request object
        // for downstream controllers to check permissions.
        try {
            Map<String, Object> jwt = verifyToken(token);
            remapTokenValues(jwt);
            Auth0UserProfile profile = MAPPER.convertValue(jwt, Auth0UserProfile.class);
            // The user attribute is used on the server side to check user permissions and does not have all of the
            // fields that the raw Auth0 profile string does.
            req.attribute("user", profile);
            // The raw_user attribute is used to return the complete user profile string to the client upon request in
            // the UserController. Previously the client sought the profile directly from Auth0 but the need to add
            req.attribute("raw_user", MAPPER.writeValueAsString(jwt));
        } catch (Exception e) {
            LOG.warn("Login failed to verify with our authorization provider.", e);
            haltWithMessage(401, "Could not verify user's token");
        }
    }

    /**
     * Choose the correct JWT verification algorithm (based on the values present in env.yml config) and verify the JWT
     * token.
     */
    private static Map<String, Object> verifyToken(String token) throws NoSuchAlgorithmException, NoSuchProviderException, InvalidKeySpecException, IOException, JWTVerifyException, InvalidKeyException, SignatureException {
        if (verifier == null) {
            if (hasConfigProperty("AUTH0_SECRET")) {
                // Use HS256 algorithm to verify token (uses client secret).
                byte[] decodedSecret = new org.apache.commons.codec.binary.Base64().decode(getConfigPropertyAsText("AUTH0_SECRET"));
                verifier = new JWTVerifier(decodedSecret);
            } else if (hasConfigProperty("AUTH0_PUBLIC_KEY")) {
                // Use RS256 algorithm to verify token (uses public key/.pem file).
                PublicKey publicKey = PemReader.readPublicKey(getConfigPropertyAsText("AUTH0_PUBLIC_KEY"));
                verifier = new JWTVerifier(publicKey);
            } else {
                throw new RuntimeException("Server authentication provider not configured correctly.");
            }
        }
        return verifier.verify(token);
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
        if (authDisabled()) {
            // If in a development environment, skip privileges check.
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
            haltWithMessage(400, "Must provide valid feedId parameter");
        }

        if (!request.requestMethod().equals("GET")) {
            if (!userProfile.canEditGTFS(feedSource.organizationId(), feedSource.projectId, feedSource.id)) {
                LOG.warn("User {} cannot edit GTFS for {}", userProfile.email, feedId);
                haltWithMessage(403, "User does not have permission to edit GTFS for feedId");
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
     * Log Spark requests.
     */
    public static void logRequest(Request request, Response response) {
        logRequestOrResponse(true, request, response);
    }

    /**
     * Log Spark responses.
     */
    public static void logResponse(Request request, Response response) {
        logRequestOrResponse(false, request, response);
    }

    /**
     * Log request/response.  Pretty print JSON if the content-type is JSON.
     */
    public static void logRequestOrResponse(boolean logRequest, Request request, Response response) {
        Auth0UserProfile userProfile = request.attribute("user");
        String userEmail = userProfile != null ? userProfile.email : "no-auth";
        HttpServletResponse raw = response.raw();
        // NOTE: Do not attempt to read the body into a string until it has been determined that the content-type is
        // JSON.
        String bodyString = "";
        try {
            String contentType;
            if (logRequest) {
                contentType = request.contentType();
            } else {
                contentType = raw.getHeader("content-type");
            }
            if ("application/json".equals(contentType)) {
                bodyString = logRequest ? request.body() : response.body();
                if (bodyString != null) {
                    // Pretty print JSON if ContentType is JSON and body is not empty
                    JsonNode jsonNode = MAPPER.readTree(bodyString);
                    // Add new line for legibility when printing
                    bodyString = "\n" + MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(jsonNode);
                } else {
                    bodyString = "{body content is null}";
                }
            } else if (contentType != null) {
                bodyString = String.format("\nnon-JSON body type: %s", contentType);
            }
        } catch (IOException e) {
            LOG.warn("Could not parse JSON", e);
            bodyString = "\nBad JSON:\n" + bodyString;
        }

        String queryString = request.queryParams().size() > 0 ? "?" + request.queryString() : "";
        LOG.info(
            "{} {} {}: {}{}{}{}",
            logRequest ? "req" : String.format("res (%s)", raw.getStatus()),
            userEmail,
            request.requestMethod(),
            BASE_URL,
            request.pathInfo(),
            queryString,
            trimLines(bodyString)
        );
    }

    private static String trimLines(String str) {
        if (str == null) return "";
        String[] lines = str.split("\n");
        boolean linesExceedLimit = lines.length > DEFAULT_LINES_TO_PRINT;
        // Gather lines to print in smaller array (so that filter below only has to be applied to a few lines).
        String[] linesToPrint = linesExceedLimit
            ? Arrays.copyOfRange(lines, 0, DEFAULT_LINES_TO_PRINT - 1)
            : lines;
        String stringToPrint = Arrays.stream(linesToPrint)
            // Filter out any password found in JSON (e.g., when creating an Auth0 user), so that sensitive information
            // is not logged. NOTE: this is a pretty clumsy match, but it is probably better to err on the side of caution.
            .map(line -> line.contains("password") ? "  \"password\": \"xxxxxx\", (value filtered by logger)" : line)
            .collect(Collectors.joining("\n"));
        return linesExceedLimit
            ? String.format("%s \n...and %d more lines", stringToPrint, lines.length - DEFAULT_LINES_TO_PRINT)
            : stringToPrint;
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
