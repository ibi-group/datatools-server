package com.conveyal.datatools.manager.auth;

import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.internal.org.apache.commons.codec.binary.Base64;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;

import java.util.Map;

import static com.conveyal.datatools.common.utils.SparkUtils.haltWithMessage;
import static com.conveyal.datatools.manager.DataManager.getConfigPropertyAsText;

/**
 * Created by demory on 3/22/16.
 */

public class Auth0Connection {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(Auth0Connection.class);

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
            haltWithMessage(req, 401, "Authorization header is missing.");
        }
        String[] parts = authHeader.split(" ");
        if (parts.length != 2 || !"bearer".equals(parts[0].toLowerCase())) {
            haltWithMessage(req, 401, String.format("Authorization header is malformed: %s", authHeader));
        }
        // Retrieve token from auth header.
        String token = parts[1];
        if (token == null) {
            haltWithMessage(req, 401, "Could not find authorization token");
        }
        // Validate the JWT and cast into the user profile, which will be attached as an attribute on the request object
        // for downstream controllers to check permissions.
        try {
            byte[] decodedSecret = new Base64().decode(getConfigPropertyAsText("AUTH0_SECRET"));
            JWTVerifier verifier = new JWTVerifier(decodedSecret);
            Map<String, Object> jwt = verifier.verify(token);
            Auth0UserProfile profile = MAPPER.convertValue(jwt, Auth0UserProfile.class);
            req.attribute("user", profile);
        } catch (Exception e) {
            LOG.warn("Login failed to verify with our authorization provider.", e);
            haltWithMessage(req, 401, "Could not verify user's token");
        }
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
            haltWithMessage(request, 400, "Must provide valid feedId parameter");
        }

        if (!request.requestMethod().equals("GET")) {
            if (!userProfile.canEditGTFS(feedSource.organizationId(), feedSource.projectId, feedSource.id)) {
                LOG.warn("User {} cannot edit GTFS for {}", userProfile.email, feedId);
                haltWithMessage(request, 403, "User does not have permission to edit GTFS for feedId");
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
            haltWithMessage(request, 400, "Must provide valid feedId parameter");
        }

        if (!request.requestMethod().equals("GET")) {
            if (!userProfile.canEditGTFS(feedSource.organizationId(), feedSource.projectId, feedSource.id)) {
                LOG.warn("User {} cannot edit GTFS for {}", userProfile.email, feedId);
                haltWithMessage(request, 403, "User does not have permission to edit GTFS for feedId");
            }
        }
    }
}
