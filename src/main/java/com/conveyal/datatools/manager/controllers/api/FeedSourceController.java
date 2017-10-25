package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.FetchSingleFeedJob;
import com.conveyal.datatools.manager.jobs.NotifyUsersForSubscriptionJob;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.*;

import static com.conveyal.datatools.common.utils.SparkUtils.haltWithError;
import static com.conveyal.datatools.manager.auth.Auth0Users.getUserById;
import static com.conveyal.datatools.manager.models.ExternalFeedSourceProperty.constructId;
import static com.mongodb.client.model.Filters.eq;
import static spark.Spark.*;

/**
 * Handlers for HTTP API requests that affect FeedSources.
 * These methods are mapped to API endpoints by Spark.
 */
public class FeedSourceController {
    public static final Logger LOG = LoggerFactory.getLogger(FeedSourceController.class);
    public static JsonManager<FeedSource> json =
            new JsonManager<>(FeedSource.class, JsonViews.UserInterface.class);
    private static ObjectMapper mapper = new ObjectMapper();

    public static FeedSource getFeedSource(Request req, Response res) {
        return requestFeedSourceById(req, "view");
    }

    public static Collection<FeedSource> getAllFeedSources(Request req, Response res) {
        Collection<FeedSource> sources = new ArrayList<>();
        Auth0UserProfile requestingUser = req.attribute("user");
        String projectId = req.queryParams("projectId");
        Boolean publicFilter = req.pathInfo().contains("public");
        String userId = req.queryParams("userId");

        if (projectId != null) {
            for (FeedSource source: Persistence.feedSources.getAll()) {
                String orgId = source.organizationId();
                if (
                    source != null && source.projectId != null && source.projectId.equals(projectId)
                    && requestingUser != null && (requestingUser.canManageFeed(orgId, source.projectId, source.id) || requestingUser.canViewFeed(orgId, source.projectId, source.id))
                ) {
                    // if requesting public sources and source is not public; skip source
                    if (publicFilter && !source.isPublic)
                        continue;
                    sources.add(source);
                }
            }
        }
        // request feed sources a specified user has permissions for
        else if (userId != null) {
            Auth0UserProfile user = getUserById(userId);
            if (user == null) return sources;

            for (FeedSource source: Persistence.feedSources.getAll()) {
                String orgId = source.organizationId();
                if (
                    source != null && source.projectId != null &&
                    (user.canManageFeed(orgId, source.projectId, source.id) || user.canViewFeed(orgId, source.projectId, source.id))
                ) {

                    sources.add(source);
                }
            }
        }
        // request feed sources that are public
        else {
            for (FeedSource source: Persistence.feedSources.getAll()) {
                String orgId = source.organizationId();
                // if user is logged in and cannot view feed; skip source
                if ((requestingUser != null && !requestingUser.canManageFeed(orgId, source.projectId, source.id) && !requestingUser.canViewFeed(orgId, source.projectId, source.id)))
                    continue;

                // if requesting public sources and source is not public; skip source
                if (publicFilter && !source.isPublic)
                    continue;
                sources.add(source);
            }
        }

        return sources;
    }

    public static FeedSource createFeedSource(Request req, Response res) throws IOException {
        // TODO factor out getting user profile, project ID and organization ID and permissions
        Auth0UserProfile userProfile = req.attribute("user");
        Document newFeedSourceFields = Document.parse(req.body());
        String projectId = newFeedSourceFields.getString("projectId");
        String organizationId = newFeedSourceFields.getString("organizationId");
        boolean allowedToCreateFeedSource = userProfile.canAdministerProject(projectId, organizationId);
        if (allowedToCreateFeedSource) {
            FeedSource newFeedSource = Persistence.feedSources.create(req.body());
            // Communicate to any registered external "resources" (sites / databases) the fact that a feed source has been
            // created in our database.
            for (String resourceType : DataManager.feedResources.keySet()) {
                DataManager.feedResources.get(resourceType).feedSourceCreated(newFeedSource, req.headers("Authorization"));
            }
            return newFeedSource;
        } else {
            haltWithError(400, "Must provide project ID for feed source");
            return null;
        }
    }

    public static FeedSource updateFeedSource(Request req, Response res) throws IOException {
        String feedSourceId = req.params("id");

        // call this method just for null and permissions check
        // TODO: it's wasteful to request the entire feed source here, need to factor out permissions checks
        requestFeedSourceById(req, "manage");

        FeedSource source = Persistence.feedSources.update(feedSourceId, req.body());

        // notify users after successful save
        NotifyUsersForSubscriptionJob notifyFeedJob = new NotifyUsersForSubscriptionJob("feed-updated", source.id, "Feed property updated for " + source.name);
        DataManager.lightExecutor.execute(notifyFeedJob);

        NotifyUsersForSubscriptionJob notifyProjectJob = new NotifyUsersForSubscriptionJob("project-updated", source.projectId, "Project updated (feed source property for " + source.name + ")");
        DataManager.lightExecutor.execute(notifyProjectJob);

        return source;
    }

    /**
     * FIXME: We should reconsider how we store external feed source properties now that we are using Mongo document
     * storage
     */
    public static FeedSource updateExternalFeedResource(Request req, Response res) throws IOException {
        FeedSource source = requestFeedSourceById(req, "manage");
        String resourceType = req.queryParams("resourceType");
        JsonNode node = mapper.readTree(req.body());
        Iterator<Map.Entry<String, JsonNode>> fieldsIter = node.fields();

        while (fieldsIter.hasNext()) {
            Map.Entry<String, JsonNode> entry = fieldsIter.next();
            ExternalFeedSourceProperty prop =
                    Persistence.externalFeedSourceProperties.getById(constructId(source, resourceType, entry.getKey()));

            if (prop != null) {
                // update the property in our DB
                String previousValue = prop.value;
                prop.value = entry.getValue().asText();
                // FIXME: add back storage of external feed source properties.
//                prop.save();

                // trigger an event on the external resource
                if(DataManager.feedResources.containsKey(resourceType)) {
                    DataManager.feedResources.get(resourceType).propertyUpdated(prop, previousValue, req.headers("Authorization"));
                }

            }

        }

        return source;
    }

    public static FeedSource deleteFeedSource(Request req, Response res) {
        FeedSource source = requestFeedSourceById(req, "manage");

        try {
            Persistence.feedSources.removeById(source.id);
            return source;
        } catch (Exception e) {
            e.printStackTrace();
            halt(400, "Unknown error deleting feed source.");
            return null;
        }
    }

    /**
     * Refetch this feed
     * @throws JsonProcessingException
     */
    public static boolean fetch (Request req, Response res) throws JsonProcessingException {
        FeedSource s = requestFeedSourceById(req, "manage");

        LOG.info("Fetching feed for source {}", s.name);

        Auth0UserProfile userProfile = req.attribute("user");
        // Run in heavyExecutor because ProcessSingleFeedJob is chained to this job (if update finds new version).
        FetchSingleFeedJob job = new FetchSingleFeedJob(s, userProfile.getUser_id(), false);
        DataManager.lightExecutor.execute(job);

        // WARNING: infinite 2D bounds Jackson error when returning job.result, so this method now returns true
        // because we don't need to return the feed immediately anyways.
        // return job.result;

        return true;
    }

    /**
     * Helper function returns feed source if user has permission for specified action.
     * @param req spark Request object from API request
     * @param action action type (either "view" or "manage")
     * @return feedsource object for ID
     */
    public static FeedSource requestFeedSourceById(Request req, String action) {
        String id = req.params("id");
        if (id == null) {
            halt(400, SparkUtils.formatJSON("Please specify id param", 400));
        }
        return checkFeedSourcePermissions(req, Persistence.feedSources.getById(id), action);
    }

    public static FeedSource checkFeedSourcePermissions(Request req, FeedSource s, String action) {
        Auth0UserProfile userProfile = req.attribute("user");
        Boolean publicFilter = Boolean.valueOf(req.queryParams("public")) ||
                req.url().split("/api/*/")[1].startsWith("public");
//        System.out.println(req.url().split("/api/manager/")[1].startsWith("public"));

        // check for null feedSource
        if (s == null)
            halt(400, SparkUtils.formatJSON("Feed source ID does not exist", 400));
        String orgId = s.organizationId();
        boolean authorized;
        switch (action) {
            case "create":
                authorized = userProfile.canAdministerProject(s.projectId, orgId);
                break;
            case "manage":
                authorized = userProfile.canManageFeed(orgId, s.projectId, s.id);
                break;
            case "edit":
                authorized = userProfile.canEditGTFS(orgId, s.projectId, s.id);
                break;
            case "view":
                if (!publicFilter) {
                    authorized = userProfile.canViewFeed(orgId, s.projectId, s.id);
                } else {
                    authorized = false;
                }
                break;
            default:
                authorized = false;
                break;
        }

        // if requesting public sources
        if (publicFilter){
            // if feed not public and user not authorized, halt
            if (!s.isPublic && !authorized)
                halt(403, SparkUtils.formatJSON("User not authorized to perform action on feed source", 403));
                // if feed is public, but action is managerial, halt (we shouldn't ever retrieveById here, but just in case)
            else if (s.isPublic && action.equals("manage"))
                halt(403, SparkUtils.formatJSON("User not authorized to perform action on feed source", 403));

        }
        else {
            if (!authorized)
                halt(403, SparkUtils.formatJSON("User not authorized to perform action on feed source", 403));
        }

        // if we make it here, user has permission and it's a valid feedsource
        return s;
    }

    // FIXME: use generic API controller and return JSON documents via BSON/Mongo
    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/feedsource/:id", FeedSourceController::getFeedSource, json::write);
        get(apiPrefix + "secure/feedsource", FeedSourceController::getAllFeedSources, json::write);
        post(apiPrefix + "secure/feedsource", FeedSourceController::createFeedSource, json::write);
        put(apiPrefix + "secure/feedsource/:id", FeedSourceController::updateFeedSource, json::write);
        put(apiPrefix + "secure/feedsource/:id/updateExternal", FeedSourceController::updateExternalFeedResource, json::write);
        delete(apiPrefix + "secure/feedsource/:id", FeedSourceController::deleteFeedSource, json::write);
        post(apiPrefix + "secure/feedsource/:id/fetch", FeedSourceController::fetch, JsonUtil.objectMapper::writeValueAsString);

        // Public routes
        get(apiPrefix + "public/feedsource/:id", FeedSourceController::getFeedSource, json::write);
        get(apiPrefix + "public/feedsource", FeedSourceController::getAllFeedSources, json::write);
    }
}