package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.extensions.ExternalFeedResource;
import com.conveyal.datatools.manager.jobs.FetchSingleFeedJob;
import com.conveyal.datatools.manager.jobs.NotifyUsersForSubscriptionJob;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static com.conveyal.datatools.common.utils.SparkUtils.formatJobMessage;
import static com.conveyal.datatools.common.utils.SparkUtils.haltWithMessage;
import static com.conveyal.datatools.manager.auth.Auth0Users.getUserById;
import static com.conveyal.datatools.manager.models.ExternalFeedSourceProperty.constructId;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;
import static spark.Spark.put;

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
        Collection<FeedSource> feedSourcesToReturn = new ArrayList<>();
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
                    feedSourcesToReturn.add(source);
                }
            }
        } else if (userId != null) {
            // request feed sources a specified user has permissions for
            Auth0UserProfile user = getUserById(userId);
            if (user == null) return feedSourcesToReturn;

            for (FeedSource source: Persistence.feedSources.getAll()) {
                String orgId = source.organizationId();
                if (
                    source != null && source.projectId != null &&
                    (user.canManageFeed(orgId, source.projectId, source.id) || user.canViewFeed(orgId, source.projectId, source.id))
                ) {

                    feedSourcesToReturn.add(source);
                }
            }
        } else {
            // request feed sources that are public
            for (FeedSource source: Persistence.feedSources.getAll()) {
                String orgId = source.organizationId();
                // if user is logged in and cannot view feed; skip source
                if ((requestingUser != null && !requestingUser.canManageFeed(orgId, source.projectId, source.id) && !requestingUser.canViewFeed(orgId, source.projectId, source.id)))
                    continue;

                // if requesting public sources and source is not public; skip source
                if (publicFilter && !source.isPublic)
                    continue;
                feedSourcesToReturn.add(source);
            }
        }

        return feedSourcesToReturn;
    }

    /**
     * HTTP endpoint to create a new feed source.
     */
    public static FeedSource createFeedSource(Request req, Response res) {
        // TODO factor out getting user profile, project ID and organization ID and permissions
        Auth0UserProfile userProfile = req.attribute("user");
        Document newFeedSourceFields = Document.parse(req.body());
        String projectId = newFeedSourceFields.getString("projectId");
        String organizationId = newFeedSourceFields.getString("organizationId");
        boolean allowedToCreateFeedSource = userProfile.canAdministerProject(projectId, organizationId);
        if (allowedToCreateFeedSource) {
            try {
                FeedSource newFeedSource = Persistence.feedSources.create(req.body());
                // Communicate to any registered external "resources" (sites / databases) the fact that a feed source has been
                // created in our database.
                for (String resourceType : DataManager.feedResources.keySet()) {
                    DataManager.feedResources.get(resourceType).feedSourceCreated(newFeedSource, req.headers("Authorization"));
                }
                // Notify project subscribers of new feed source creation.
                Project parentProject = Persistence.projects.getById(projectId);
                NotifyUsersForSubscriptionJob.createNotification(
                        "project-updated",
                        projectId,
                        String.format("New feed %s created in project %s.", newFeedSource.name, parentProject.name));
                return newFeedSource;
            } catch (Exception e) {
                LOG.error("Unknown error creating feed source", e);
                haltWithMessage(req, 400, "Unknown error encountered creating feed source", e);
                return null;
            }
        } else {
            haltWithMessage(req, 400, "Must provide project ID for feed source");
            return null;
        }
    }

    public static FeedSource updateFeedSource(Request req, Response res) {
        String feedSourceId = req.params("id");

        // call this method just for null and permissions check
        // TODO: it's wasteful to request the entire feed source here, need to factor out permissions checks. However,
        // we need the URL to see if it has been updated in order to then set the lastFetched value to null.
        FeedSource formerFeedSource = requestFeedSourceById(req, "manage");
        Document fieldsToUpdate = Document.parse(req.body());
        if (fieldsToUpdate.containsKey("url") && formerFeedSource.url != null) {
            // Reset last fetched timestamp if the URL has been updated.
            if (!fieldsToUpdate.get("url").toString().equals(formerFeedSource.url.toString())) {
                LOG.info("Feed source fetch URL has been modified. Resetting lastFetched value from {} to {}", formerFeedSource.lastFetched, null);
                fieldsToUpdate.put("lastFetched", null);
            }
        }
        FeedSource source = Persistence.feedSources.update(feedSourceId, fieldsToUpdate.toJson());

        // Notify feed- and project-subscribed users after successful save
        NotifyUsersForSubscriptionJob.createNotification(
                "feed-updated",
                source.id,
                String.format("Feed property updated for %s.", source.name));
        NotifyUsersForSubscriptionJob.createNotification(
                "project-updated",
                source.projectId,
                String.format("Project updated (feed source property changed for %s).", source.name));
        return source;
    }

    /**
     * Update a set of properties for an external feed resource. This updates the local copy of the properties in the
     * Mongo database and then triggers the {@link ExternalFeedResource#propertyUpdated} method to update the external
     * resource.
     *
     * FIXME: Should we reconsider how we store external feed source properties now that we are using Mongo document
     * storage? This might should be refactored in the future, but it isn't really hurting anything at the moment.
     */
    public static FeedSource updateExternalFeedResource(Request req, Response res) throws IOException {
        FeedSource source = requestFeedSourceById(req, "manage");
        String resourceType = req.queryParams("resourceType");
        JsonNode node = mapper.readTree(req.body());
        Iterator<Map.Entry<String, JsonNode>> fieldsIterator = node.fields();
        ExternalFeedResource externalFeedResource = DataManager.feedResources.get(resourceType);
        if (externalFeedResource == null) {
            haltWithMessage(req, 400, String.format("Resource '%s' not registered with server.", resourceType));
        }
        // Iterate over fields found in body and update external properties accordingly.
        while (fieldsIterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = fieldsIterator.next();
            String propertyId = constructId(source, resourceType, entry.getKey());
            ExternalFeedSourceProperty prop = Persistence.externalFeedSourceProperties.getById(propertyId);

            if (prop == null) {
                haltWithMessage(req, 400, String.format("Property '%s' does not exist!", propertyId));
            }
            // Hold previous value for use when updating third-party resource
            String previousValue = prop.value;
            // Update the property in our database.
            ExternalFeedSourceProperty updatedProp = Persistence.externalFeedSourceProperties.updateField(
                    propertyId, "value", entry.getValue().asText());

            // Trigger an event on the external resource
            externalFeedResource.propertyUpdated(updatedProp, previousValue, req.headers("Authorization"));
        }
        // Updated external properties will be included in JSON (FeedSource#externalProperties)
        return source;
    }

    /**
     * HTTP endpoint to delete a feed source.
     *
     * FIXME: Should this just set a "deleted" flag instead of removing from the database entirely?
     */
    private static FeedSource deleteFeedSource(Request req, Response res) {
        FeedSource source = requestFeedSourceById(req, "manage");

        try {
            source.delete();
            return source;
        } catch (Exception e) {
            LOG.error("Could not delete feed source", e);
            haltWithMessage(req, 400, "Unknown error deleting feed source.");
            return null;
        }
    }

    /**
     * Re-fetch this feed from the feed source URL.
     */
    public static String fetch (Request req, Response res) {
        FeedSource s = requestFeedSourceById(req, "manage");

        LOG.info("Fetching feed for source {}", s.name);

        Auth0UserProfile userProfile = req.attribute("user");
        // Run in heavyExecutor because ProcessSingleFeedJob is chained to this job (if update finds new version).
        FetchSingleFeedJob fetchSingleFeedJob = new FetchSingleFeedJob(s, userProfile.getUser_id(), false);
        DataManager.lightExecutor.execute(fetchSingleFeedJob);

        // Return the jobId so that the requester can track the job's progress.
        return formatJobMessage(fetchSingleFeedJob.jobId, "Fetching latest feed source.");
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
            haltWithMessage(req, 400, "Please specify id param");
        }
        return checkFeedSourcePermissions(req, Persistence.feedSources.getById(id), action);
    }

    public static FeedSource checkFeedSourcePermissions(Request req, FeedSource feedSource, String action) {
        Auth0UserProfile userProfile = req.attribute("user");
        Boolean publicFilter = Boolean.valueOf(req.queryParams("public")) ||
                req.url().split("/api/*/")[1].startsWith("public");

        // check for null feedSource
        if (feedSource == null)
            haltWithMessage(req, 400, "Feed source ID does not exist");
        String orgId = feedSource.organizationId();
        boolean authorized;
        switch (action) {
            case "create":
                authorized = userProfile.canAdministerProject(feedSource.projectId, orgId);
                break;
            case "manage":
                authorized = userProfile.canManageFeed(orgId, feedSource.projectId, feedSource.id);
                break;
            case "edit":
                authorized = userProfile.canEditGTFS(orgId, feedSource.projectId, feedSource.id);
                break;
            case "view":
                if (!publicFilter) {
                    authorized = userProfile.canViewFeed(orgId, feedSource.projectId, feedSource.id);
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
            if (!feedSource.isPublic && !authorized)
                haltWithMessage(req, 403, "User not authorized to perform action on feed source");
                // if feed is public, but action is managerial, halt (we shouldn't ever retrieveById here, but just in case)
            else if (feedSource.isPublic && action.equals("manage"))
                haltWithMessage(req, 403, "User not authorized to perform action on feed source");

        }
        else {
            if (!authorized)
                haltWithMessage(req, 403, "User not authorized to perform action on feed source");
        }

        // if we make it here, user has permission and it's a valid feedsource
        return feedSource;
    }

    // FIXME: use generic API controller and return JSON documents via BSON/Mongo
    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/feedsource/:id", FeedSourceController::getFeedSource, json::write);
        get(apiPrefix + "secure/feedsource", FeedSourceController::getAllFeedSources, json::write);
        post(apiPrefix + "secure/feedsource", FeedSourceController::createFeedSource, json::write);
        put(apiPrefix + "secure/feedsource/:id", FeedSourceController::updateFeedSource, json::write);
        put(apiPrefix + "secure/feedsource/:id/updateExternal", FeedSourceController::updateExternalFeedResource, json::write);
        delete(apiPrefix + "secure/feedsource/:id", FeedSourceController::deleteFeedSource, json::write);
        post(apiPrefix + "secure/feedsource/:id/fetch", FeedSourceController::fetch, json::write);

        // Public routes
        get(apiPrefix + "public/feedsource/:id", FeedSourceController::getFeedSource, json::write);
        get(apiPrefix + "public/feedsource", FeedSourceController::getAllFeedSources, json::write);
    }
}
