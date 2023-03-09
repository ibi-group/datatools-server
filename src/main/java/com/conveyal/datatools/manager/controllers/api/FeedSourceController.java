package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.common.utils.Scheduler;
import com.conveyal.datatools.common.utils.aws.CheckedAWSException;
import com.conveyal.datatools.common.utils.aws.S3Utils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Actions;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.extensions.ExternalFeedResource;
import com.conveyal.datatools.manager.jobs.FetchSingleFeedJob;
import com.conveyal.datatools.manager.jobs.NotifyUsersForSubscriptionJob;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedRetrievalMethod;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.models.transform.NormalizeFieldTransformation;
import com.conveyal.datatools.manager.models.transform.Substitution;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.JobUtils;
import com.conveyal.datatools.manager.utils.PersistenceUtils;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.conveyal.datatools.common.utils.SparkUtils.formatJobMessage;
import static com.conveyal.datatools.common.utils.SparkUtils.getPOJOFromRequestBody;
import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;
import static com.conveyal.datatools.manager.models.ExternalFeedSourceProperty.constructId;
import static com.conveyal.datatools.manager.models.transform.NormalizeFieldTransformation.getInvalidSubstitutionMessage;
import static com.conveyal.datatools.manager.models.transform.NormalizeFieldTransformation.getInvalidSubstitutionPatterns;
import static com.mongodb.client.model.Filters.in;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;
import static spark.Spark.put;

/**
 * Handlers for HTTP API requests that affect FeedSources.
 * These methods are mapped to API endpoints by Spark.
 */
public class FeedSourceController {
    private static final Logger LOG = LoggerFactory.getLogger(FeedSourceController.class);
    private static JsonManager<FeedSource> json = new JsonManager<>(FeedSource.class, JsonViews.UserInterface.class);
    private static ObjectMapper mapper = new ObjectMapper();

    /**
     * Spark HTTP endpoint to get a single feed source by ID.
     */
    private static FeedSource getFeedSource(Request req, Response res) {
        return requestFeedSourceById(req, Actions.VIEW);
    }

    /**
     * Spark HTTP endpoint that handles getting all feed sources for a handful of use cases:
     * - for a single project (if projectId query param provided)
     * - for the entire application
     */
    private static Collection<FeedSource> getProjectFeedSources(Request req, Response res) {
        Collection<FeedSource> feedSourcesToReturn = new ArrayList<>();
        Auth0UserProfile user = req.attribute("user");
        String projectId = req.queryParams("projectId");

        Project project = Persistence.projects.getById(projectId);

        if (project == null) {
            logMessageAndHalt(req, 400, "Must provide valid projectId query param to retrieve feed sources.");
        }
        boolean isAdmin = user.canAdministerProject(project);

        Collection<FeedSource> projectFeedSources = project.retrieveProjectFeedSources();
        for (FeedSource source: projectFeedSources) {
            String orgId = source.organizationId();
            // If user can view or manage feed, add to list of feeds to return. NOTE: By default most users with access
            // to a project should be able to view all feed sources. Custom privileges would need to be provided to
            // override this behavior.
            if (
                source.projectId != null && source.projectId.equals(projectId) &&
                    user.canManageOrViewFeed(orgId, source.projectId, source.id)
            ) {
                // Remove labels user can't view, then add to list of feeds to return
                feedSourcesToReturn.add(cleanFeedSourceForNonAdmins(source, isAdmin));
            }
        }
        return feedSourcesToReturn;
    }

    /**
     * HTTP endpoint to create a new feed source.
     */
    private static FeedSource createFeedSource(Request req, Response res) throws IOException {
        Auth0UserProfile userProfile = req.attribute("user");
        FeedSource newFeedSource = getPOJOFromRequestBody(req, FeedSource.class);
        validate(req, newFeedSource);
        boolean allowedToCreateFeedSource = userProfile.canAdministerProject(newFeedSource.projectId);
        if (!allowedToCreateFeedSource) {
            logMessageAndHalt(req, 403, "User not allowed to create feed source");
            return null;
        }
        // User checks out. OK to create new feed source.
        try {
            Persistence.feedSources.create(newFeedSource);
            // Communicate to any registered external "resources" (sites / databases) the fact that a feed source has been
            // created in our database.
            for (String resourceType : DataManager.feedResources.keySet()) {
                DataManager.feedResources.get(resourceType).feedSourceCreated(newFeedSource, req.headers("Authorization"));
            }
            // After successful save, handle auto fetch job setup.
            Scheduler.handleAutoFeedFetch(newFeedSource);
            // Notify project subscribers of new feed source creation.
            Project parentProject = Persistence.projects.getById(newFeedSource.projectId);
            NotifyUsersForSubscriptionJob.createNotification(
                "project-updated",
                newFeedSource.projectId,
                String.format("New feed %s created in project %s.", newFeedSource.name, parentProject.name)
            );
            return newFeedSource;
        } catch (Exception e) {
            logMessageAndHalt(req, 500, "Unknown error encountered creating feed source", e);
            return null;
        }
    }

    /**
     * Check that updated or new feedSource object is valid. This method should be called before a feedSource is
     * persisted to the database.
     * TODO: Determine if other checks ought to be applied here.
     */
    private static void validate(Request req, FeedSource feedSource) {
        List<String> validationIssues = new ArrayList<>();
        if (StringUtils.isEmpty(feedSource.name)) {
            validationIssues.add("Name field must not be empty.");
        }
        if (feedSource.retrieveProject() == null) {
            validationIssues.add("Valid project ID must be provided.");
        }
        if (Persistence.labels.getByIds(feedSource.labelIds).size() != feedSource.labelIds.size()) {
            validationIssues.add("All labels assigned to feed must exist.");
        }
        // Collect all retrieval methods found in transform rules into a list.
        List<FeedRetrievalMethod> retrievalMethods = feedSource.transformRules.stream()
            .map(rule -> rule.retrievalMethods)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
        Set<FeedRetrievalMethod> retrievalMethodSet = new HashSet<>(retrievalMethods);
        if (retrievalMethods.size() > retrievalMethodSet.size()) {
            // Explicitly check that the list of retrieval methods is not larger than the set (i.e., that there are no
            // duplicates).
            validationIssues.add("Retrieval methods cannot be defined more than once in transformation rules.");
        }
        // Validate transformations (currently this just checks that regex patterns are valid).
        List<Substitution> substitutions = feedSource.transformRules.stream()
            .map(rule -> rule.transformations)
            .flatMap(Collection::stream)
            .filter(t -> t instanceof NormalizeFieldTransformation)
            .map(t -> ((NormalizeFieldTransformation) t).substitutions)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
        List<String> invalidPatterns = getInvalidSubstitutionPatterns(substitutions);
        if (!invalidPatterns.isEmpty()) {
            validationIssues.add(getInvalidSubstitutionMessage(invalidPatterns));
        }
        if (!validationIssues.isEmpty()) {
            logMessageAndHalt(
                req,
                HttpStatus.BAD_REQUEST_400,
                "Request was invalid for the following reasons: " + String.join(", ", validationIssues)
            );
        }
    }

    /**
     * Spark HTTP endpoint to update a feed source. Note: at one point this endpoint accepted a JSON object
     * representing a single field to update for the feed source, but it now requires that the JSON body represent all
     * fields the updated feed source should contain. This change allows us to parse the JSON into a POJO, which
     * essentially does type checking for us and prevents issues with deserialization from MongoDB into POJOs.
     */
    private static FeedSource updateFeedSource(Request req, Response res) throws IOException, CheckedAWSException {
        String feedSourceId = req.params("id");
        FeedSource formerFeedSource = requestFeedSourceById(req, Actions.MANAGE);
        FeedSource updatedFeedSource = getPOJOFromRequestBody(req, FeedSource.class);
        validate(req, updatedFeedSource);
        // Feed source previously had a URL, but it has been changed. In this case, we reset the last fetched timestamp.
        if (formerFeedSource.url != null && !formerFeedSource.url.equals(updatedFeedSource.url)) {
            LOG.info("Feed source fetch URL has been modified. Resetting lastFetched value from {} to {}", formerFeedSource.lastFetched, null);
            updatedFeedSource.lastFetched = null;
        }
        Persistence.feedSources.replace(feedSourceId, updatedFeedSource);

        // If feed just changed from public to private, delete feed from public repo if it's present there.
        if (formerFeedSource.isPublic && !updatedFeedSource.isPublic) {
            LOG.info("Deleting {} as feed is being adjusted from public to private.", updatedFeedSource.toPublicKey());
            S3Utils.getDefaultS3Client().deleteObject(S3Utils.DEFAULT_BUCKET, updatedFeedSource.toPublicKey());
        }

        if (shouldNotifyUsersOnFeedUpdated(formerFeedSource, updatedFeedSource)) {
            return updatedFeedSource;
        }
        // After successful save, handle auto fetch job setup.
        Scheduler.handleAutoFeedFetch(updatedFeedSource);
        // Notify feed- and project-subscribed users after successful save
        NotifyUsersForSubscriptionJob.createNotification(
            "feed-updated",
            updatedFeedSource.id,
            String.format("Feed property updated for %s.", updatedFeedSource.name));
        NotifyUsersForSubscriptionJob.createNotification(
            "project-updated",
            updatedFeedSource.projectId,
            String.format("Project updated (feed source property changed for %s).", updatedFeedSource.name));
        return updatedFeedSource;
    }

    /**
     * Update a set of properties for an external feed resource. This updates the local copy of the properties in the
     * Mongo database and then triggers the {@link ExternalFeedResource#propertyUpdated} method to update the external
     * resource.
     *
     * FIXME: Should we reconsider how we store external feed source properties now that we are using Mongo document
     *   storage? This might should be refactored in the future, but it isn't really hurting anything at the moment.
     */
    private static FeedSource updateExternalFeedResource(Request req, Response res) {
        FeedSource source = requestFeedSourceById(req, Actions.MANAGE);
        String resourceType = req.queryParams("resourceType");
        JsonNode node = null;
        try {
            node = mapper.readTree(req.body());
        } catch (IOException e) {
            logMessageAndHalt(req, 400, "Unable to parse request body", e);
        }
        Iterator<Map.Entry<String, JsonNode>> fieldsIterator = node.fields();
        ExternalFeedResource externalFeedResource = DataManager.feedResources.get(resourceType);
        if (externalFeedResource == null) {
            logMessageAndHalt(req, 400, String.format("Resource '%s' not registered with server.", resourceType));
        }
        // Iterate over fields found in body and update external properties accordingly.
        while (fieldsIterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = fieldsIterator.next();
            String propertyId = constructId(source, resourceType, entry.getKey());
            ExternalFeedSourceProperty prop = Persistence.externalFeedSourceProperties.getById(propertyId);

            if (prop == null) {
                logMessageAndHalt(req, 400, String.format("Property '%s' does not exist!", propertyId));
                return null;
            }
            // Hold previous value for use when updating third-party resource
            String previousValue = prop.value;

            // Update the property with the value to be submitted.
            prop.value = entry.getValue().asText();

            // Trigger an event on the external resource.
            // After updating the external resource, we will update Mongo with values sent by the external resource.
            try {
                externalFeedResource.propertyUpdated(prop, previousValue, req.headers("Authorization"));
            } catch (IOException e) {
                logMessageAndHalt(req, 500, "Could not update external feed source", e);
            }
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
        FeedSource source = requestFeedSourceById(req, Actions.MANAGE);
        try {
            source.delete();
            return source;
        } catch (Exception e) {
            logMessageAndHalt(req, 500, "Unknown error occurred while deleting feed source.", e);
            return null;
        }
    }

    /**
     * Re-fetch this feed from the feed source URL.
     */
    private static String fetch (Request req, Response res) {
        FeedSource s = requestFeedSourceById(req, Actions.MANAGE);
        if (s.url == null) {
            logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Cannot fetch feed source with null URL.");
        }
        LOG.info("Fetching feed at {} for source {}", s.url, s.name);
        Auth0UserProfile userProfile = req.attribute("user");
        // Run in light executor, but if a new feed is found, do not continue thread (a new one will be started in
        // heavyExecutor in the body of the fetch job.
        FetchSingleFeedJob fetchSingleFeedJob = new FetchSingleFeedJob(s, userProfile, false);
        JobUtils.lightExecutor.execute(fetchSingleFeedJob);

        // Return the jobId so that the requester can track the job's progress.
        return formatJobMessage(fetchSingleFeedJob.jobId, "Fetching latest feed source.");
    }

    /**
     * Helper function returns feed source if user has permission for specified action.
     * @param req spark Request object from API request
     * @param action action type (either "view" or Permission.MANAGE)
     * @return feedsource object for ID
     */
    private static FeedSource requestFeedSourceById(Request req, Actions action) {
        String id = req.params("id");
        if (id == null) {
            logMessageAndHalt(req, 400, "Please specify id param");
        }

        return checkFeedSourcePermissions(req, Persistence.feedSources.getById(id), action);
    }

    public static FeedSource checkFeedSourcePermissions(Request req, FeedSource feedSource, Actions action) {
        Auth0UserProfile userProfile = req.attribute("user");
        // check for null feedSource
        if (feedSource == null) {
            logMessageAndHalt(req, 400, "Feed source ID does not exist");
            return null;
        }
        boolean isProjectAdmin = userProfile.canAdministerProject(feedSource);
        boolean authorized;

        switch (action) {
            case CREATE:
                authorized = isProjectAdmin;
                break;
            case MANAGE:
                authorized = userProfile.canManageFeed(feedSource);
                break;
            case EDIT:
                authorized = userProfile.canEditGTFS(feedSource);
                break;
            case VIEW:
                authorized = userProfile.canViewFeed(feedSource);
                break;
            default:
                authorized = false;
                break;
        }
        if (!authorized) {
            // Throw halt if user not authorized.
            logMessageAndHalt(req, 403, "User not authorized to perform action on feed source");
        }


        // If we make it here, user has permission and the requested feed source is valid.
        // This final step removes labels the user can't view
        return cleanFeedSourceForNonAdmins(feedSource, isProjectAdmin);
    }

    /** Determines whether a change to a feed source is significant enough that it warrants sending a notification
     *
     * @param formerFeedSource  A feed source object, without new changes
     * @param updatedFeedSource A feed source object, with new changes
     * @return                  A boolean value indicating if the updated feed source is changed enough to warrant sending a notification.
     */
    private static boolean shouldNotifyUsersOnFeedUpdated(FeedSource formerFeedSource, FeedSource updatedFeedSource) {
        return
                // If only labels have changed, don't send out an email
                formerFeedSource.equalsExceptLabels(updatedFeedSource);
    }

    /**
     * Removes labels and notes from a feed that a user is not allowed to view. Returns cleaned feed source.
     * @param feedSource    The feed source to clean
     * @param isAdmin       Is the user an admin? Changes what is returned.
     * @return              A feed source containing only labels/notes the user is allowed to see
     */
    protected static FeedSource cleanFeedSourceForNonAdmins(FeedSource feedSource, boolean isAdmin) {
        // Admin can view all feed labels, but a non-admin should only see those with adminOnly=false
        feedSource.labelIds = Persistence.labels
            .getFiltered(PersistenceUtils.applyAdminFilter(in("_id", feedSource.labelIds), isAdmin)).stream()
            .map(label -> label.id)
            .collect(Collectors.toList());
        feedSource.noteIds = Persistence.notes
            .getFiltered(PersistenceUtils.applyAdminFilter(in("_id", feedSource.noteIds), isAdmin)).stream()
            .map(note -> note.id)
            .collect(Collectors.toList());
        return feedSource;
    }

    // FIXME: use generic API controller and return JSON documents via BSON/Mongo
    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/feedsource/:id", FeedSourceController::getFeedSource, json::write);
        get(apiPrefix + "secure/feedsource", FeedSourceController::getProjectFeedSources, json::write);
        post(apiPrefix + "secure/feedsource", FeedSourceController::createFeedSource, json::write);
        put(apiPrefix + "secure/feedsource/:id", FeedSourceController::updateFeedSource, json::write);
        put(apiPrefix + "secure/feedsource/:id/updateExternal", FeedSourceController::updateExternalFeedResource, json::write);
        delete(apiPrefix + "secure/feedsource/:id", FeedSourceController::deleteFeedSource, json::write);
        post(apiPrefix + "secure/feedsource/:id/fetch", FeedSourceController::fetch, json::write);
    }
}
