package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.common.utils.S3Utils;
import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.editor.controllers.EditorLockController;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.conveyal.gtfs.loader.JdbcTableWriter;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.Entity;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;

import static com.conveyal.datatools.common.utils.SparkUtils.formatJSON;
import static com.conveyal.datatools.common.utils.SparkUtils.haltWithMessage;
import static com.conveyal.datatools.editor.controllers.EditorLockController.sessionsForFeedIds;
import static spark.Spark.*;
import static spark.Spark.delete;
import static spark.Spark.post;

/**
 * Abstract controller that sets HTTP endpoints for editing GTFS entities. This class contains methods that can be
 * overridden that serve as hooks to perform custom logic on
 */
public abstract class EditorController<T extends Entity> {
    private static final String ID_PARAM = "/:id";
    private final String ROOT_ROUTE;
    private static final String SECURE = "secure/";
    private static final Logger LOG = LoggerFactory.getLogger(EditorController.class);
    private DataSource datasource;
    private final String classToLowercase;
    private static final ObjectMapper mapper = new ObjectMapper();
    public static final JsonManager<Entity> json = new JsonManager<>(Entity.class, JsonViews.UserInterface.class);
    private final Table table;

    EditorController(String apiPrefix, Table table, DataSource datasource) {
        this.table = table;
        this.datasource = datasource;
        this.classToLowercase = table.getEntityClass().getSimpleName().toLowerCase();
        this.ROOT_ROUTE = apiPrefix + SECURE + classToLowercase;
        registerRoutes();
    }

    /**
     * Add static HTTP endpoints to Spark static instance.
     */
    private void registerRoutes() {
        LOG.info("Registering editor routes for {}", ROOT_ROUTE);
        // Note, get single and multiple entity methods are handled by GraphQLGtfsSchema class. Only create, update, and
        // delete HTTP endpoints are handled as REST. TODO: migrate these REST endpoints to GraphQL mutations.
        // Options response for CORS
        options(ROOT_ROUTE, (q, s) -> "");
        // Create entity request
        post(ROOT_ROUTE, this::createOrUpdate, json::write);
        // Update entity request
        put(ROOT_ROUTE + ID_PARAM, this::createOrUpdate, json::write);
        // Handle uploading agency and route branding to s3
        // TODO: Merge as a hook into createOrUpdate?
        if ("agency".equals(classToLowercase) || "route".equals(classToLowercase)) {
            post(ROOT_ROUTE + ID_PARAM + "/uploadbranding", this::uploadEntityBranding, json::write);
        }
        // Delete entity request
        delete(ROOT_ROUTE + ID_PARAM, this::deleteOne, json::write);

        // Handle special multiple delete method for trip endpoint
        if ("trip".equals(classToLowercase)) {
            delete(ROOT_ROUTE, this::deleteMultipleTrips, json::write);
        }

        // Handle update useFrequency field. Hitting this endpoint will delete all trips for a pattern and update the
        // useFrequency field.
        if ("pattern".equals(classToLowercase)) {
            delete(ROOT_ROUTE + ID_PARAM + "/trips", this::deleteTripsForPattern, json::write);
        }
    }

    /**
     * HTTP endpoint to delete all trips for a given string pattern_id (i.e., not the integer ID field).
     */
    private String deleteTripsForPattern(Request req, Response res) {
        String namespace = getNamespaceAndValidateSession(req);
        // NOTE: This is a string pattern ID, not the integer ID that all other HTTP endpoints use.
        String patternId = req.params("id");
        if (patternId == null) {
            haltWithMessage(400, "Must provide valid pattern_id");
        }
        JdbcTableWriter tableWriter = new JdbcTableWriter(Table.TRIPS, datasource, namespace);
        try {
            int deletedCount = tableWriter.deleteWhere("pattern_id", patternId, true);
            return formatJSON(String.format("Deleted %d.", deletedCount), 200);
        } catch (SQLException e) {
            e.printStackTrace();
            haltWithMessage(400, "Error deleting entity", e);
            return null;
        }
    }

    /**
     * Currently designed to delete multiple trips in a single transaction. Trip IDs should be comma-separated in a query
     * parameter. TODO: Implement this for other entity types?
     */
    private String deleteMultipleTrips(Request req, Response res) {
        String namespace = getNamespaceAndValidateSession(req);
        JdbcTableWriter tableWriter = new JdbcTableWriter(table, datasource, namespace);
        String[] tripIds = req.queryParams("tripIds").split(",");
        try {
            for (String tripId: tripIds) {
                // Delete each trip ID found in query param WITHOUT auto-committing.
                int result = tableWriter.delete(Integer.parseInt(tripId), false);
                if (result != 1) {
                    // If exactly one entity was not deleted, throw an error.
                    String message = String.format("Could not delete trip %s. Result: %d", tripId, result);
                    throw new SQLException(message);
                }
            }
            // Commit the transaction after iterating over trip IDs (because the deletes where made without autocommit).
            tableWriter.commit();
            LOG.info("Deleted {} trips", tripIds.length);
        } catch (Exception e) {
            e.printStackTrace();
            haltWithMessage(400, "Error deleting entity", e);
        }
        return formatJSON(String.format("Deleted %d.", tripIds.length), 200);
    }

    /**
     * HTTP endpoint to delete one GTFS editor entity specified by the integer ID field.
     */
    private String deleteOne(Request req, Response res) {
        String namespace = getNamespaceAndValidateSession(req);
        Integer id = getIdFromRequest(req);
        JdbcTableWriter tableWriter = new JdbcTableWriter(table, datasource, namespace);
        try {
            if (tableWriter.delete(id, true) == 1) {
                // FIXME: change return message based on result value
                return formatJSON(String.valueOf("Deleted one."), 200);
            }
        } catch (Exception e) {
            e.printStackTrace();
            haltWithMessage(400, "Error deleting entity", e);
        }
        return null;
    }

    /**
     * HTTP endpoint to upload branding image to S3 for either agency or route entities. The endpoint also handles
     * updating the branding URL field to match the S3 URL.
     */
    private String uploadEntityBranding (Request req, Response res) {
        int id = getIdFromRequest(req);
        String url = null;
        try {
            // FIXME: remove cast to string.
            String idAsString = String.valueOf(id);
            url = S3Utils.uploadBranding(req, idAsString);
        } catch (Exception e) {
            LOG.error("Could not upload branding for {}", id);
            e.printStackTrace();
            haltWithMessage(400, "Could not upload branding", e);
        }
        // Update URL in GTFS entity using JSON.
        JdbcTableWriter tableWriter = new JdbcTableWriter(table, datasource, getNamespaceAndValidateSession(req));
        ObjectNode jsonObject = mapper.createObjectNode();
        jsonObject.put(String.format("%s_branding_url", classToLowercase), url);
        try {
            return tableWriter.update(id, jsonObject.toString(), true);
        } catch (SQLException | IOException e) {
            e.printStackTrace();
            haltWithMessage(400, "Could not update branding url", e);
        }
        return null;
    }

    /**
     * HTTP endpoint to create or update a single GTFS editor entity. If the ID param is supplied and the HTTP method is
     * PUT, an update operation will be applied to the specified entity using the JSON body. Otherwise, a new entity will
     * be created.
     */
    private String createOrUpdate(Request req, Response res) {
        // Check if an update or create operation depending on presence of id param
        // This needs to be final because it is used in a lambda operation below.
        if (req.params("id") == null && req.requestMethod().equals("PUT")) {
            haltWithMessage(400, "Must provide id");
        }
        final boolean isCreating = req.params("id") == null;
        String namespace = getNamespaceAndValidateSession(req);
        Integer id = getIdFromRequest(req);
        // Get the JsonObject
        JdbcTableWriter tableWriter = new JdbcTableWriter(table, datasource, namespace);
        try {
            if (isCreating) {
                return tableWriter.create(req.body(), true);
            } else {
                return tableWriter.update(id, req.body(), true);
            }
        } catch (Exception e) {
            e.printStackTrace();
            haltWithMessage(400, "Operation failed.", e);
        }
        return null;
    }

    /**
     * Get the namespace for the feed ID found in the request. Also, check that the user has an active editing session
     * for the provided feed ID.
     */
    private static String getNamespaceAndValidateSession(Request req) {
        String feedId = req.queryParams("feedId");
        String sessionId = req.queryParams("sessionId");
        FeedSource feedSource = Persistence.feedSources.getById(feedId);
        if (feedSource == null) {
            haltWithMessage(400, "Feed ID is invalid");
        }
        // FIXME: Switch to using spark session IDs rather than query parameter?
//        String sessionId = req.session().id();
        EditorLockController.EditorSession currentSession = sessionsForFeedIds.get(feedId);
        if (currentSession == null) {
            haltWithMessage(400, "There is no active editing session for user.");
        }
        if (!currentSession.sessionId.equals(sessionId)) {
            // This session does not match the current active session for the feed.
            Auth0UserProfile userProfile = req.attribute("user");
            if (currentSession.userEmail.equals(userProfile.getEmail())) {
                LOG.warn("User {} already has editor session {} for feed {}. Same user cannot make edits on session {}.", currentSession.userEmail, currentSession.sessionId, feedId, req.session().id());
                haltWithMessage(400, "You have another editing session open for " + feedSource.name);
            } else {
                LOG.warn("User {} already has editor session {} for feed {}. User {} cannot make edits on session {}.", currentSession.userEmail, currentSession.sessionId, feedId, userProfile.getEmail(), req.session().id());
                haltWithMessage(400, "Somebody else is editing the " + feedSource.name + " feed.");
            }
        } else {
            currentSession.lastEdit = System.currentTimeMillis();
            LOG.info("Updating session {} last edit time to {}", sessionId, currentSession.lastEdit);
        }
        String namespace = feedSource.editorNamespace;
        if (namespace == null) {
            haltWithMessage(400, "Cannot edit feed that has not been snapshotted (namespace is null).");
        }
        return namespace;
    }

    /**
     * Get integer entity ID from request.
     */
    private Integer getIdFromRequest(Request req) {
        Integer id = null;
        // FIXME: what if a null value is specified in id param
        if (req.params("id") != null) {
            // If an update, parse the id param
            try {
                // If we cannot parse the integer, the ID is not valid
                id = Integer.valueOf(req.params("id"));
            } catch (NumberFormatException e) {
                LOG.error("ID provided must be an integer", e);
                haltWithMessage(400, "ID provided is not a number");
            }
        }
        return id;
    }

    // TODO add hooks
    abstract void getEntityHook(T entity);
    abstract void createEntityHook(T entity);
    abstract void updateEntityHook(T entity);
    abstract void deleteEntityHook(T entity);
}
