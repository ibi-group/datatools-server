package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.common.utils.S3Utils;
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
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.HaltException;
import spark.Request;
import spark.Response;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.conveyal.datatools.common.utils.SparkUtils.formatJSON;
import static com.conveyal.datatools.common.utils.SparkUtils.haltWithMessage;
import static com.conveyal.datatools.editor.controllers.EditorLockController.sessionsForFeedIds;
import static spark.Spark.delete;
import static spark.Spark.options;
import static spark.Spark.post;
import static spark.Spark.put;

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
        long startTime = System.currentTimeMillis();
        String namespace = getNamespaceAndValidateSession(req);
        // NOTE: This is a string pattern ID, not the integer ID that all other HTTP endpoints use.
        String patternId = req.params("id");
        if (patternId == null) {
            haltWithMessage(req, 400, "Must provide valid pattern_id");
        }
        try {
            JdbcTableWriter tableWriter = new JdbcTableWriter(Table.TRIPS, datasource, namespace);
            int deletedCount = tableWriter.deleteWhere("pattern_id", patternId, true);
            return formatJSON(String.format("Deleted %d.", deletedCount), 200);
        } catch (Exception e) {
            e.printStackTrace();
            haltWithMessage(req, 400, "Error deleting entity", e);
            return null;
        } finally {
            LOG.info("Delete operation took {} msec", System.currentTimeMillis() - startTime);
        }
    }

    /**
     * Currently designed to delete multiple trips in a single transaction. Trip IDs should be comma-separated in a query
     * parameter. TODO: Implement this for other entity types?
     */
    private String deleteMultipleTrips(Request req, Response res) {
        long startTime = System.currentTimeMillis();
        String namespace = getNamespaceAndValidateSession(req);
        String[] tripIds = req.queryParams("tripIds").split(",");
        try {
            JdbcTableWriter tableWriter = new JdbcTableWriter(table, datasource, namespace);
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
            haltWithMessage(req, 400, "Error deleting entity", e);
        } finally {
            LOG.info("Delete operation took {} msec", System.currentTimeMillis() - startTime);
        }
        return formatJSON(String.format("Deleted %d.", tripIds.length), 200);
    }

    /**
     * HTTP endpoint to delete one GTFS editor entity specified by the integer ID field.
     */
    private String deleteOne(Request req, Response res) {
        long startTime = System.currentTimeMillis();
        String namespace = getNamespaceAndValidateSession(req);
        Integer id = getIdFromRequest(req);
        try {
            JdbcTableWriter tableWriter = new JdbcTableWriter(table, datasource, namespace);
            if (tableWriter.delete(id, true) == 1) {
                // FIXME: change return message based on result value
                return formatJSON(String.valueOf("Deleted one."), 200);
            }
        } catch (Exception e) {
            e.printStackTrace();
            haltWithMessage(req, 400, "Error deleting entity", e);
        } finally {
            LOG.info("Delete operation took {} msec", System.currentTimeMillis() - startTime);
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
            url = S3Utils.uploadBranding(req, String.join("_", classToLowercase, idAsString));
        } catch (HaltException e) {
            // Do not re-catch halts thrown for exceptions that have already been caught.
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            String message = String.format("Could not upload branding for %s id=%d", classToLowercase, id);
            LOG.error(message);
            e.printStackTrace();
            haltWithMessage(req, 400, message, e);
        }
        String namespace = getNamespaceAndValidateSession(req);
        // Prepare json object for response. (Note: this is not the full entity object, but just the URL field).
        ObjectNode jsonObject = mapper.createObjectNode();
        jsonObject.put(String.format("%s_branding_url", classToLowercase), url);
        Connection connection = null;
        // Update URL in GTFS entity with simple SQL update. Note: the request object only contains an image file, so
        // the standard JdbcTableWriter update method that requires a complete JSON string cannot be used.
        try {
            connection = datasource.getConnection();
            String updateSql = String.format("update %s.%s set %s_branding_url = ?", namespace, table.name, classToLowercase);
            PreparedStatement preparedStatement = connection.prepareStatement(updateSql);
            preparedStatement.setString(1, url);
            preparedStatement.executeUpdate();
            connection.commit();
            return jsonObject.toString();
        } catch (SQLException e) {
            e.printStackTrace();
            haltWithMessage(req, 400, "Could not update branding url", e);
            return null;
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }

    /**
     * HTTP endpoint to create or update a single GTFS editor entity. If the ID param is supplied and the HTTP method is
     * PUT, an update operation will be applied to the specified entity using the JSON body. Otherwise, a new entity will
     * be created.
     */
    private String createOrUpdate(Request req, Response res) {
        long startTime = System.currentTimeMillis();
        // Check if an update or create operation depending on presence of id param
        // This needs to be final because it is used in a lambda operation below.
        if (req.params("id") == null && req.requestMethod().equals("PUT")) {
            haltWithMessage(req, 400, "Must provide id");
        }
        final boolean isCreating = req.params("id") == null;
        String namespace = getNamespaceAndValidateSession(req);
        Integer id = getIdFromRequest(req);
        // Get the JsonObject
        try {
            JdbcTableWriter tableWriter = new JdbcTableWriter(table, datasource, namespace);
            if (isCreating) {
                return tableWriter.create(req.body(), true);
            } else {
                return tableWriter.update(id, req.body(), true);
            }
        } catch (Exception e) {
            e.printStackTrace();
            haltWithMessage(req, 400, "Operation failed.", e);
        } finally {
            String operation = isCreating ? "Create" : "Update";
            LOG.info("{} operation took {} msec", operation, System.currentTimeMillis() - startTime);
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
            haltWithMessage(req, 400, "Feed ID is invalid");
        }
        // FIXME: Switch to using spark session IDs rather than query parameter?
//        String sessionId = req.session().id();
        EditorLockController.EditorSession currentSession = sessionsForFeedIds.get(feedId);
        if (currentSession == null) {
            haltWithMessage(req, 400, "There is no active editing session for user.");
        }
        if (!currentSession.sessionId.equals(sessionId)) {
            // This session does not match the current active session for the feed.
            Auth0UserProfile userProfile = req.attribute("user");
            if (currentSession.userEmail.equals(userProfile.getEmail())) {
                LOG.warn("User {} already has editor session {} for feed {}. Same user cannot make edits on session {}.", currentSession.userEmail, currentSession.sessionId, feedId, req.session().id());
                haltWithMessage(req, 400, "You have another editing session open for " + feedSource.name);
            } else {
                LOG.warn("User {} already has editor session {} for feed {}. User {} cannot make edits on session {}.", currentSession.userEmail, currentSession.sessionId, feedId, userProfile.getEmail(), req.session().id());
                haltWithMessage(req, 400, "Somebody else is editing the " + feedSource.name + " feed.");
            }
        } else {
            currentSession.lastEdit = System.currentTimeMillis();
            LOG.info("Updating session {} last edit time to {}", sessionId, currentSession.lastEdit);
        }
        String namespace = feedSource.editorNamespace;
        if (namespace == null) {
            haltWithMessage(req, 400, "Cannot edit feed that has not been snapshotted (namespace is null).");
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
                haltWithMessage(req, 400, "ID provided is not a number");
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
