package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.editor.controllers.EditorLockController;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.JdbcTableWriter;
import com.conveyal.gtfs.loader.Requirement;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.Entity;
import com.conveyal.gtfs.storage.StorageException;
import com.conveyal.gtfs.util.InvalidNamespaceException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.dbutils.DbUtils;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.HaltException;
import spark.Request;
import spark.Response;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.conveyal.datatools.common.utils.SparkUtils.formatJSON;
import static com.conveyal.datatools.common.utils.SparkUtils.getObjectNode;
import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;
import static com.conveyal.datatools.manager.controllers.api.UserController.inTestingEnvironment;
import static org.eclipse.jetty.http.HttpStatus.OK_200;
import static spark.Spark.delete;
import static spark.Spark.options;
import static spark.Spark.patch;
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
    private static final String SNAKE_CASE_REGEX = "\\b[a-z]+(_[a-z]+)*\\b";
    private static final ObjectMapper mapper = new ObjectMapper();
    public static final JsonManager<Entity> json = new JsonManager<>(Entity.class, JsonViews.UserInterface.class);
    private final Table table;
    // List of operators used to construct where clauses. Derived from list maintained for Postgrest:
    // https://github.com/PostgREST/postgrest/blob/75a42b77ea59724cd8b5020781ac8685100667f8/src/PostgREST/Types.hs#L298-L316
    // Postgrest docs: http://postgrest.org/en/v6.0/api.html#operators
    // Note: not all of these are tested. Expect the array or ranged operators to fail.
    private final Map<String, String> operators = Stream.of(new String[][] {
        {"eq", "="},
        {"gte", ">="},
        {"gt", ">"},
        {"lte", "<="},
        {"lt", "<"},
        {"neq", "<>"},
        {"like", "LIKE"},
        {"ilike", "ILIKE"},
        {"in", "IN"},
        {"is", "IS"},
        {"cs", "@>"},
        {"cd", "<@"},
        {"ov", "&&"},
        {"sl", "<<"},
        {"sr", ">>"},
        {"nxr", "&<"},
        {"nxl", "&>"},
        {"adj", "-|-"},
    }).collect(Collectors.toMap(data -> data[0], data -> data[1]));

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
        // Patch table request (set values for certain fields for all or some of the records in a table).
        patch(ROOT_ROUTE, this::patchTable, json::write);
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
            put(ROOT_ROUTE + ID_PARAM + "/stop_times", this::updateStopTimesFromPatternStops, json::write);
            delete(ROOT_ROUTE + ID_PARAM + "/trips", this::deleteTripsForPattern, json::write);
        }

        if ("stop".equals(classToLowercase)) {
            delete(ROOT_ROUTE + ID_PARAM + "/cascadeDeleteStop", this::cascadeDeleteStop, json::write);
        }
    }

    /**
     * HTTP endpoint to patch an entire table with the provided JSON object according to the filtering criteria provided
     * in the query parameters.
     */
    private String patchTable(Request req, Response res) {
        String namespace = getNamespaceAndValidateSession(req);
        // Collect fields to filter on with where clause from the query parameters.
        List<Field> filterFields = new ArrayList<>();
        for (String param : req.queryParams()) {
            // Skip the feed and session IDs used to get namespace/validate editing session.
            if ("feedId".equals(param) || "sessionId".equals(param)) continue;
            filterFields.add(table.getFieldForName(param));
        }
        Connection connection = null;
        try {
            // First, check that the field names all conform to the GTFS snake_case convention as a guard against SQL
            // injection.
            JsonNode jsonNode = mapper.readTree(req.body());
            if (jsonNode == null) {
                logMessageAndHalt(req, 400, "JSON body must be provided with patch table request.");
            }
            Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
            List<Field> fieldsToPatch = new ArrayList<>();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String fieldName = field.getKey();
                if (!fieldName.matches(SNAKE_CASE_REGEX)) {
                    logMessageAndHalt(req, 400, "Field does not match GTFS snake_case convention: " + fieldName);
                }
                Field fieldToPatch = table.getFieldForName(fieldName);
                if (fieldToPatch.requirement.equals(Requirement.UNKNOWN)) {
                    LOG.warn("Attempting to modify unknown field: {}", fieldToPatch.name);
                }
                fieldsToPatch.add(fieldToPatch);
            }
            // Initialize the update SQL and add all of the patch fields.
            String updateSql = String.format("update %s.%s set ", namespace, table.name);
            String setFields = fieldsToPatch.stream()
                .map(field -> field.name + " = ?")
                .collect(Collectors.joining(", "));
            updateSql += setFields;
            // Next, construct the where clause from any filter fields found above.
            List<String> filterValues = new ArrayList<>();
            List<String> filterConditionStrings = new ArrayList<>();
            if (filterFields.size() > 0) {
                updateSql += " where ";
                try {
                    for (Field field : filterFields) {
                        String[] filter = req.queryParams(field.name).split("\\.", 2);
                        String operator = operators.get(filter[0]);
                        if (operator == null) {
                            logMessageAndHalt(req, 400, "Invalid operator provided: " + filter[0]);
                        }
                        filterValues.add(filter[1]);
                        filterConditionStrings.add(String.format(" %s %s ?", field.name, operator));
                    }
                    String conditions = String.join(" AND ", filterConditionStrings);
                    updateSql += conditions;
                } catch (ArrayIndexOutOfBoundsException e) {
                    logMessageAndHalt(req, 400, "Error encountered parsing filter.", e);
                }
            }
            // Set up the db connection and set all of the patch and where clause parameters.
            connection = datasource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(updateSql);
            int oneBasedIndex = 1;
            for (Field field : fieldsToPatch) {
                field.setParameter(preparedStatement, oneBasedIndex, jsonNode.get(field.name).asText());
                oneBasedIndex++;
            }
            for (int i = 0; i < filterFields.size(); i++) {
                Field field = filterFields.get(i);
                try {
                    field.setParameter(preparedStatement, oneBasedIndex, filterValues.get(i));
                } catch (Exception e) {
                    logMessageAndHalt(req, 400, "Invalid value used for field " + field.name, e);
                }
                oneBasedIndex++;
            }
            // Execute the update and commit!
            LOG.info(preparedStatement.toString());
            int recordsUpdated = preparedStatement.executeUpdate();
            connection.commit();
            ObjectNode response = getObjectNode(String.format("%d %s(s) updated", recordsUpdated, classToLowercase), HttpStatus.OK_200, null);
            response.put("count", recordsUpdated);
            return response.toString();
        } catch (HaltException e) {
            throw e;
        } catch (StorageException e) {
            // If an invalid value was applied to a field filter, a Storage Exception will be thrown, which we should
            // catch and share details with the user.
            logMessageAndHalt(req, 400, "Could not patch update table", e);
        } catch (Exception e) {
            // This catch-all accounts for any issues encountered with SQL exceptions or other unknown issues.
            logMessageAndHalt(req, 500, "Could not patch update table", e);
        } finally {
            DbUtils.closeQuietly(connection);
        }
        return null;
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
            logMessageAndHalt(req, 400, "Must provide valid pattern_id");
        }
        try {
            JdbcTableWriter tableWriter = new JdbcTableWriter(Table.TRIPS, datasource, namespace);
            int deletedCount = tableWriter.deleteWhere("pattern_id", patternId, true);
            return formatJSON(String.format("Deleted %d.", deletedCount), 200);
        } catch (InvalidNamespaceException e) {
            logMessageAndHalt(req, 400, "Invalid namespace");
            return null;
        } catch (Exception e) {
            logMessageAndHalt(req, 500, "Error deleting entity", e);
            return null;
        } finally {
            LOG.info("Delete trips for pattern operation took {} msec", System.currentTimeMillis() - startTime);
        }
    }

    /**
     * HTTP endpoint to delete a stop and all references in stop times and pattern stops given a string stop_id (i.e. not
     * the integer ID field). Then normalize the stop times for all updated patterns (i.e. the ones where the stop has
     * been deleted).
     */
    private String cascadeDeleteStop(Request req, Response res) {
        // Table writer closes the database connection after use, so a new one is required for each task.
        JdbcTableWriter tableWriter;
        long startTime = System.currentTimeMillis();
        String namespace = getNamespaceAndValidateSession(req);
        String stopIdColumnName = "stop_id";

        // NOTE: This is a string stop ID, not the integer ID that other HTTP endpoints use.
        String stopId = req.params("id");
        if (stopId == null) {
            logMessageAndHalt(req, 400, "Must provide a valid stopId.");
        }

        try (
            Connection connection = datasource.getConnection();
            PreparedStatement statement = connection.prepareStatement(
                String.format("select id, stop_sequence from %s.pattern_stops where %s = ?", namespace, stopIdColumnName)
            )
        ) {
            // Get the patterns to be normalized before the related stop is deleted.
            statement.setString(1, stopId);
            ResultSet resultSet = statement.executeQuery();
            Map<Integer, Integer> patternsToBeNormalized = new HashMap<>();
            while (resultSet.next()) {
                patternsToBeNormalized.put(
                    resultSet.getInt("id"),
                    resultSet.getInt("stop_sequence")
                );
            }

            tableWriter = new JdbcTableWriter(Table.STOP_TIMES, datasource, namespace);
            int deletedCountStopTimes = tableWriter.deleteWhere(stopIdColumnName, stopId, true);

            int deletedCountPatternStops = 0;
            if (!patternsToBeNormalized.isEmpty()) {
                tableWriter = new JdbcTableWriter(Table.PATTERN_STOP, datasource, namespace);
                deletedCountPatternStops = tableWriter.deleteWhere(stopIdColumnName, stopId, true);
                if (deletedCountPatternStops > 0) {
                    for (Map.Entry<Integer, Integer> patternStop : patternsToBeNormalized.entrySet()) {
                        tableWriter = new JdbcTableWriter(Table.PATTERN_STOP, datasource, namespace);
                        int stopSequence = patternStop.getValue();
                        // Begin with the stop prior to the one deleted, unless at the beginning.
                        int beginWithSequence = (stopSequence != 0) ? stopSequence - 1 : stopSequence;
                        tableWriter.normalizeStopTimesForPattern(patternStop.getKey(), beginWithSequence);
                    }
                }
            }

            tableWriter = new JdbcTableWriter(Table.STOPS, datasource, namespace);
            int deletedCountStop = tableWriter.deleteWhere(stopIdColumnName, stopId, true);

            return formatJSON(
                String.format(
                    "Deleted %d stop, %d pattern stops and %d stop times.",
                    deletedCountStop,
                    deletedCountPatternStops,
                    deletedCountStopTimes),
                OK_200
            );
        } catch (InvalidNamespaceException e) {
            logMessageAndHalt(req, 400, "Invalid namespace.", e);
            return null;
        } catch (Exception e) {
            logMessageAndHalt(req, 500, "Error deleting entity.", e);
            return null;
        } finally {
            LOG.info("Cascade delete of stop operation took {} msec.", System.currentTimeMillis() - startTime);
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
        JdbcTableWriter tableWriter = null;
        try {
            tableWriter = new JdbcTableWriter(table, datasource, namespace);
            for (String tripId: tripIds) {
                // Delete each trip ID found in query param WITHOUT auto-committing.
                int result = tableWriter.delete(Integer.parseInt(tripId), false);
                if (result != 1) {
                    // If exactly one entity was not deleted, throw an error.
                    String message = String.format("Could not delete trip %s. Result: %d", tripId, result);
                    throw new SQLException(message);
                }
            }
            // Commit the transaction after iterating over trip IDs (because the deletes were made without autocommit).
            tableWriter.commit();
            LOG.info("Deleted {} trips", tripIds.length);
        } catch (InvalidNamespaceException e) {
            logMessageAndHalt(req, 400, "Invalid namespace");
        } catch (Exception e) {
            logMessageAndHalt(req, 500, "Error deleting entity", e);
        } finally {
            if (tableWriter != null) tableWriter.close();
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
                return formatJSON("Deleted one.", 200);
            }
        } catch (Exception e) {
            logMessageAndHalt(req, 400, "Error deleting entity", e);
        } finally {
            LOG.info("Delete operation took {} msec", System.currentTimeMillis() - startTime);
        }
        return null;
    }

    /**
     * For a given pattern ID, update all its trips' stop times to conform to the default travel and dwell times. This
     * is used, for instance, when a new pattern stop is added or inserted into an existing pattern that has trips which
     * need the updated travel times applied in bulk.
     */
    private String updateStopTimesFromPatternStops (Request req, Response res) {
        long startTime = System.currentTimeMillis();
        String namespace = getNamespaceAndValidateSession(req);
        int patternId = getIdFromRequest(req);
        try {
            int beginStopSequence = Integer.parseInt(req.queryParams("stopSequence"));
            JdbcTableWriter tableWriter = new JdbcTableWriter(table, datasource, namespace);
            int stopTimesUpdated = tableWriter.normalizeStopTimesForPattern(patternId, beginStopSequence);
            return SparkUtils.formatJSON("updateResult", stopTimesUpdated + " stop times updated.");
        } catch (Exception e) {
            logMessageAndHalt(req, 400, "Error normalizing stop times", e);
            return null;
        } finally {
            LOG.info("Normalize stop times operation took {} msec", System.currentTimeMillis() - startTime);
        }
    }

    /**
     * HTTP endpoint to upload branding image to S3 for either agency or route entities. The endpoint also handles
     * updating the branding URL field to match the S3 URL.
     */
    private String uploadEntityBranding (Request req, Response res) {
        int id = getIdFromRequest(req);
        String url;
        try {
            url = SparkUtils.uploadMultipartRequestBodyToS3(req, "branding", String.format("%s_%d", classToLowercase, id));
        } catch (HaltException e) {
            // Do not re-catch halts thrown for exceptions that have already been caught.
            throw e;
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
        } catch (Exception e) {
            logMessageAndHalt(req, 500, "Could not update branding url", e);
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
            logMessageAndHalt(req, 400, "Must provide id");
        }
        final boolean isCreating = req.params("id") == null;
        String namespace = getNamespaceAndValidateSession(req);
        Integer id = getIdFromRequest(req);
        // Save or update to database
        try {
            JdbcTableWriter tableWriter = new JdbcTableWriter(table, datasource, namespace);
            String jsonBody = req.body();
            if (isCreating) {
                return tableWriter.create(jsonBody, true);
            } else {
                return update(tableWriter, id, jsonBody);
            }
        } catch (InvalidNamespaceException e) {
            logMessageAndHalt(req, 400, "Invalid namespace");
        } catch (IOException e) {
            logMessageAndHalt(req, 400, "Invalid json", e);
        } catch (Exception e) {
            logMessageAndHalt(req, 500, "An error was encountered while trying to save to the database", e);
        } finally {
            String operation = isCreating ? "Create" : "Update";
            LOG.info("{} operation took {} msec", operation, System.currentTimeMillis() - startTime);
        }
        return null;
    }

    /**
     * Handle specific entity updates.
     */
    private String update(JdbcTableWriter tableWriter, Integer id, String jsonBody) throws IOException, SQLException {
        try {
            return tableWriter.update(id, jsonBody, true);
        } catch (SQLException e) {
            if (table.name.equals(Table.TRIPS.name)) {
                // If an exception is thrown updating a trip, provide additional information to help rectify the
                // issue.
                JsonNode trip = mapper.readTree(jsonBody);
                throw new SQLException(
                    String.format(
                        "Trip id %s conflicts with an existing trip id.",
                        trip.get("trip_id").asText()
                    )
                );
            } else {
                // Continue to pass the exception to the frontend.
                throw e;
            }
        }
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
            logMessageAndHalt(req, 400, "Feed ID is invalid");
        }
        // FIXME: Switch to using spark session IDs rather than query parameter?
//        String sessionId = req.session().id();
        // Only check for editing session if not in testing environment.
        // TODO: Add way to mock session.
        if (!inTestingEnvironment()) {
            Auth0UserProfile userProfile = req.attribute("user");
            EditorLockController.EditorSession currentSession = EditorLockController.getSession(feedId);
            if (EditorLockController.checkUserHasActiveSession(req, sessionId, userProfile.getEmail(), currentSession)) {
                currentSession.lastEdit = System.currentTimeMillis();
                LOG.info("Updating session {} last edit time to {}", sessionId, currentSession.lastEdit);
            }
        }
        String namespace = feedSource.editorNamespace;
        if (namespace == null) {
            logMessageAndHalt(req, 400, "Cannot edit feed that has not been snapshotted (namespace is null).");
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
                logMessageAndHalt(req, 400, "ID provided is not a number");
            }
        }
        return id;
    }
}
