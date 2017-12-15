package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.common.utils.S3Utils;
import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.Entity;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.conveyal.datatools.common.utils.SparkUtils.formatJSON;
import static com.conveyal.datatools.common.utils.SparkUtils.haltWithError;
import static spark.Spark.*;
import static spark.Spark.delete;
import static spark.Spark.post;

public abstract class EditorController<T extends Entity> {
    private static final String ID_PARAM = "/:id";
    private final String ROOT_ROUTE;
    private final Class entityClass;
    private static final String SECURE = "secure/";
    private static final Logger LOG = LoggerFactory.getLogger(EditorController.class);
    private final String apiPrefix;
    private DataSource datasource;
    private final String classToLowercase;
    public static final JsonManager<Entity> json = new JsonManager<>(Entity.class, JsonViews.UserInterface.class);
    private final ResultMapper resultMapper;
    private final Table table;
    private Gson gson = new GsonBuilder().setPrettyPrinting().create();

    EditorController(String apiPrefix, Table table, DataSource datasource) {
        this.table = table;
        this.entityClass = table.getEntityClass();
        this.apiPrefix = apiPrefix;
        this.datasource = datasource;
        this.classToLowercase = entityClass.getSimpleName().toLowerCase();
        this.ROOT_ROUTE = apiPrefix + SECURE + classToLowercase;
        this.resultMapper = new ResultMapper();
        registerRoutes();
    }

    private FeedSource getFeedSourceFromRequest(Request req) {
        String feedId = req.queryParams("feedId");
        FeedSource feedSource = Persistence.feedSources.getById(feedId);
        if (feedSource == null) {
            haltWithError(400, "Feed ID is invalid");
        }
        return feedSource;
    }

    private void registerRoutes() {
        LOG.info("Registering editor routes for {}", ROOT_ROUTE);
        // Get single entity method
        // TODO: Delete unneeded query?
        get(ROOT_ROUTE + ID_PARAM, resultMapper::getOne, json::write);
        // Options response for CORS
        options(ROOT_ROUTE, (q, s) -> "");
        // Get all entities request
        // TODO: Delete unneeded query?
        get(ROOT_ROUTE, resultMapper::getAll, json::write);
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
    }

    private String deleteOne(Request req, Response res) {
        FeedSource feedSource = getFeedSourceFromRequest(req);
        Connection connection;
        try {
            connection = datasource.getConnection();
            PreparedStatement statement =
                    connection.prepareStatement(table.generateDeleteSql(feedSource.editorNamespace));
            // FIXME: Handle cascading delete
            statement.setInt(1, Integer.parseInt(req.params("id")));
            LOG.info(statement.toString());
            // Execute query
            int result = statement.executeUpdate();
            connection.commit();
            if (result == 0) {
                throw new SQLException("Could not delete entity");
            }
            // FIXME: change return message based on result value
            return formatJSON(String.valueOf(result), 200);
        } catch (SQLException e) {
            e.printStackTrace();
            haltWithError(400, "Error deleting entity", e);
        }
        return null;
    }

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
            haltWithError(400, "Could not upload branding", e);
        }
        // FIXME: handle closing connection??
        Connection connection;
        try {
            connection = datasource.getConnection();
            // set x_branding_url to new s3 URL
            String tableName = getTableNameFromRequest(req);
            String statementString = String.format("update %s set %s_branding_url = '%s' where id = %d", tableName, classToLowercase, url, id);
            PreparedStatement statement = connection.prepareStatement(statementString, Statement.RETURN_GENERATED_KEYS);
            handleStatementExecution(connection, statement, false);
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("url", url);
            return jsonObject.toString();
        } catch (SQLException e) {
            LOG.error("Error storing branding URL", e);
            haltWithError(400, "Error storing branding URL", e);
        }
        return null;
    }

    private JsonObject getJsonObjectFromRequest (Request req) {
        JsonElement jsonElement = null;
        try {
            jsonElement = gson.fromJson(req.body(), JsonElement.class);
        } catch (JsonSyntaxException e) {
            LOG.error("Bad JSON syntax", e);
            haltWithError(400, "Bad JSON syntax", e);
        }
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        return jsonObject;
    }

    /**
     * Create or update entity. Update depends on existence of ID param, which should only be supplied by the PUT method.
     */
    private String createOrUpdate(Request req, Response res) {
        // Check if an update or create operation depending on presence of id param
        // This needs to be final because it is used in a lambda operation below.
        final boolean isCreating = req.params("id") == null;
        Integer id = getIdFromRequest(req);
        // Get the JsonObject
        JsonObject jsonObject = getJsonObjectFromRequest(req);
        // Parse the fields/values into a Field -> String map (drops ALL unknown fields)
        Map<Field, String> fieldStringMap = jsonToFieldStringMap(jsonObject);
        String tableName = getTableNameFromRequest(req);
        Connection connection;
        try {
            connection = datasource.getConnection();
            // Ensure that the key field is unique and that referencing tables are updated if the value is updated.
            ensureReferentialIntegrity(connection, jsonObject, isCreating, tableName, table.getKeyFieldName(), id);
            // Collect field names for string joining from JsonObject.
            List<String> fieldNames = fieldStringMap.keySet().stream()
                    // If updating, add suffix for use in set clause
                    .map(field -> isCreating ? field.getName() : field.getName() + " = ?")
                    .collect(Collectors.toList());
            String statementString;
            if (isCreating) {
                // If creating a new entity, use default next value for ID.
                String questionMarks = String.join(", ", Collections.nCopies(fieldNames.size(), "?"));
                statementString = String.format("insert into %s (id, %s) values (DEFAULT, %s)", tableName, String.join(", ", fieldNames), questionMarks);
            } else {
                // If updating an existing entity, specify record to update fields found with where clause on id.
                statementString = String.format("update %s set %s where id = %d", tableName, String.join(", ", fieldNames), id);
            }
            // Prepare statement and set RETURN_GENERATED_KEYS to get the entity id back
            PreparedStatement preparedStatement = connection.prepareStatement(
                    statementString,
                    Statement.RETURN_GENERATED_KEYS);
            // one-based index for statement parameters
            int index = 1;
            // Fill statement parameters with strings from JSON
            for (Map.Entry<Field, String> entry : fieldStringMap.entrySet()) {
                entry.getKey().setParameter(preparedStatement, index, entry.getValue());
                index += 1;
            }
            // ID from create/update result
            long newId = handleStatementExecution(connection, preparedStatement, isCreating);
            // Add new ID to JSON object.
            jsonObject.addProperty("id", newId);
            // FIXME: Should this return the entity from the database?
            return jsonObject.toString();
        } catch (Exception e) {
            e.printStackTrace();
            haltWithError(400, "Operation failed.", e);
        }
        return null;
    }

    /**
     * Checks for modification of GTFS key field (e.g., stop_id, route_id) in supplied JSON object and ensures
     * both uniqueness and that referencing tables are appropriately updated.
     * @throws Exception
     */
    private static void ensureReferentialIntegrity(Connection connection, JsonObject jsonObject, boolean isCreating, String tableName, String keyField, Integer id) throws Exception {
        String keyValue = jsonObject.get(keyField).getAsString();
        // If updating key field, check that there is no ID conflict on value (e.g., stop_id or route_id)
        String idCheckSql = String.format("select * from %s where %s = '%s'", tableName, keyField, keyValue);
        LOG.info(idCheckSql);
        // Create statement for counting rows selected
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(idCheckSql);
        // Keep track of number of records found with key field
        int size = 0;
        Set<String> uniqueIds = new HashSet<>();
        while (resultSet.next()) {
            String uniqueId = resultSet.getString(1);
            uniqueIds.add(uniqueId);
            LOG.info("id: {}, name: {}", uniqueId, resultSet.getString(4));
            size++;
        }
        if (size == 1) {
            if (isCreating) {
                // Under no circumstance should a entity have a conflict with existing key field.
                throw new Exception("New entity's key field must not match existing value.");
            }
            if (!uniqueIds.contains(id.toString())) {
                // ID for this entity has been updated to match some other entity's ID (conflict).
                // If this condition is not met, the entity being updated is the same as the queried entity (no conflict).
                throw new Exception("Entity ID must be unique.");
            }
        } else if (size > 1) {
            // FIXME: Handle edge case where original data set contains duplicate values for key field and this is an
            // attempt to rectify bad data.
            LOG.warn("{} entity shares the same key field ({}={})! This is Bad!!", size, keyField, keyValue);
            throw new Exception("More than one entity must not share the same id field");
        } else if (size == 0 && !isCreating) {
            // FIXME: update referencing tables.
            // Entity key value is being changed to an entirely new one.  We need to update any entities that
            // reference this value
            LOG.info("Key being changed, but no matches. FIXME: need to update related entities.");
        }
    }

    private long handleStatementExecution(Connection connection, PreparedStatement statement, boolean isCreating) {
        try {
            // Log the SQL for the prepared statement
            LOG.info(statement.toString());
            int affectedRows = statement.executeUpdate();
            // Commit the transaction
            connection.commit();
            // Determine operation-specific action for any error messages
            String messageAction = isCreating ? "Creating" : "Updating";
            if (affectedRows == 0) {
                // No update occurred.
                // TODO: add some clarity on cause (e.g., where clause found no entity with provided ID)?
                throw new SQLException(messageAction + " entity failed, no rows affected.");
            }
            try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    // Get the auto-generated ID from the update execution
                    long newId = generatedKeys.getLong(1);
                    return newId;
                } else {
                    throw new SQLException(messageAction + " entity failed, no ID obtained.");
                }
            }
        } catch (SQLException e) {
            haltWithError(400, "Operation failed.", e);
            e.printStackTrace();
        }
        return 0;
    }

    private String getTableNameFromRequest(Request req) {
        FeedSource feedSource = getFeedSourceFromRequest(req);
        String namespace = feedSource.editorNamespace;
        if (namespace == null) {
            haltWithError(400, "Cannot edit feed that has not been snapshotted (namespace is null).");
        }
        String tableName = String.join(".", namespace, table.name);
        return tableName;
    }

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
                haltWithError(400, "ID provided is not a number");
            }
        }
        return id;
    }

    /**
     * Handles converting JSON object into a map from Fields (which have methods to set parameter type in SQL
     * statements) to String values found in the JSON.
     */
    private Map<Field, String> jsonToFieldStringMap(JsonObject jsonObject) {
        HashMap<Field, String> fieldStringMap = new HashMap<>();
        // Iterate over fields in JSON and remove those not found in table.
        for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
            // TODO: clean field names? Currently, unknown fields are just skipped, but in the future, if a bad
            // field name is found, we will want to throw an exception, log an error, and halt.
            Field field = table.getFieldForName(entry.getKey());
            if (field.isUnknown()) {
                // Skip all unknown fields (this includes id field because it is not listed in table fields)
                continue;
            }
            JsonElement value = entry.getValue();
            // FIXME: need to be able to set fields to null and handle empty strings -> null
            if (!value.isJsonNull()) {
                // Only add non-null fields to map
                fieldStringMap.put(field, value.getAsString());
            }
        }
        return fieldStringMap;
    }

    private T getEntityFromRequest(Request req) {
        try {
            if ("".equals(req.body())) {
                haltWithError(400, "JSON body must not be empty");
            }
            T entity = (T) Base.mapper.readValue(req.body(), entityClass);
            return entity;
        } catch (IOException e) {
            LOG.error("Could not read entity from JSON", e);
            haltWithError(400, "Could not read entity from JSON.", e);
        }
        return null;
    }

    // TODO add hooks
    abstract void getEntityHook(T entity);
    abstract void createEntityHook(T entity);
    abstract void updateEntityHook(T entity);
    abstract void deleteEntityHook(T entity);

//    /**
//     * Wrapper function to get entity without throwing SQL exception.
//     */
//    private T getEntity (TableReader table, String id) {
//        try {
//            return (T) table.get(id);
//        } catch (Exception e) {
//            LOG.error("Could not find entity: {}", e.getMessage());
//            return null;
//        }
//    }

    private Feed getFeedFromRequest(Request req) {
        String feedId = req.queryParams("feedId");
        FeedSource feedSource = Persistence.feedSources.getById(feedId);
        if (feedSource == null) {
            haltWithError(400, "Feed ID is invalid");
        }
        Feed feed = new Feed(DataManager.GTFS_DATA_SOURCE, feedSource.editorNamespace);
        return feed;
    }

    private class ResultMapper {
        private final ObjectMapper objectMapper;

        ResultMapper () {
            SimpleModule module = new SimpleModule();
            module.addSerializer(new ResultSetSerializer());
            objectMapper = new ObjectMapper();
            objectMapper.registerModule(module);
        }

        // FIXME: does not filter with where clause on ID
        public String getOne(Request req, Response res) {
            FeedSource feedSource = getFeedSourceFromRequest(req);
            Integer id = Integer.valueOf(req.params("id"));
            Connection connection = null;
            try {
                connection = datasource.getConnection();
                // FIXME: add where clause for single entity
                PreparedStatement statement =
                        connection.prepareStatement(table.generateSelectSql(feedSource.editorNamespace));
                // Execute query
                ResultSet resultset = statement.executeQuery();

                // Use the DataBind Api here
                ObjectNode objectNode = objectMapper.createObjectNode();

                // put the resultset in a containing structure
                objectNode.putPOJO("data", resultset);

                // generate all
                return objectMapper.writeValueAsString(objectNode.get("data"));
            } catch (SQLException | JsonProcessingException e) {
                e.printStackTrace();
                haltWithError(400, "Error fetching entities", e);
            }
            return null;
        }

        public String getAll(Request req, Response res) {
            FeedSource feedSource = getFeedSourceFromRequest(req);
            Connection connection = null;
            try {
                connection = datasource.getConnection();
                PreparedStatement statement =
                        connection.prepareStatement(table.generateSelectSql(feedSource.editorNamespace));
                // Execute query
                ResultSet resultset = statement.executeQuery();

                // Use the DataBind Api here
                ObjectNode objectNode = objectMapper.createObjectNode();

                // put the resultset in a containing structure
                objectNode.putPOJO("data", resultset);

                // generate all
                return objectMapper.writeValueAsString(objectNode.get("data"));
            } catch (SQLException | JsonProcessingException e) {
                e.printStackTrace();
                haltWithError(400, "Error fetching entities", e);
            }
            return null;
        }
    }

    private class ResultSetSerializer extends JsonSerializer<ResultSet> {



        @Override
        public Class<ResultSet> handledType() {
            return ResultSet.class;
        }

        @Override
        public void serialize(ResultSet rs, JsonGenerator jgen, SerializerProvider provider) throws IOException {

            try {
                ResultSetMetaData rsmd = rs.getMetaData();
                int numColumns = rsmd.getColumnCount();
                String[] columnNames = new String[numColumns];
                int[] columnTypes = new int[numColumns];

                for (int i = 0; i < columnNames.length; i++) {
                    columnNames[i] = rsmd.getColumnLabel(i + 1);
                    columnTypes[i] = rsmd.getColumnType(i + 1);
                }

                jgen.writeStartArray();

                while (rs.next()) {

                    boolean b;
                    long l;
                    double d;

                    jgen.writeStartObject();

                    for (int i = 0; i < columnNames.length; i++) {

                        jgen.writeFieldName(columnNames[i]);
                        switch (columnTypes[i]) {

                            case Types.INTEGER:
                                l = rs.getInt(i + 1);
                                if (rs.wasNull()) {
                                    jgen.writeNull();
                                } else {
                                    jgen.writeNumber(l);
                                }
                                break;

                            case Types.BIGINT:
                                l = rs.getLong(i + 1);
                                if (rs.wasNull()) {
                                    jgen.writeNull();
                                } else {
                                    jgen.writeNumber(l);
                                }
                                break;

                            case Types.DECIMAL:
                            case Types.NUMERIC:
                                jgen.writeNumber(rs.getBigDecimal(i + 1));
                                break;

                            case Types.FLOAT:
                            case Types.REAL:
                            case Types.DOUBLE:
                                d = rs.getDouble(i + 1);
                                if (rs.wasNull()) {
                                    jgen.writeNull();
                                } else {
                                    jgen.writeNumber(d);
                                }
                                break;

                            case Types.NVARCHAR:
                            case Types.VARCHAR:
                            case Types.LONGNVARCHAR:
                            case Types.LONGVARCHAR:
                                jgen.writeString(rs.getString(i + 1));
                                break;

                            case Types.BOOLEAN:
                            case Types.BIT:
                                b = rs.getBoolean(i + 1);
                                if (rs.wasNull()) {
                                    jgen.writeNull();
                                } else {
                                    jgen.writeBoolean(b);
                                }
                                break;

                            case Types.BINARY:
                            case Types.VARBINARY:
                            case Types.LONGVARBINARY:
                                jgen.writeBinary(rs.getBytes(i + 1));
                                break;

                            case Types.TINYINT:
                            case Types.SMALLINT:
                                l = rs.getShort(i + 1);
                                if (rs.wasNull()) {
                                    jgen.writeNull();
                                } else {
                                    jgen.writeNumber(l);
                                }
                                break;

                            case Types.DATE:
                                provider.defaultSerializeDateValue(rs.getDate(i + 1), jgen);
                                break;

                            case Types.TIMESTAMP:
                                provider.defaultSerializeDateValue(rs.getTime(i + 1), jgen);
                                break;

                            case Types.BLOB:
                                Blob blob = rs.getBlob(i);
                                provider.defaultSerializeValue(blob.getBinaryStream(), jgen);
                                blob.free();
                                break;

                            case Types.CLOB:
                                Clob clob = rs.getClob(i);
                                provider.defaultSerializeValue(clob.getCharacterStream(), jgen);
                                clob.free();
                                break;

                            case Types.ARRAY:
                                throw new RuntimeException("ResultSetSerializer not yet implemented for SQL type ARRAY");

                            case Types.STRUCT:
                                throw new RuntimeException("ResultSetSerializer not yet implemented for SQL type STRUCT");

                            case Types.DISTINCT:
                                throw new RuntimeException("ResultSetSerializer not yet implemented for SQL type DISTINCT");

                            case Types.REF:
                                throw new RuntimeException("ResultSetSerializer not yet implemented for SQL type REF");

                            case Types.JAVA_OBJECT:
                            default:
                                provider.defaultSerializeValue(rs.getObject(i + 1), jgen);
                                break;
                        }
                    }

                    jgen.writeEndObject();
                }

                jgen.writeEndArray();

            } catch (SQLException e) {
                throw new IOException(e);
            }
        }
    }

}
