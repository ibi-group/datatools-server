package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.common.utils.S3Utils;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.conveyal.gtfs.loader.JdbcTableWriter;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.Entity;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.JsonObject;
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
import java.sql.Types;

import static com.conveyal.datatools.common.utils.SparkUtils.formatJSON;
import static com.conveyal.datatools.common.utils.SparkUtils.haltWithError;
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
    public static final JsonManager<Entity> json = new JsonManager<>(Entity.class, JsonViews.UserInterface.class);
    private final ResultMapper resultMapper;
    private final Table table;

    EditorController(String apiPrefix, Table table, DataSource datasource) {
        this.table = table;
        this.datasource = datasource;
        this.classToLowercase = table.getEntityClass().getSimpleName().toLowerCase();
        this.ROOT_ROUTE = apiPrefix + SECURE + classToLowercase;
        this.resultMapper = new ResultMapper();
        registerRoutes();
    }

    /**
     * Add static HTTP endpoints to Spark static instance.
     */
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

        // Handle special multiple delete method for trip endpoint
        if ("trip".equals(classToLowercase)) {
            delete(ROOT_ROUTE, this::deleteMultiple, json::write);
        }
    }

    private String deleteMultiple(Request req, Response res) {
        String namespace = getNamespaceFromRequest(req);
        JdbcTableWriter tableWriter = new JdbcTableWriter(table, datasource, namespace);
        String[] tripIds = req.queryParams("tripIds").split(",");
        for (String tripId: tripIds) {
            try {
                if (tableWriter.delete(Integer.parseInt(tripId)) == 1) {
                    continue;
                    // FIXME: change return message based on result value
//                        return formatJSON(String.valueOf("Deleted one."), 200);
                }
            } catch (Exception e) {
                e.printStackTrace();
                haltWithError(400, "Error deleting entity", e);
            }
        }
        return formatJSON(String.format("Deleted %d.", tripIds.length), 200);
    }

    private String deleteOne(Request req, Response res) {
        String namespace = getNamespaceFromRequest(req);
        Integer id = getIdFromRequest(req);
        JdbcTableWriter tableWriter = new JdbcTableWriter(table, datasource, namespace);
        try {
            if (tableWriter.delete(id) == 1) {
                // FIXME: change return message based on result value
                return formatJSON(String.valueOf("Deleted one."), 200);
            }
        } catch (Exception e) {
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
        // Update URL in GTFS entity using JSON.
        JdbcTableWriter tableWriter = new JdbcTableWriter(table, datasource, getNamespaceFromRequest(req));
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty(String.format("%s_branding_url", classToLowercase), url);
        try {
            return tableWriter.update(id, jsonObject.getAsString());
        } catch (SQLException e) {
            e.printStackTrace();
            haltWithError(400, "Could not update branding url", e);
        }
        return null;
    }

    /**
     * Create or update entity. Update depends on existence of ID param, which should only be supplied by the PUT method.
     */
    private String createOrUpdate(Request req, Response res) {
        // Check if an update or create operation depending on presence of id param
        // This needs to be final because it is used in a lambda operation below.
        if (req.params("id") == null && req.requestMethod().equals("PUT")) {
            haltWithError(400, "Must provide id");
        }
        final boolean isCreating = req.params("id") == null;
        String namespace = getNamespaceFromRequest(req);
        Integer id = getIdFromRequest(req);
        // Get the JsonObject
        JdbcTableWriter tableWriter = new JdbcTableWriter(table, datasource, namespace);
        try {
            if (isCreating) {
                return tableWriter.create(req.body());
            } else {
                return tableWriter.update(id, req.body());
            }
        } catch (Exception e) {
            e.printStackTrace();
            haltWithError(400, "Operation failed.", e);
        }
        return null;
    }

    /**
     * Get the namespace for the feed ID found in the request
     */
    private String getNamespaceFromRequest(Request req) {
        String feedId = req.queryParams("feedId");
        FeedSource feedSource = Persistence.feedSources.getById(feedId);
        if (feedSource == null) {
            haltWithError(400, "Feed ID is invalid");
        }
        String namespace = feedSource.editorNamespace;
        if (namespace == null) {
            haltWithError(400, "Cannot edit feed that has not been snapshotted (namespace is null).");
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
                haltWithError(400, "ID provided is not a number");
            }
        }
        return id;
    }

    // TODO add hooks
    abstract void getEntityHook(T entity);
    abstract void createEntityHook(T entity);
    abstract void updateEntityHook(T entity);
    abstract void deleteEntityHook(T entity);


    /**
     * Used to map SQL query ResultSet to JSON string response.
     */
    private class ResultMapper {
        private final ObjectMapper objectMapper;

        ResultMapper () {
            SimpleModule module = new SimpleModule();
            module.addSerializer(new ResultSetSerializer());
            objectMapper = new ObjectMapper();
            objectMapper.registerModule(module);
        }

        // FIXME: does not filter with where clause on ID
        String getOne(Request req, Response res) {
            String namespace = getNamespaceFromRequest(req);
            Integer id = Integer.valueOf(req.params("id"));
            Connection connection = null;
            try {
                connection = datasource.getConnection();
                // FIXME: add where clause for single entity
                PreparedStatement statement =
                        connection.prepareStatement(table.generateSelectSql(namespace));
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

        String getAll(Request req, Response res) {
            String namespace = getNamespaceFromRequest(req);
            Connection connection;
            try {
                connection = datasource.getConnection();
                PreparedStatement statement =
                        connection.prepareStatement(table.generateSelectSql(namespace));
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
        public void serialize(ResultSet rs, JsonGenerator jsonGenerator, SerializerProvider provider) throws IOException {

            try {
                ResultSetMetaData resultSetMetaData = rs.getMetaData();
                int numColumns = resultSetMetaData.getColumnCount();
                String[] columnNames = new String[numColumns];
                int[] columnTypes = new int[numColumns];

                for (int i = 0; i < columnNames.length; i++) {
                    columnNames[i] = resultSetMetaData.getColumnLabel(i + 1);
                    columnTypes[i] = resultSetMetaData.getColumnType(i + 1);
                }

                jsonGenerator.writeStartArray();

                while (rs.next()) {

                    boolean b;
                    long l;
                    double d;

                    jsonGenerator.writeStartObject();

                    for (int i = 0; i < columnNames.length; i++) {

                        jsonGenerator.writeFieldName(columnNames[i]);
                        switch (columnTypes[i]) {

                            case Types.INTEGER:
                                l = rs.getInt(i + 1);
                                if (rs.wasNull()) {
                                    jsonGenerator.writeNull();
                                } else {
                                    jsonGenerator.writeNumber(l);
                                }
                                break;

                            case Types.BIGINT:
                                l = rs.getLong(i + 1);
                                if (rs.wasNull()) {
                                    jsonGenerator.writeNull();
                                } else {
                                    jsonGenerator.writeNumber(l);
                                }
                                break;

                            case Types.DECIMAL:
                            case Types.NUMERIC:
                                jsonGenerator.writeNumber(rs.getBigDecimal(i + 1));
                                break;

                            case Types.FLOAT:
                            case Types.REAL:
                            case Types.DOUBLE:
                                d = rs.getDouble(i + 1);
                                if (rs.wasNull()) {
                                    jsonGenerator.writeNull();
                                } else {
                                    jsonGenerator.writeNumber(d);
                                }
                                break;

                            case Types.NVARCHAR:
                            case Types.VARCHAR:
                            case Types.LONGNVARCHAR:
                            case Types.LONGVARCHAR:
                                jsonGenerator.writeString(rs.getString(i + 1));
                                break;

                            case Types.BOOLEAN:
                            case Types.BIT:
                                b = rs.getBoolean(i + 1);
                                if (rs.wasNull()) {
                                    jsonGenerator.writeNull();
                                } else {
                                    jsonGenerator.writeBoolean(b);
                                }
                                break;

                            case Types.BINARY:
                            case Types.VARBINARY:
                            case Types.LONGVARBINARY:
                                jsonGenerator.writeBinary(rs.getBytes(i + 1));
                                break;

                            case Types.TINYINT:
                            case Types.SMALLINT:
                                l = rs.getShort(i + 1);
                                if (rs.wasNull()) {
                                    jsonGenerator.writeNull();
                                } else {
                                    jsonGenerator.writeNumber(l);
                                }
                                break;

                            case Types.DATE:
                                provider.defaultSerializeDateValue(rs.getDate(i + 1), jsonGenerator);
                                break;

                            case Types.TIMESTAMP:
                                provider.defaultSerializeDateValue(rs.getTime(i + 1), jsonGenerator);
                                break;

                            case Types.BLOB:
                                Blob blob = rs.getBlob(i);
                                provider.defaultSerializeValue(blob.getBinaryStream(), jsonGenerator);
                                blob.free();
                                break;

                            case Types.CLOB:
                                Clob clob = rs.getClob(i);
                                provider.defaultSerializeValue(clob.getCharacterStream(), jsonGenerator);
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
                                provider.defaultSerializeValue(rs.getObject(i + 1), jsonGenerator);
                                break;
                        }
                    }

                    jsonGenerator.writeEndObject();
                }

                jsonGenerator.writeEndArray();

            } catch (SQLException e) {
                throw new IOException(e);
            }
        }
    }

}
