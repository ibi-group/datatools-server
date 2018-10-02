package com.conveyal.gtfs;

import com.conveyal.gtfs.graphql.GTFSGraphQL;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.conveyal.datatools.common.utils.SparkUtils.haltWithMessage;
import static com.conveyal.datatools.manager.DataManager.GTFS_DATA_SOURCE;

/**
 * This Spark Controller contains methods to provide HTTP responses to GraphQL queries, including a query for the
 * GraphQL schema.
 */
public class GraphQLController {
    private static final Logger LOG = LoggerFactory.getLogger(GraphQLController.class);

    /**
     * A Spark Controller that responds to a GraphQL query in HTTP GET query parameters.
     */
    public static Map<String, Object> get (Request request, Response response) {
        JsonNode varsJson = null;
        try {
            varsJson = GraphQLMain.mapper.readTree(request.queryParams("variables"));
        } catch (IOException e) {
            LOG.warn("Error processing variables", e);
            haltWithMessage(400, "Malformed JSON");
        }
        String queryJson = request.queryParams("query");
        return doQuery(varsJson, queryJson, response);
    }

    /**
     * A Spark Controller that responds to a GraphQL query in an HTTP POST body.
     */
    public static Map<String, Object> post (Request req, Response response) {
        JsonNode node = null;
        try {
            node = GraphQLMain.mapper.readTree(req.body());
        } catch (IOException e) {
            LOG.warn("Error processing POST body JSON", e);
            haltWithMessage(400, "Malformed JSON");
        }
        JsonNode vars = node.get("variables");
        String query = node.get("query").asText();
        return doQuery(vars, query, response);
    }

    /**
     * Execute a GraphQL query and return result that fully complies with the GraphQL specification.
     */
    private static Map<String, Object> doQuery (JsonNode varsJson, String queryJson, Response response) {
        long startTime = System.currentTimeMillis();
        if (varsJson == null && queryJson == null) {
            return getSchema(null, null);
        }
        // The graphiql app sends over this unparseable string while doing an introspection query.  Therefore this code
        // checks for it and sends an empty map in that case.
        Map<String, Object> variables = varsJson.toString().equals("\"{}\"")
            ? new HashMap<>()
            : GraphQLMain.mapper.convertValue(varsJson, Map.class);
        Map<String, Object> result = GTFSGraphQL.run(
            GTFS_DATA_SOURCE,
            queryJson,
            variables
        );
        long endTime = System.currentTimeMillis();
        LOG.info("Query took {} msec", endTime - startTime);
        return result;
    }


    /**
     * A Spark Controller that returns the GraphQL schema.
     */
    static Map<String, Object> getSchema(Request req, Response res) {
        return GTFSGraphQL.introspectedSchema;
    }


}
