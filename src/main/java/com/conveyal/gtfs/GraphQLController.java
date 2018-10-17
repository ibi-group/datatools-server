package com.conveyal.gtfs;

import com.conveyal.gtfs.graphql.GTFSGraphQL;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.conveyal.datatools.common.utils.SparkUtils.haltWithMessage;
import static spark.Spark.get;
import static spark.Spark.post;

/**
 * This Spark Controller contains methods to provide HTTP responses to GraphQL queries, including a query for the
 * GraphQL schema.
 */
public class GraphQLController {
    private static final Logger LOG = LoggerFactory.getLogger(GraphQLController.class);
    // Shared object mapper with GraphQLController.
    private static final ObjectMapper mapper = new ObjectMapper();
    private static DataSource GRAPHQL_DATA_SOURCE;

    /**
     * A Spark Controller that responds to a GraphQL query in HTTP GET query parameters.
     */
    public static Map<String, Object> getGraphQL (Request request, Response response) {
        JsonNode varsJson = null;
        try {
            varsJson = mapper.readTree(request.queryParams("variables"));
        } catch (IOException e) {
            LOG.warn("Error processing variables", e);
            haltWithMessage(request, 400, "Malformed JSON");
        }
        String queryJson = request.queryParams("query");
        return doQuery(varsJson, queryJson, response);
    }

    /**
     * A Spark Controller that responds to a GraphQL query in an HTTP POST body.
     */
    public static Map<String, Object> postGraphQL (Request req, Response response) {
        JsonNode node = null;
        try {
            node = mapper.readTree(req.body());
        } catch (IOException e) {
            LOG.warn("Error processing POST body JSON", e);
            haltWithMessage(req, 400, "Malformed JSON");
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
        Map<String, Object> variables = varsJson == null || varsJson.toString().equals("\"{}\"")
            ? new HashMap<>()
            : mapper.convertValue(varsJson, Map.class);
        Map<String, Object> result = GTFSGraphQL.run(
            GRAPHQL_DATA_SOURCE,
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


    /**
     * Register Spark HTTP endpoints. API prefix should begin and end with "/", e.g. "/api/".
     */
    public static void initialize (DataSource dataSource, String apiPrefix) {
        LOG.info("Initialized GTFS GraphQL API at localhost:port{}", apiPrefix);
        if (dataSource == null) {
            throw new RuntimeException("Cannot initialize GraphQL endpoints. Data source must not be null.");
        }
        GRAPHQL_DATA_SOURCE = dataSource;
        get(apiPrefix + "graphql", GraphQLController::getGraphQL, mapper::writeValueAsString);
        post(apiPrefix + "graphql", GraphQLController::postGraphQL, mapper::writeValueAsString);
        get(apiPrefix + "graphql/schema", GraphQLController::getSchema, mapper::writeValueAsString);
        post(apiPrefix + "graphql/schema", GraphQLController::getSchema, mapper::writeValueAsString);
    }
}
