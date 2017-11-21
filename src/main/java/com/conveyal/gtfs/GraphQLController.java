package com.conveyal.gtfs;

import com.conveyal.gtfs.graphql.GTFSGraphQL;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.ExecutionResult;
import graphql.GraphQLError;
import graphql.introspection.IntrospectionQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static spark.Spark.halt;

/**
 * This Spark Controller contains methods to provide HTTP responses to GraphQL queries, including a query for the
 * GraphQL schema.
 */
public class GraphQLController {
    // todo shared objectmapper
    private static final ObjectMapper mapper = new ObjectMapper();

    private static final Logger LOG = LoggerFactory.getLogger(com.conveyal.gtfs.GraphQLController.class);

    /**
     * A Spark Controller that responds to a GraphQL query in HTTP GET query parameters.
     */
    public static Object get (Request request, Response response) {
        String varsJson = request.queryParams("variables");
        String queryJson = request.queryParams("query");
        return doQuery(varsJson, queryJson, response);
    }

    /**
     * A Spark Controller that responds to a GraphQL query in an HTTP POST body.
     */
    public static Object post (Request req, Response response) {
        JsonNode node = null;
        try {
            node = mapper.readTree(req.body());
        } catch (IOException e) {
            LOG.warn("Error processing POST body JSON", e);
            halt(400, "Malformed JSON");
        }
        // FIXME converting String to JSON nodes and back to string, then re-parsing to Map.
        String vars = node.get("variables").asText();
        String query = node.get("query").asText();
        return doQuery(vars, query, response);
    }

    private static Object doQuery (String varsJson, String queryJson, Response response) {
        long startTime = System.currentTimeMillis();
        if (varsJson == null && queryJson == null) {
            return GTFSGraphQL.getGraphQl().execute(IntrospectionQuery.INTROSPECTION_QUERY).getData();
        }
        try {
            Map<String, Object> variables = mapper.readValue(varsJson, new TypeReference<Map<String, Object>>(){});
            ExecutionResult result = GTFSGraphQL.getGraphQl().execute(queryJson, null, null, variables);
            List<GraphQLError> errs = result.getErrors();
            if (!errs.isEmpty()) {
                response.status(400);
                return errs;
            } else {
                long endTime = System.currentTimeMillis();
                LOG.info("Query took {} msec", endTime - startTime);
                return result.getData();
            }
        } catch (IOException e) {
            LOG.warn("Error processing variable JSON", e);
            halt(404, "Malformed JSON");
        }
        return null;
    }


    /**
     * A Spark Controller that returns the GraphQL schema.
     */
    public static Object getSchema (Request req, Response res) {
        return GTFSGraphQL.getGraphQl().execute(IntrospectionQuery.INTROSPECTION_QUERY).getData();
    }


}
