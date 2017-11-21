package com.conveyal.gtfs;

import com.conveyal.gtfs.graphql.GTFSGraphQL;
import spark.ResponseTransformer;

import javax.sql.DataSource;

import static spark.Spark.get;
import static spark.Spark.post;

/**
 * Test main method to set up a new-style (as of June 2017) GraphQL API
 *
 * What we're trying to provide is this:
 * The queries that analysis-ui makes are at https://github.com/conveyal/analysis-ui/blob/dev/lib/graphql/query.js ; note that feeds are wrapped in bundles in analysis-ui (we wrap the GTFS API types)
 * GraphQL queries for datatools-ui are at https://github.com/catalogueglobal/datatools-ui/blob/dev/lib/gtfs/util/graphql.js.
 *
 * We will eventually want to replace some of the REST-ish endpoints in datatools-ui, including:
 * stops/routes by bounding box
 * stop/routes by text string search (route_long_name/route_short_name, stop_name/stop_id/stop_code)
 * Feeds - to get a list of the feed_ids that have been loaded into the gtfs-api
 *
 * Here are some sample database URLs
 * H2_FILE_URL = "jdbc:h2:file:~/test-db"; // H2 memory does not seem faster than file
 * SQLITE_FILE_URL = "jdbc:sqlite:/Users/abyrd/test-db";
 * POSTGRES_LOCAL_URL = "jdbc:postgresql://localhost/catalogue";
 */
public class GraphQLMain {

    private static final ResponseTransformer JSON_TRANSFORMER = new JsonTransformer();

    public static final DataSource dataSource = null; // deprecated, just here to make this project compile.

    /**
     * @param args to use the local postgres database, jdbc:postgresql://localhost/gtfs
     */
    public static void main (String[] args) {
        String databaseUrl = args[0];
        String apiPrefix = "/";
        if (args.length > 1) {
            apiPrefix = args[1];
        }
        DataSource dataSource = GTFS.createDataSource(databaseUrl, null, null);
        initialize(dataSource, apiPrefix);
        CorsFilter.apply();
    }

    /**
     * DataSource created with GTFS::createDataSource (see main() for example)
     * API prefix should begin and end with "/", e.g. "/api/"
     */
    public static void initialize (DataSource dataSource, String apiPrefix) {
        GTFSGraphQL.initialize(dataSource);
        // Can we just pass in reference objectMapper::writeValueAsString?
        // Why does jsonTransformer have mix-ins?
        get(apiPrefix + "graphql", GraphQLController::get, JSON_TRANSFORMER);
        post(apiPrefix + "graphql", GraphQLController::post, JSON_TRANSFORMER);
        get(apiPrefix + "graphql/schema", GraphQLController::getSchema, JSON_TRANSFORMER);
        post(apiPrefix + "graphql/schema", GraphQLController::getSchema, JSON_TRANSFORMER);
    }

}

