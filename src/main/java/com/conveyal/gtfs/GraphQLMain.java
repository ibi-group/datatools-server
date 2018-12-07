package com.conveyal.gtfs;

import com.conveyal.datatools.common.utils.CorsFilter;
import com.conveyal.gtfs.graphql.GTFSGraphQL;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.ResponseTransformer;

import javax.sql.DataSource;

import static spark.Spark.after;
import static spark.Spark.get;
import static spark.Spark.post;

/**
 * Test main method to set up a new-style (as of June 2017) GraphQL API
 *
 * What we're trying to provide is this:
 * The queries that analysis-ui makes are at https://github.com/conveyal/analysis-ui/blob/dev/lib/graphql/query.js ;
 * note that feeds are wrapped in bundles in analysis-ui (we wrap the GTFS API types)
 * GraphQL queries for datatools-ui are at https://github.com/conveyal/datatools-ui/blob/dev/lib/gtfs/util/graphql.js.
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
    // Shared object mapper with GraphQLController.
    protected static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(GraphQLMain.class);

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
        after((request, response) -> response.header("Content-Encoding", "gzip"));
    }

    /**
     * DataSource created with GTFS::createDataSource (see main() for example)
     * API prefix should begin and end with "/", e.g. "/api/"
     */
    public static void initialize (DataSource dataSource, String apiPrefix) {
        LOG.info("Initialized GTFS GraphQL API at localhost:port{}", apiPrefix);
        GTFSGraphQL.initialize(dataSource);
        get(apiPrefix + "graphql", GraphQLController::get, mapper::writeValueAsString);
        post(apiPrefix + "graphql", GraphQLController::post, mapper::writeValueAsString);
        get(apiPrefix + "graphql/schema", GraphQLController::getSchema, mapper::writeValueAsString);
        post(apiPrefix + "graphql/schema", GraphQLController::getSchema, mapper::writeValueAsString);
    }

}

