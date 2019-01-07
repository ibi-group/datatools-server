package com.conveyal.gtfs;

import com.conveyal.datatools.common.utils.CorsFilter;

import javax.sql.DataSource;

import static spark.Spark.after;

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
    /**
     * @param args to use the local postgres database, jdbc:postgresql://localhost/gtfs
     */
    public static void main (String[] args) {
        String databaseUrl = args[0];
        String apiPrefix = "/";
        if (args.length > 1) {
            apiPrefix = args[1];
        }
        // Initialize HTTP endpoints with new data source.
        GraphQLController.initialize(GTFS.createDataSource(databaseUrl, null, null), apiPrefix);
        // Apply CORS and content encoding header.
        CorsFilter.apply();
        after((request, response) -> response.header("Content-Encoding", "gzip"));
    }

}

