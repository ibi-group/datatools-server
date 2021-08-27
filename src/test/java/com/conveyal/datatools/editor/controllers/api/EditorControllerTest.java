package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.editor.jobs.CreateSnapshotJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.controllers.api.UserController;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.models.Snapshot;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.utils.IOUtils;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Date;

import static com.conveyal.datatools.TestUtils.createFeedVersionFromGtfsZip;
import static com.conveyal.datatools.manager.auth.Auth0Users.USERS_API_PATH;
import static com.conveyal.datatools.manager.controllers.api.UserController.TEST_AUTH0_DOMAIN;
import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class EditorControllerTest extends UnitTest {
    private static final Logger LOG = LoggerFactory.getLogger(EditorControllerTest.class);
    private static Project project;
    private static FeedVersion feedVersion;
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Prepare and start a testing-specific web server
     */
    @BeforeAll
    public static void setUp() throws Exception {
        long startTime = System.currentTimeMillis();
        // start server if it isn't already running
        DatatoolsTest.setUp();
        // Set users URL to test domain used by wiremock.
        UserController.setBaseUsersUrl("http://" + TEST_AUTH0_DOMAIN + USERS_API_PATH);
        // Create a project, feed sources, and feed versions to merge.
        project = new Project();
        project.name = String.format("Test %s", new Date().toString());
        Persistence.projects.create(project);
        FeedSource feedSource = new FeedSource("BART");
        feedSource.projectId = project.id;
        Persistence.feedSources.create(feedSource);
        feedVersion = createFeedVersionFromGtfsZip(feedSource, "bart_old.zip");
        // Create and run snapshot job
        Snapshot snapshot = new Snapshot("Snapshot of " + feedVersion.name, feedSource.id, feedVersion.namespace);
        CreateSnapshotJob createSnapshotJob =
            new CreateSnapshotJob(Auth0UserProfile.createTestAdminUser(), snapshot, true, false, false);
        // Run in current thread so tests do not run until this is complete.
        createSnapshotJob.run();
        LOG.info("{} setup completed in {} ms", EditorControllerTest.class.getSimpleName(), System.currentTimeMillis() - startTime);
    }

    /**
     * Creates a new route in a new feed source, then creates a snapshot and checks that the route id was
     * exported correctly.
     * @throws IOException
     */
    @Test
    public void canSnapshotNewRouteAndSeeId()  throws IOException {
        // Create new feedsource
        FeedSource feedSource = new FeedSource("new_test_feedsource");
        feedSource.projectId = project.id;
        Persistence.feedSources.create(feedSource);

        Snapshot snapshot = new Snapshot("Snapshot of " + feedSource.name, feedSource.id, feedSource.editorNamespace);
        CreateSnapshotJob createSnapshotJob =
                new CreateSnapshotJob(Auth0UserProfile.createTestAdminUser(), snapshot, true, false, false);
        // Run in current thread so tests do not run until this is complete.
        createSnapshotJob.run();


        // Add a route
        ObjectNode jsonBody = mapper.createObjectNode();
        String field = "route_id";
        String value = "999999";

        jsonBody.put(field, value);
        jsonBody.put("route_color", "333333");
        jsonBody.put("route_text_color", "FFFFFF");
        jsonBody.put("route_type", "3");
        jsonBody.put("route_short_name", "test_route");
        jsonBody.put("route_long_name", "teeeeeeeeeeeeeeest_route");
        jsonBody.put("route_desc", "this route should have an id");
        jsonBody.put("route_url", (String) null);
        jsonBody.put("route_branding_url", (String) null);
        jsonBody.put("publicly_visible", "1");
        jsonBody.put("wheelchair_accessible", (String) null);
        jsonBody.put("route_sort_order", (BigInteger) null);
        jsonBody.put("status", "2");
        jsonBody.put("agency_id", (String) null);
        JsonNode responseJson = postTableRequest("route", feedSource.id, null, jsonBody);
        String returnedRouteId = responseJson.get("route_id").asText();
        assertThat(returnedRouteId, equalTo(value));

        Snapshot secondSnapshot = new Snapshot("Snapshot of " + feedSource.name, feedSource.id, feedSource.editorNamespace);
        CreateSnapshotJob createSecondSnapshotJob =
                new CreateSnapshotJob(Auth0UserProfile.createTestAdminUser(), secondSnapshot, true, true, false);
        // Run in current thread so tests do not run until this is complete.
        createSecondSnapshotJob.run();

        // Get list of routes
        JsonNode graphQL = graphqlQuery(snapshot.namespace, "graphql/routes.txt");
        JsonNode routes = graphQL.get("data").get("feed").get("routes");
        assertThat(routes.size(), equalTo(1));
        for (JsonNode route : graphQL.get("data").get("feed").get("routes")) {
            assertThat(route.get(field).asText(), equalTo(value));
        }

        // FIXME: this should return the new route?
        JsonNode secondGraphQL = graphqlQuery(secondSnapshot.namespace, "graphql/routes.txt");
        JsonNode secondRoutes = secondGraphQL.get("data").get("feed").get("routes");

        assertThat(secondRoutes.size(), equalTo(1));
        // Check that the route_id of the new route is correct
        for (JsonNode route : secondGraphQL.get("data").get("feed").get("routes")) {
            assertThat(route.get(field).asText(), equalTo(value));
        }
    }

    /**
     * Make sure the patch table endpoint can patch routes table.
     */
    @Test
    public void canPatchRoutes() throws IOException {
        LOG.info("Making patch routes request");
        ObjectNode jsonBody = mapper.createObjectNode();
        String field = "route_short_name";
        String value = "NEW";
        jsonBody.put(field, value);
        int count = patchTableRequest("route", feedVersion.feedSourceId, null, jsonBody);
        // Assert that all six routes contained within BART feed were modified.
        assertThat(count, equalTo(6));
        // Check GraphQL to verify that route_short_name has indeed been updated.
        // Get fresh feed source so that editor namespace was updated after snapshot.
        FeedSource feedSource = Persistence.feedSources.getById(feedVersion.feedSourceId);
        JsonNode graphQL = graphqlQuery(feedSource.editorNamespace, "graphql/routes.txt");
        // Every route should now have the short name defined in the patch JSON body above.
        for (JsonNode route : graphQL.get("data").get("feed").get("routes")) {
            assertThat(route.get(field).asText(), equalTo(value));
        }
    }

    /**
     * Make sure the patch table endpoint can patch stops conditionally with query.
     */
    @Test
    public void canPatchStopsConditionally() throws IOException {
        LOG.info("Making conditional patch stops request");
        ObjectNode jsonBody = mapper.createObjectNode();
        String field = "stop_desc";
        String value = "Oakland!";
        // This query basically selects all stops that are east of the Bay Bridge (should be 34 in total).
        String queryField = "stop_lon";
        double queryValue = -122.374593;
        String query = String.format("%s=gt.%.6f", queryField, queryValue);
        jsonBody.put(field, value);
        int count = patchTableRequest("stop", feedVersion.feedSourceId, query, jsonBody);
        // Assert that all 34 stops in the East Bay were modified.
        assertThat(count, equalTo(34));
        // Check GraphQL to verify that stop_desc has indeed been updated.
        // Get fresh feed source so that editor namespace was updated after snapshot.
        FeedSource feedSource = Persistence.feedSources.getById(feedVersion.feedSourceId);
        JsonNode graphQL = graphqlQuery(feedSource.editorNamespace, "graphql/stops.txt");
        // Every stop meeting the stop_lon condition should now have the desc defined in the patch JSON body above.
        for (JsonNode stop : graphQL.get("data").get("feed").get("stops")) {
            double filteredValue = stop.get(queryField).asDouble();
            JsonNode patchedValue = stop.get(field);
            if (filteredValue > queryValue) assertThat(patchedValue.asText(), equalTo(value));
            // stop_desc should be null if it does not meet query condition.
            else assertThat(patchedValue.isNull(), is(true));
        }
    }

    /**
     * Perform patch table request on the feed source ID with the requested query and patch JSON. A null query will
     * apply the patch JSON to the entire table.
     */
    private static int patchTableRequest(String entity, String feedId, String query, JsonNode oatchJSON) throws IOException {
        String path = String.format("/api/editor/secure/%s?feedId=%s", entity, feedId);
        if (query != null) path += "&" + query;
        String response = given()
            .port(DataManager.PORT)
            .body(oatchJSON)
            .patch(path)
            .then()
            .extract()
            .response()
            .asString();
        JsonNode json = mapper.readTree(response);
        return json.get("count").asInt();
    }

    /**
     * Creates a new entity and return newly created entity
     */
    private static JsonNode postTableRequest(String entity, String feedId, String query, JsonNode oatchJSON) throws IOException {
        String path = String.format("/api/editor/secure/%s?feedId=%s", entity, feedId);
        if (query != null) path += "&" + query;
        String response = given()
                .port(DataManager.PORT)
                .body(oatchJSON)
                .post(path)
                .then()
                .extract()
                .response()
                .asString();
        return mapper.readTree(response);
    }

    /**
     * Perform basic graphQL query for feed/namespace and query file.
     */
    private static JsonNode graphqlQuery (String namespace, String graphQLQueryFile) throws IOException {
        ObjectNode graphQLBody = mapper.createObjectNode();
        ObjectNode variables = mapper.createObjectNode();
        variables.put("namespace", namespace);
        graphQLBody.set("variables", variables);
        String query = IOUtils.toString(EditorControllerTest.class.getClassLoader().getResourceAsStream(graphQLQueryFile));
        graphQLBody.put("query", query);
        String graphQLString = given()
            .port(DataManager.PORT)
            .body(graphQLBody)
            .post("api/manager/secure/gtfs/graphql")
            .then()
            .extract()
            .response()
            .asString();
        return mapper.readTree(graphQLString);
    }
}
