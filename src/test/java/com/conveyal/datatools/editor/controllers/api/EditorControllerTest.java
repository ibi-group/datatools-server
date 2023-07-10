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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.utils.IOUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;
import java.util.stream.Stream;

import static com.conveyal.datatools.TestUtils.assertThatSqlCountQueryYieldsExpectedCount;
import static com.conveyal.datatools.TestUtils.createFeedVersionFromGtfsZip;
import static com.conveyal.datatools.manager.auth.Auth0Users.USERS_API_PATH;
import static com.conveyal.datatools.manager.controllers.api.UserController.TEST_AUTH0_DOMAIN;
import static io.restassured.RestAssured.given;
import static org.eclipse.jetty.http.HttpStatus.OK_200;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class EditorControllerTest extends UnitTest {
    private static final Logger LOG = LoggerFactory.getLogger(EditorControllerTest.class);
    private static Project project;
    private static FeedSource feedSource;
    private static FeedSource feedSourceCascadeDelete;
    private static FeedVersion feedVersion;
    private static FeedVersion feedVersionCascadeDelete;
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
        project.name = String.format("Test %s", new Date());
        Persistence.projects.create(project);

        feedSource = new FeedSource("BART");
        feedSource.projectId = project.id;
        Persistence.feedSources.create(feedSource);

        feedSourceCascadeDelete = new FeedSource("CASCADE_DELETE");
        feedSourceCascadeDelete.projectId = project.id;
        Persistence.feedSources.create(feedSourceCascadeDelete);

        feedVersion = createFeedVersionFromGtfsZip(feedSource, "bart_old.zip");
        feedVersionCascadeDelete = createFeedVersionFromGtfsZip(feedSourceCascadeDelete, "bart_old.zip");

        // Create and run snapshot jobs
        crateAndRunSnapshotJob(feedVersion.name, feedSource.id, feedVersion.namespace);
        crateAndRunSnapshotJob(feedVersionCascadeDelete.name, feedSourceCascadeDelete.id, feedVersionCascadeDelete.namespace);
        LOG.info("{} setup completed in {} ms", EditorControllerTest.class.getSimpleName(), System.currentTimeMillis() - startTime);
    }

    @AfterAll
    public static void tearDown() {
        project.delete();
        feedSource.delete();
        feedSourceCascadeDelete.delete();
    }

    /**
     * Create and run a snapshot job in the current thread (so tests do not run until this is complete).
     */
    private static void crateAndRunSnapshotJob(String feedVersionName, String feedSourceId, String namespace) {
        Snapshot snapshot = new Snapshot("Snapshot of " + feedVersionName, feedSourceId, namespace);
        CreateSnapshotJob createSnapshotJob =
            new CreateSnapshotJob(Auth0UserProfile.createTestAdminUser(), snapshot, true, false, false);
        createSnapshotJob.run();
    }

    private static Stream<Arguments> createPatchTableTests() {
        return Stream.of(
            Arguments.of("route_short_name", "route", 6, "graphql/routes.txt", "routes"),
            Arguments.of("organization_name", "attribution", 1, "graphql/attributions.txt", "attributions"),
            Arguments.of("translation", "translation", 3, "graphql/translations.txt", "translations")
        );
    }

    /**
     * Confirm that the provided table endpoint can be patched.
     */
    @ParameterizedTest
    @MethodSource("createPatchTableTests")
    public void canPatchTableTests(
        String field,
        String entity,
        int expectedCount,
        String graphQLQueryFile,
        String table
    ) throws IOException {

        LOG.info("Making patch {} request", table);
        String value = "NEW";
        ObjectNode jsonBody = mapper.createObjectNode();
        jsonBody.put(field, value);
        int count = patchTableRequest(entity, feedVersion.feedSourceId, null, jsonBody);
        // Assert that the correct table within the BART feed was modified.
        assertThat(count, equalTo(expectedCount));
        // Get fresh feed source so that editor namespace was updated after snapshot.
        FeedSource freshFeedSource = Persistence.feedSources.getById(feedVersion.feedSourceId);
        JsonNode graphQL = graphqlQuery(freshFeedSource.editorNamespace, graphQLQueryFile);
        for (JsonNode node : graphQL.get("data").get("feed").get(table)) {
            assertThat(node.get(field).asText(), equalTo(value));
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
        FeedSource freshFeedSource = Persistence.feedSources.getById(feedVersion.feedSourceId);
        JsonNode graphQL = graphqlQuery(freshFeedSource.editorNamespace, "graphql/stops.txt");
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
     * Test the removal of a stop and all references in stop times and pattern stops.
     */
    @Test
    void canCascadeDeleteStop() throws IOException, SQLException {
        // Get a fresh feed source so that the editor namespace was updated after snapshot.
        FeedSource freshFeedSource = Persistence.feedSources.getById(feedVersionCascadeDelete.feedSourceId);
        String stopId = "WARM";
        String stopCountSql = getCountSql(freshFeedSource.editorNamespace, "stops", stopId);
        String stopTimesCountSql = getCountSql(freshFeedSource.editorNamespace, "stop_times", stopId);
        String patternStopsCountSql = getCountSql(freshFeedSource.editorNamespace, "pattern_stops", stopId);

        // Check for presence of stopId in stops, stop times and pattern stops.
        assertThatSqlCountQueryYieldsExpectedCount(stopCountSql, 1);
        assertThatSqlCountQueryYieldsExpectedCount(stopTimesCountSql, 522);
        assertThatSqlCountQueryYieldsExpectedCount(patternStopsCountSql, 4);

        String path = String.format(
            "/api/editor/secure/stop/%s/cascadeDeleteStop?feedId=%s&sessionId=test",
            stopId,
            feedVersionCascadeDelete.feedSourceId
        );
        String response = given()
            .port(DataManager.PORT)
            .delete(path)
            .then()
            .extract()
            .response()
            .asString();
        JsonNode json = mapper.readTree(response);
        assertEquals(OK_200, json.get("code").asInt());

        // Check for removal of stopId in stops, stop times and pattern stops.
        assertThatSqlCountQueryYieldsExpectedCount(stopCountSql, 0);
        assertThatSqlCountQueryYieldsExpectedCount(stopTimesCountSql, 0);
        assertThatSqlCountQueryYieldsExpectedCount(patternStopsCountSql, 0);
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

    /**
     * Build a sql statement to provide a count on the number of rows matching the stop id.
     */
    private static String getCountSql(String namespace, String tableName, String stopId) {
        return String.format(
            "SELECT count(*) FROM %s.%s WHERE stop_id = '%s'",
            namespace,
            tableName,
            stopId
        );
    }
}
