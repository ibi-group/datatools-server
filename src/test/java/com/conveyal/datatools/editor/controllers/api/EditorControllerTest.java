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
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.utils.IOUtils;

import java.io.IOException;
import java.util.Date;

import static com.conveyal.datatools.TestUtils.createFeedVersion;
import static com.conveyal.datatools.manager.auth.Auth0Users.USERS_API_PATH;
import static com.conveyal.datatools.manager.controllers.api.UserController.TEST_AUTH0_DOMAIN;
import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class EditorControllerTest extends UnitTest {
    private static final Logger LOG = LoggerFactory.getLogger(EditorControllerTest.class);
    private static Project project;
    private static FeedVersion feedVersion;
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Prepare and start a testing-specific web server
     */
    @BeforeClass
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
        feedVersion = createFeedVersion(feedSource, "bart_old.zip");
        // Create and run snapshot job
        Snapshot snapshot = new Snapshot("Snapshot of " + feedVersion.name, feedSource.id, feedVersion.namespace);
        snapshot.storeUser(Auth0UserProfile.createTestAdminUser());
        CreateSnapshotJob createSnapshotJob =
            new CreateSnapshotJob(snapshot, true, false, false);
        // Run in current thread so tests do not run until this is complete.
        createSnapshotJob.run();
        LOG.info("{} setup completed in {} ms", EditorControllerTest.class.getSimpleName(), System.currentTimeMillis() - startTime);
    }

    /**
     * Make sure the patch table endpoint can patch routes table.
     */
    @Test
    public void canPatchRoutes() throws IOException {
        LOG.info("Making patch routes request");
        ObjectNode jsonBody = mapper.createObjectNode();
        String newName = "NEW";
        jsonBody.put("route_short_name", newName);
        String response = given()
            .port(DataManager.PORT)
            .body(jsonBody)
            .patch("/api/editor/secure/route?feedId=" + feedVersion.feedSourceId)
            .then()
            .extract()
            .response()
            .asString();
        JsonNode json = mapper.readTree(response);
        int count = json.get("count").asInt();
        // Assert that all six routes contained within BART feed were modified.
        assertThat(count, equalTo(6));
        // Check GraphQL to verify that route_short_name has indeed been updated.
        ObjectNode graphQLBody = mapper.createObjectNode();
        ObjectNode variables = mapper.createObjectNode();
        // Get fresh feed source so that editor namespace was updated after snapshot.
        FeedSource feedSource = Persistence.feedSources.getById(feedVersion.feedSourceId);
        variables.put("namespace", feedSource.editorNamespace);
        graphQLBody.set("variables", variables);
        String query = IOUtils.toString(EditorControllerTest.class.getClassLoader().getResourceAsStream("graphql/routes.txt"));
        graphQLBody.put("query", query);
        String graphQLString = given()
            .port(DataManager.PORT)
            .body(graphQLBody)
            .post("api/manager/secure/gtfs/graphql")
            .then()
            .extract()
            .response()
            .asString();
        JsonNode graphQL = mapper.readTree(graphQLString);
        // Every route should now have the short name defined in the patch JSON body above.
        for (JsonNode route : graphQL.get("data").get("feed").get("routes")) {
            assertThat(route.get("route_short_name").asText(), equalTo(newName));
        }
    }
}
