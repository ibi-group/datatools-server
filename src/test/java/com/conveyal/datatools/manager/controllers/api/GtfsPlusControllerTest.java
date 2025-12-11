package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import io.restassured.response.Response;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

import static com.conveyal.datatools.TestUtils.createFeedVersionFromGtfsZip;
import static com.conveyal.datatools.manager.auth.Auth0Users.USERS_API_PATH;
import static com.conveyal.datatools.manager.controllers.api.UserController.TEST_AUTH0_DOMAIN;
import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;

class GtfsPlusControllerTest extends UnitTest {
    private static final Logger LOG = LoggerFactory.getLogger(GtfsPlusControllerTest.class);
    private static Project project;
    private static FeedSource feedSource;
    private static FeedVersion feedVersion;

    /**
     * Prepare and start a testing-specific web server
     */
    @BeforeAll
    static void setUp() throws Exception {
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

        feedVersion = createFeedVersionFromGtfsZip(feedSource, "bart_old.zip");

        LOG.info("{} setup completed in {} ms", GtfsPlusControllerTest.class.getSimpleName(), System.currentTimeMillis() - startTime);
    }

    @AfterAll
    static void tearDown() {
        project.delete();
        feedSource.delete();
    }

    /**
     * Make sure the gtfsplus endpoint can retrieve files.
     */
    @Test
    void canRetrieveAndPublishGtfsPlusFile() {
        feedVersion.persistFeedVersionAfterValidation(true);
        LOG.info("Making gtfsplus file request");
        Response downloadResponse = gtfsPlusFileRequest(feedVersion.id);
        assertEquals(200, downloadResponse.getStatusCode());

        // Upload (same file should be ok)
        LOG.info("Making gtfsplus upload request");
        Response uploadResponse = uploadGtfsPlusFile(feedVersion.id, downloadResponse.asByteArray());
        assertEquals(200, uploadResponse.getStatusCode());

        // Attempt to publish file
        LOG.info("Making gtfsplus publish request");
        Response publishResponse = gtfsPlusPublishRequest(feedVersion.id);
        assertEquals(200, publishResponse.getStatusCode());
    }

    /**
     * Perform retrieval of an existing GTFS+ file on the feed source ID.
     */
    private static Response gtfsPlusFileRequest(String versionId) {
        String path = String.format("/api/manager/secure/gtfsplus/%s", versionId);
        return given()
            .port(DataManager.PORT)
            .get(path)
            .then()
            .extract()
            .response();
    }

    /**
     * Perform retrieval of an existing GTFS+ file on the feed source ID.
     */
    private static Response uploadGtfsPlusFile(String versionId, byte[] bytes) {
        String path = String.format("/api/manager/secure/gtfsplus/%s", versionId);
        return given()
            .port(DataManager.PORT)
            .body(bytes)
            .post(path)
            .then()
            .extract()
            .response();
    }

    /**
     * Perform retrieval of an existing GTFS+ file on the feed source ID.
     */
    private static Response gtfsPlusPublishRequest(String versionId) {
        String path = String.format("/api/manager/secure/gtfsplus/%s/publish", versionId);
        return given()
            .port(DataManager.PORT)
            .post(path)
            .then()
            .extract()
            .response();
    }
}
