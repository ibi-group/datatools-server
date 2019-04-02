package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.manager.DataManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import static com.conveyal.datatools.TestUtils.parseJson;
import static com.conveyal.datatools.manager.controllers.api.UserController.TEST_AUTH0_DOMAIN;
import static com.conveyal.datatools.manager.controllers.api.UserController.TEST_AUTH0_PORT;
import static com.conveyal.datatools.manager.controllers.api.UserController.USERS_PATH;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static com.zenika.snapshotmatcher.SnapshotMatcher.matchesSnapshot;
import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * These tests verify that various Auth0 API calls behave as expected. The Auth0 server is mocked in order to return
 * certain responses needed to verify functionality.
 */
public class UserControllerTest {
    /**
     * This sets up a mock server that accepts requests and sends predefined responses to mock an Auth0 server.
     */
    @Rule
    public WireMockRule wireMockRule = new WireMockRule(
        options()
            .port(TEST_AUTH0_PORT)
            .usingFilesUnderDirectory("src/test/resources/com/conveyal/datatools/auth0-mock-responses/")
    );

    /**
     * Prepare and start a testing-specific web server
     */
    @BeforeClass
    public static void setUp() throws Exception {
        // start server if it isn't already running
        DatatoolsTest.setUp();
        // Set users URL to test domain used by wiremock.
        UserController.setBaseUsersUrl("http://" + TEST_AUTH0_DOMAIN + USERS_PATH);
    }

    /**
     * Reset some Auth0 stuff to non-testing values.
     */
    @AfterClass
    public static void tearDown() {
        UserController.setBaseUsersUrl(UserController.DEFAULT_BASE_USERS_URL);
    }

    /**
     * Make sure a meaningful Auth0 error can be returned when a duplicate user is being created
     */
    @Test
    public void canReturnMeaningfulAuth0Error() throws IOException {
        String emailForExistingAccount = "test-existing-user@test.com";

        // create wiremock stub for create users endpoint that responds with a message saying a user with the email
        // already exists
        stubFor(
            post(urlPathEqualTo(USERS_PATH))
                .withRequestBody(matchingJsonPath("$.email", equalTo(emailForExistingAccount)))
                .willReturn(
                    aResponse()
                        .withStatus(409)
                        .withBodyFile("createExistingUserResponse.json")
                )
        );


        // create a request body of a user that the above stub will recognize as an existing user
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode requestJson = mapper.createObjectNode();
        requestJson.put("email", emailForExistingAccount);
        requestJson.put("password", "password");
        requestJson.putObject("permissions");

        // make request and parse the json response
        JsonNode createUserResponse = parseJson(
            given()
                .port(DataManager.PORT)
                .body(requestJson)
                .post(DataManager.API_PREFIX + "secure/user")
            .then()
                .extract()
                .response()
                .asString()
        );

        // make sure the response matches the saved snapshot
        assertThat(createUserResponse, matchesSnapshot());
    }
}
