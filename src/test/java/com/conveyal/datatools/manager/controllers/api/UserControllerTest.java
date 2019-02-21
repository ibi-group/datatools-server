package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.DatatoolsTest;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import static com.conveyal.datatools.TestUtils.parseJson;
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
            .port(8089)
            .usingFilesUnderDirectory("src/test/resources/com/conveyal/datatools/auth0-mock-responses/")
    );

    /**
     * Prepare and start a testing-specific web server
     */
    @BeforeClass
    public static void setUp() {
        // start server if it isn't already running
        DatatoolsTest.setUp();
    }

    /**
     * Make sure a meaningful Auth0 error can be returned when a duplicate user is being created
     */
    @Test
    public void canReturnMeaningfulAuth0Error() throws IOException {
        // create wiremock stub for create users endpoint that responds with a message saying a user with the email
        // already exists
        stubFor(
            post(urlPathEqualTo("/api/v2/users"))
                .withRequestBody(matchingJsonPath("$.email", equalTo("test-existing-user@test.com")))
                .willReturn(
                    aResponse()
                        .withStatus(409)
                        .withBodyFile("createExistingUserResponse.json")
                )
        );


        // make request and parse the json response
        JsonNode createUserResponse = parseJson(
            given()
                .port(4000)
                .body("{\n" +
                    "  \"email\" : \"test-existing-user@test.com\",\n" +
                    "  \"password\" : \"password\",\n" +
                    "  \"permissions\" : {}\n" +
                    "}")
                .post("/api/manager/secure/user")
            .then()
                .extract()
                .response()
                .asString()
        );

        // make sure the response matches the saved snapshot
        assertThat(createUserResponse, matchesSnapshot());
    }
}
