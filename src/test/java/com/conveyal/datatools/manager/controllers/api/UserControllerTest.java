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
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.patch;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static com.zenika.snapshotmatcher.SnapshotMatcher.matchesSnapshot;
import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * These tests verify the
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
     * Make sure the user endpoint can return a list of users
     */
    @Test
    public void canListFirstTenUsers() throws IOException {
        // create wiremock stub for get users endpoint
        stubFor(
            get(urlPathEqualTo("/api/v2/users"))
                .withQueryParam("page", equalTo("1"))
                .willReturn(
                    aResponse()
                        .withBodyFile("getFirstTenUsers.json")
                )
        );


        // make request and parse the json response
        JsonNode userResponse = parseJson(
            given()
                .port(4000)
                .get("/api/manager/secure/user?page=1")
            .then()
                .extract()
                .response()
                .asString()
        );

        // make sure the response matches the saved snapshot
        assertThat(userResponse, matchesSnapshot());
    }

    /**
     * Make sure a user can be created
     */
    @Test
    public void canCreateUser() throws IOException {
        // create wiremock stub for create users endpoint
        stubFor(
            post(urlPathEqualTo("/api/v2/users"))
                .withRequestBody(matchingJsonPath("$.email", equalTo("test-new-user@test.com")))
                .willReturn(
                    aResponse()
                        .withBodyFile("createNewUserResponse.json")
                )
        );


        // make request and parse the json response
        JsonNode createUserResponse = parseJson(
            given()
                .port(4000)
                .body("{\n" +
                    "  \"email\" : \"test-new-user@test.com\",\n" +
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

    /**
     * Make sure a user can be updated
     */
    @Test
    public void canUpdateUser() throws IOException {
        // create wiremock stub for update users endpoint
        stubFor(
            patch(urlPathEqualTo("/api/v2/users/auth0%7Ctest-existing-user"))
                .withRequestBody(
                    matchingJsonPath(
                        "$.app_metadata.datatools[0].permissions[0].type",
                        equalTo("administer-application")
                    )
                )
                .willReturn(
                    aResponse()
                        .withBodyFile("updateExistingUserResponse.json")
                )
        );

        // create wiremock stub for get user by id endpoint
        stubFor(
            get(urlPathEqualTo("/api/v2/users/auth0%7Ctest-existing-user"))
                .willReturn(
                    aResponse()
                        .withBodyFile("getExistingUserResponse.json")
                )
        );


        // make request and parse the json response
        JsonNode createUserResponse = parseJson(
            given()
                .port(4000)
                .body("{" +
                    "\"user_id\": \"auth0|test-existing-user\"" +
                    ",\"data\": [{" +
                    "\"permissions\": [{\"type\": \"administer-application\"}]," +
                    "\"projects\": []," +
                    "\"organizations\": []," +
                    "\"client_id\":\"testing-client-id\"" +
                    "}]" +
                    "}")
                .put("/api/manager/secure/user/auth0|test-existing-user")
            .then()
                .extract()
                .response()
                .asString()
        );

        // make sure the response matches the saved snapshot
        assertThat(createUserResponse, matchesSnapshot());
    }

    /**
     * Make sure a user can be deleted
     */
    @Test
    public void canDeleteUser() throws IOException {
        // create wiremock stub for the delate users endpoint
        stubFor(
            delete(urlPathEqualTo("/api/v2/users/auth0%7Ctest-existing-user"))
                .willReturn(aResponse())
        );


        // make request and parse the json response
        JsonNode deleteUserResponse = parseJson(
            given()
                .port(4000)
                .delete("/api/manager/secure/user/auth0|test-existing-user")
            .then()
                .extract()
                .response()
                .asString()
        );

        // make sure the response matches the saved snapshot
        assertThat(deleteUserResponse, matchesSnapshot());
    }
}
