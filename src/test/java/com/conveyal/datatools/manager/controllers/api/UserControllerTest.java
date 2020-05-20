package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0Users;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import static com.conveyal.datatools.TestUtils.parseJson;
import static com.conveyal.datatools.manager.auth.Auth0Users.USERS_API_PATH;
import static com.conveyal.datatools.manager.auth.Auth0Users.setCachedApiToken;
import static com.conveyal.datatools.manager.controllers.api.UserController.TEST_AUTH0_DOMAIN;
import static com.conveyal.datatools.manager.controllers.api.UserController.TEST_AUTH0_PORT;
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
 * These tests verify that various Auth0 API calls behave as expected. The Auth0 server is mocked in order to return
 * certain responses needed to verify functionality.
 */
public class UserControllerTest extends UnitTest {
    private String emailForExistingAccount = "test-existing-user@test.com";
    private ObjectMapper mapper = new ObjectMapper();

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
        UserController.setBaseUsersUrl("http://" + TEST_AUTH0_DOMAIN + USERS_API_PATH);
    }

    /**
     * To be run before each test to ensure that stubs needed for any request are created.
     */
    @Before
    public void init() {
        // Create wiremock stub for get API access token. Note: this can be overridden for tests needing an invalid
        // access token.
        stubForApiToken("getAccessToken.json");
    }

    /**
     * Reset some Auth0 stuff to non-testing values.
     */
    @AfterClass
    public static void tearDown() {
        UserController.setBaseUsersUrl(UserController.DEFAULT_BASE_USERS_URL);
    }

    /**
     * Make sure the user endpoint can return a list of users
     */
    @Test
    public void canListFirstTenUsers() throws IOException {
        // create wiremock stub for get users endpoint
        stubFor(
            get(urlPathEqualTo(USERS_API_PATH))
                .withQueryParam("page", equalTo("1"))
                .willReturn(
                    aResponse()
                        .withBodyFile("getFirstTenUsersResponse.json")
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
        String newUserEmail = "test-new-user@test.com";

        // create wiremock stub for create users endpoint
        stubFor(
            post(urlPathEqualTo(USERS_API_PATH))
                .withRequestBody(matchingJsonPath("$.email", equalTo(newUserEmail)))
                .willReturn(
                    aResponse()
                        .withBodyFile("createNewUserResponse.json")
                )
        );

        ObjectNode requestJson = getBaseUserObject();
        requestJson.put("email", newUserEmail);

        // make request and parse the json response
        JsonNode createUserResponse = parseJson(
            given()
                .port(4000)
                .body(requestJson)
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
            post(urlPathEqualTo(USERS_API_PATH))
                .withRequestBody(matchingJsonPath("$.email", equalTo(emailForExistingAccount)))
                .willReturn(
                    aResponse()
                        .withStatus(409)
                        .withBodyFile("createExistingUserResponse.json")
                )
        );

        // make request and parse the json response
        JsonNode createUserResponse = parseJson(
            given()
                .port(DataManager.PORT)
                .body(getBaseUserObject())
                .post(DataManager.API_PREFIX + "secure/user")
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
            patch(urlPathEqualTo(USERS_API_PATH + "/auth0%7Ctest-existing-user"))
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

        // Create wiremock stub for get user by id endpoint.
        stubFor(
            get(urlPathEqualTo(USERS_API_PATH + "/auth0%7Ctest-existing-user"))
                .willReturn(
                    aResponse()
                        .withBodyFile("getExistingUserResponse.json")
                )
        );

        ObjectNode requestJson = mapper.createObjectNode();
        requestJson.put("email", emailForExistingAccount);

        ObjectNode testClientPermissions = mapper.createObjectNode();
        testClientPermissions.put("type", "administer-application");

        ObjectNode testClientData = mapper.createObjectNode();
        testClientData.putArray("permissions").add(testClientPermissions);
        testClientData.putArray("projects");
        testClientData.putArray("organizations");
        testClientData.put("client_id", "testing-client-id");

        requestJson.putArray("data").add(testClientData);

        // make request and parse the json response
        JsonNode updateUserResponse = parseJson(
            given()
                .port(4000)
                .body(requestJson)
                .put("/api/manager/secure/user/auth0|test-existing-user")
            .then()
                .extract()
                .response()
                .asString()
        );

        // make sure the response matches the saved snapshot
        assertThat(updateUserResponse, matchesSnapshot());
    }

    /**
     * Make sure a user can be deleted
     */
    @Test
    public void canDeleteUser() throws IOException {
        // create wiremock stub for the delete users endpoint
        stubFor(
            delete(urlPathEqualTo(USERS_API_PATH + "/auth0%7Ctest-existing-user"))
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

    /**
     * Ensure that a user update fails when the access token has a invalid/null scope, which ultimately results in a
     * null API token response from {@link Auth0Users#getApiToken()}.
     */
    @Test
    public void updateUserFailsWhenApiTokenInvalid() throws IOException {
        // Clear cached token and set up wiremock stub for invalid API token.
        setCachedApiToken(null);
        stubForApiToken("getAccessTokenWithInvalidScope.json");

        // create wiremock stub for update users endpoint
        stubFor(
            patch(urlPathEqualTo(USERS_API_PATH + "/auth0%7Ctest-existing-user"))
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

        // Create wiremock stub for get user by id endpoint.
        stubFor(
            get(urlPathEqualTo(USERS_API_PATH + "/auth0%7Ctest-existing-user"))
                .willReturn(
                    aResponse()
                        .withBodyFile("getExistingUserResponse.json")
                )
        );

        ObjectNode requestJson = mapper.createObjectNode();
        requestJson.put("email", emailForExistingAccount);

        ObjectNode testClientPermissions = mapper.createObjectNode();
        testClientPermissions.put("type", "administer-application");

        ObjectNode testClientData = mapper.createObjectNode();
        testClientData.putArray("permissions").add(testClientPermissions);
        testClientData.putArray("projects");
        testClientData.putArray("organizations");
        testClientData.put("client_id", "testing-client-id");

        requestJson.putArray("data").add(testClientData);

        // make request and parse the json response
        JsonNode updateUserResponse = parseJson(
            given()
                .port(4000)
                .body(requestJson)
                .put("/api/manager/secure/user/auth0|test-existing-user")
                .then()
                .extract()
                .response()
                .asString()
        );
        // Assert that update response matches snapshot (failure message).
        assertThat(updateUserResponse, matchesSnapshot());
        // Clear API token to avoid interfering with future requests.
        setCachedApiToken(null);
    }

    private static void stubForApiToken(String bodyFile) {
        stubFor(
            post(urlPathEqualTo("/oauth/token"))
                .willReturn(
                    aResponse()
                        .withBodyFile(bodyFile)
                )
        );
    }

    /**
     * create a request body of a user that the above stub will recognize as an existing user
     */
    private ObjectNode getBaseUserObject() {
        ObjectNode requestJson = mapper.createObjectNode();
        requestJson.put("email", emailForExistingAccount);
        requestJson.put("password", "password");
        requestJson.putObject("permissions");
        return requestJson;
    }
}
