package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0Users;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.stream.Stream;

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
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
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
    private final String emailForExistingAccount = "test-existing-user@test.com";
    private final ObjectMapper mapper = new ObjectMapper();
    private final String USER_SEARCH_API_PATH = USERS_API_PATH + "?sort=email%3A1&per_page=10&page=0&include_totals=false&search_engine=v3&q=email%3A";
    private static WireMockServer wireMockServer;

    /**
     * Prepare and start a testing-specific web server
     */
    @BeforeAll
    public static void setUp() throws Exception {
        // start server if it isn't already running
        DatatoolsTest.setUp();
        // Set users URL to test domain used by wiremock.
        UserController.setBaseUsersUrl("http://" + TEST_AUTH0_DOMAIN + USERS_API_PATH);
        // This sets up a mock server that accepts requests and sends predefined responses to mock an Auth0 server.
        wireMockServer = new WireMockServer(
            options()
                .port(TEST_AUTH0_PORT)
                .usingFilesUnderDirectory("src/test/resources/com/conveyal/datatools/auth0-mock-responses/")
        );
        wireMockServer.start();
    }

    /**
     * To be run before each test to ensure that stubs needed for any request are created.
     */
    @BeforeEach
    public void init() {
        // Create wiremock stub for get API access token. Note: this can be overridden for tests needing an invalid
        // access token.
        stubForApiToken("getAccessToken.json");
    }

    /**
     * Reset some Auth0 stuff to non-testing values.
     */
    @AfterAll
    public static void tearDown() {
        UserController.setBaseUsersUrl(UserController.DEFAULT_BASE_USERS_URL);
        wireMockServer.stop();
    }

    /**
     * Make sure the user endpoint can return a list of users
     */
    @Test
    public void canListFirstTenUsers() throws IOException {
        // create wiremock stub for get users endpoint
        wireMockServer.stubFor(
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
     * When creating a new datatools user for an email address that already exists as an Auth0 user (for a different
     * datatools client/application in Auth0), make sure the permissions for the existing Auth0 users can be updated.
     */
    @Test
    public void canUpdatePermissionsOfExistingAuth0User() throws IOException {
        String newUserEmail = "test-existing-user@test.com";

        // create wiremock stub for user search that matches existing user
        String url = USER_SEARCH_API_PATH + URLEncoder.encode(newUserEmail, "UTF-8") + "*";
        wireMockServer.stubFor(
            get(urlEqualTo(url))
                .willReturn(
                    aResponse()
                        .withBodyFile("userSearchResponse.json")
                )
        );

        // create wiremock stub for update users endpoint
        wireMockServer.stubFor(
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

        ObjectNode requestJson = createJsonRequestBody("testing-client-id", false);

        // make request and parse the json response
        JsonNode updateUserResponse = parseJson(
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
        assertThat(updateUserResponse, matchesSnapshot());

    }

    /**
     * When creating a new datatools user which matches an existing Auth0 user, make sure the permissions for the
     * existing Auth0 users can be updated when permissions for another instance already exists.
     */
    @Test
    public void canUpdatePermsOfExistingAuth0UserWithExistingPerms() throws IOException {
        String newUserEmail = "test-existing-user@test.com";

        // create wiremock stub for user search that matches existing user
        String url = USER_SEARCH_API_PATH + URLEncoder.encode(newUserEmail, "UTF-8") + "*";
        wireMockServer.stubFor(
            get(urlEqualTo(url))
                .willReturn(
                    aResponse()
                        .withBodyFile("userSearchExistingPermsResponse.json")
                )
        );

        // create wiremock stub for update users endpoint
        wireMockServer.stubFor(
            patch(urlPathEqualTo(USERS_API_PATH + "/auth0%7Ctest-existing-user"))
                .willReturn(
                    aResponse()
                        .withBodyFile("updateExistingUserPermsResponse.json")
                )
        );

        ObjectNode requestJson = createJsonRequestBody("testing-client-id", false);

        // make request and parse the json response
        JsonNode updateUserResponse = parseJson(
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
        assertThat(updateUserResponse, matchesSnapshot());

    }

    /**
     * When creating a new datatools user which matches an existing Auth0 user, make sure the permissions for the
     * existing Auth0 user can not be updated for the same datatools instance.
     */
    @Test
    public void willNotUpdatePermsOfExistingAuth0UserWithExistingPerms() throws IOException {
        String newUserEmail = "test-existing-user@test.com";

        // create wiremock stub for user search that matches existing user
        String url = USER_SEARCH_API_PATH + URLEncoder.encode(newUserEmail, "UTF-8") + "*";
        wireMockServer.stubFor(
            get(urlEqualTo(url))
                .willReturn(
                    aResponse()
                        .withBodyFile("userSearchMatchingPermsResponse.json")
                )
        );

        // create wiremock stub for update users endpoint
        wireMockServer.stubFor(
            patch(urlPathEqualTo(USERS_API_PATH + "/auth0%7Ctest-existing-user"))
                .willReturn(
                    aResponse()
                        .withBodyFile("updateMatchingPermsResponse.json")
                )
        );

        ObjectNode requestJson = createJsonRequestBody("your-auth0-client-id", false);

        // make request and parse the json response
        JsonNode updateUserResponse = parseJson(
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
        assertThat(updateUserResponse, matchesSnapshot());

    }

    /**
     * Make sure a user can be created
     */
    @Test
    public void canCreateUser() throws IOException {
        String newUserEmail = "test-new-user@test.com";

        // create wiremock stub for empty user search
        String url = USER_SEARCH_API_PATH + URLEncoder.encode(newUserEmail, "UTF-8") + "*";
        wireMockServer.stubFor(
            get(urlEqualTo(url))
                .willReturn(
                    aResponse()
                        .withBody("[]")
                )
        );

        // create wiremock stub for create users endpoint
        wireMockServer.stubFor(
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

        String url = USER_SEARCH_API_PATH + URLEncoder.encode(emailForExistingAccount, "UTF-8") + "*";
        wireMockServer.stubFor(
            get(urlEqualTo(url))
                .willReturn(
                    aResponse()
                        .withBody("[]")
                )
        );

        // create wiremock stub for create users endpoint that responds with a message saying a user with the email
        // already exists
        wireMockServer.stubFor(
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
        wireMockServer.stubFor(
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
        wireMockServer.stubFor(
            get(urlPathEqualTo(USERS_API_PATH + "/auth0%7Ctest-existing-user"))
                .willReturn(
                    aResponse()
                        .withBodyFile("getExistingUserResponse.json")
                )
        );

        ObjectNode requestJson = createJsonRequestBody("testing-client-id", true);

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
        wireMockServer.stubFor(
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
        wireMockServer.stubFor(
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

        ObjectNode requestJson = createJsonRequestBody("testing-client-id", true);

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

    /**
     * Ensures that search wildcards are correctly inserted into search queries.
     */
    @ParameterizedTest
    @MethodSource("createMakeEmailFilterQueryCases")
    public void shouldMakeEmailFilterQuery(String queryString, String result) {
        assertThat(
            "Auth0 email user queries should be valid.",
            UserController.makeEmailFilterQuery(queryString).equals(result)
        );
    }

    private static Stream<Arguments> createMakeEmailFilterQueryCases() {
        return Stream.of(
            Arguments.of("Joe", "email:*Joe*"),
            // Don't insert the "begins with" wildcard if the search term is less than 3 characters long.
            Arguments.of("Jo", "email:Jo*")
        );
    }

    private static void stubForApiToken(String bodyFile) {
        wireMockServer.stubFor(
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

    /**
     * Create a request body containing email and datatool permissions. The permissions are structured slightly
     * differently if updating.
     */
    private ObjectNode createJsonRequestBody(String clientId, boolean updateUserRequest) {
        ObjectNode requestJson = mapper.createObjectNode();
        requestJson.put("email", emailForExistingAccount);
        ObjectNode testClientPermissions = mapper.createObjectNode();
        testClientPermissions.put("type", "administer-application");
        ObjectNode testClientData = mapper.createObjectNode();
        testClientData.putArray("permissions").add(testClientPermissions);
        testClientData.putArray("projects");
        testClientData.putArray("organizations");
        testClientData.put("client_id", clientId);
        if (updateUserRequest) {
            requestJson.putArray("data").add(testClientData);
        } else {
            requestJson.set("permissions", testClientData);
        }
        return requestJson;
    }
}
