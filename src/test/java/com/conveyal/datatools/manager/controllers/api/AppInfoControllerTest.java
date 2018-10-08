package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.DatatoolsTest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class AppInfoControllerTest {
    /**
     * Prepare and start a testing-specific web server
     */
    @BeforeAll
    public static void setUp() {
        // start server if it isn't already running
        DatatoolsTest.setUp();
    }

    /**
     * Make sure the app info endpoint can load and return expected data.
     */
    @Test
    public void canReturnApprInfo() throws IOException {
        String jsonString = given()
            .port(4000)
            .get("/api/manager/public/appinfo")
        .then()
            // make sure the repoUrl matches what is found in the pom.xml
            .body("repoUrl", equalTo("https://github.com/catalogueglobal/datatools-server.git"))
            .extract().response().asString();

        // parse the json and make sure the commit is the length of characters that a commit hash would be
        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(jsonString);
        assertThat(json.get("commit").asText().length(), equalTo(40));
    }
}
