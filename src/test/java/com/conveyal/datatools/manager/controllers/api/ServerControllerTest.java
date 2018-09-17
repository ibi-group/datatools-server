package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.DatatoolsTest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class ServerControllerTest {
    private static boolean setUpIsDone;

    /**
     * Prepare and start a testing-specific web server
     */
    @BeforeClass
    public static void setUp() {
        if (setUpIsDone) {
            return;
        }

        // start server if it isn't already running
        DatatoolsTest.setUp();

        // populate
        setUpIsDone = true;
    }

    /**
     * Make sure the server info endpoint can load and return expected data.
     */
    @Test
    public void canReturnTimetables() throws IOException {
        String jsonString = given()
            .port(4000)
            .get("/api/manager/public/serverinfo")
        .then()
            // make sure the repoUrl matches what is found in the pom.xml
            .body("repoUrl", equalTo("https://github.com/catalogueglobal/datatools-server.git"))
            // extract the response to do more complex things
            .extract().response().asString();

        // parse the json and make sure the commit is a the length of characters that a commit hash would be
        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(jsonString);
        assertThat(json.get("commit").asText().length(), equalTo(40));
    }
}
