package com.conveyal.datatools.manager;

import com.conveyal.datatools.DatatoolsTest;
import org.junit.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * These tests make sure various webserver routes work as expected
 */
public class DataManagerTest extends DatatoolsTest {
    /**
     * Make sure the html index page can be retrieved
     */
    @Test
    public void canGetIndex() {
        given()
            .port(4000)
        .when()
            .get("/")
        .then()
            .statusCode(200)
            .body(containsString("<title>Catalogue</title>"));
    }

    /**
     * Make sure the Auth0SilentCallback page can be retrived
     */
    @Test
    public void canGetAuth0SilentCallback() {
        given()
            .port(4000)
        .when()
            .get("/api/auth0-silent-callback")
        .then()
            .statusCode(200)
            .body(containsString("type: 'auth0:silent-authentication',"));
    }

    /**
     * Make sure the server gracefully fails with a helpful message on an unregistered api manager route.
     */
    @Test
    public void respondsWithErrorForUnknownApiRoute() {
        given()
            .port(4000)
        .when()
            .get("/api/manager/undefined-route")
        .then()
            .header("content-type", "application/json")
            .statusCode(404)
            .body("result", equalTo("ERR"))
            .body("message", equalTo("API route not defined."));
    }
}
