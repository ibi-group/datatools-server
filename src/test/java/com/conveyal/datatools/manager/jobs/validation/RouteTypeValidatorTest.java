package com.conveyal.datatools.manager.jobs.validation;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class RouteTypeValidatorTest extends UnitTest {
    @BeforeAll
    public static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();
    }

    @Test
    void validateRouteType() {
        assertThat(RouteTypeValidator.isRouteTypeValid(3), is(true)); // Bus
        assertThat(RouteTypeValidator.isRouteTypeValid(36), is(false)); // Invalid
        assertThat(RouteTypeValidator.isRouteTypeValid(101), is(true)); // High Speed Rail
    }
}
