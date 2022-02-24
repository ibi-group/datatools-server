package com.conveyal.datatools.manager.jobs.validation;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RouteTypeValidatorBuilderTest extends UnitTest {
    @BeforeAll
    static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();
    }

    @Test
    void canGetConfiguredRouteTypes() {
        List<Integer> routeTypes = RouteTypeValidatorBuilder.getConfiguredRouteTypes();

        // Test a small subset of route types.
        assertTrue(routeTypes.contains(3)); // bus
        assertTrue(routeTypes.contains(106)); // regional rail
        assertTrue(routeTypes.contains(1702)); // Horse carriage
        assertFalse(routeTypes.contains(98)); // unknown
    }
}
