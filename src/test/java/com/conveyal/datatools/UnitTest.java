package com.conveyal.datatools;

import org.junit.BeforeClass;

import static com.conveyal.datatools.TestUtils.getBooleanEnvVar;
import static org.junit.Assume.assumeFalse;

/**
 * This class is used by all unit tests to indicate that all test cases are not part of the end-to-end tests. This way,
 * if the e2e tests should be ran, all the unit tests can be skipped in order to save time and generate appropriate
 * code coverage reports.
 */
public abstract class UnitTest {
    @BeforeClass
    public static void beforeAll () {
        // make sure the RUN_E2E environment variable is set to true.  Otherwise, this test suite should be skipped and
        // the overall build should not depend on inheriting test suites passing in order to save time.
        assumeFalse(getBooleanEnvVar("RUN_E2E"));
    }
}
