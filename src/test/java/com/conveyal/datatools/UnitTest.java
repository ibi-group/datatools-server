package com.conveyal.datatools;

import com.conveyal.datatools.editor.controllers.api.EditorControllerTest;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.conveyal.datatools.TestUtils.getBooleanEnvVar;
import static org.junit.Assume.assumeFalse;

/**
 * This class is used by all unit tests to indicate that all test cases are not part of the end-to-end tests. This way,
 * if the e2e tests should be ran, all the unit tests can be skipped in order to save time and generate appropriate
 * code coverage reports.
 */
public abstract class UnitTest {
    private static final Logger LOG = LoggerFactory.getLogger(UnitTest.class);
    @BeforeClass
    public static void beforeAll () {
        LOG.info(String.valueOf(getBooleanEnvVar("RUN_E2E")));
        // make sure the RUN_E2E environment variable is set to true.  Otherwise, this test suite should be skipped and
        // the overall build should not depend on inheriting test suites passing in order to save time.
        assumeFalse(getBooleanEnvVar("RUN_E2E"));
    }
}
