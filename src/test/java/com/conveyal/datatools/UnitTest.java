package com.conveyal.datatools;

import org.junit.BeforeClass;

import static com.conveyal.datatools.TestUtils.getBooleanEnvVar;
import static org.junit.Assume.assumeFalse;

public abstract class UnitTest {
    @BeforeClass
    public static void beforeAll () {
        // make sure the RUN_E2E environment variable is set to true.  Otherwise, this test should
        // be skipped and the overall build should not depend on this test passing in order to save
        // time.
        assumeFalse(getBooleanEnvVar("RUN_E2E"));
    }
}
