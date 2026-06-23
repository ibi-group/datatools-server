package com.conveyal.datatools.common.utils.aws;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class S3UtilsTest extends UnitTest {
    /**
     * Create feed version for GTFS+ validation test.
     */
    @BeforeAll
    static void setUp() throws IOException {
        // Start server if it isn't already running.
        DatatoolsTest.setUp();
    }

    @Test
    void canGetDefaultBucketUrlForKey() {
        final String FILENAME = "public/index.html";

        assertEquals(
            "https://bucket-name.s3.amazonaws.com/public/index.html",
            S3Utils.getDefaultBucketUrlForKey(FILENAME)
        );
    }
}
