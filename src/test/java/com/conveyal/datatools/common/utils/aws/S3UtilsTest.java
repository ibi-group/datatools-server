package com.conveyal.datatools.common.utils.aws;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Uri;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class S3UtilsTest extends UnitTest {

    public static final String BUCKET = "bucket-name";
    public static final String KEY = "public/index.html";
    public static final String URL = String.format("https://%s.s3.amazonaws.com/%s", BUCKET, KEY);

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
        assertEquals(URL, S3Utils.getDefaultBucketUrlForKey(KEY));
    }

    @Test
    void canMakeUri() {
        S3Uri uri = S3Utils.makeUri(URL);
        assertEquals(BUCKET, uri.bucket().orElse(null));
        assertEquals(KEY, uri.key().orElse(null));
    }
}
