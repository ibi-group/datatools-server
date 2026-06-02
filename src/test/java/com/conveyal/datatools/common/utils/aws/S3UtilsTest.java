package com.conveyal.datatools.common.utils.aws;

import com.amazonaws.auth.AWSCredentials;
import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.TestUtils;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.DataManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.List;

import static com.conveyal.datatools.manager.DataManager.getConfigPropertyAsText;
import static com.conveyal.datatools.manager.DataManager.isExtensionEnabled;
import static org.junit.jupiter.api.Assertions.assertEquals;

class S3UtilsTest extends UnitTest {
    // TODO: Move these constants
    public static final String CONFIG_MTC_ENABLED = "extensions.mtc.enabled";
    public static final String CONFIG_MTC_CREDENTIALS = "extensions.mtc.s3_credentials_file";
    public static final String CONFIG_MTC_REGION = "extensions.mtc.s3_region";
    private static boolean defaultMtcEnabled;
    private static String defaultMtcAwsCredentials;
    private static String defaultMtcAwsRegion;

    /**
     * Create feed version for GTFS+ validation test.
     */
    @BeforeAll
    public static void setUp() throws IOException {
        // Start server if it isn't already running.
        DatatoolsTest.setUp();
        defaultMtcEnabled = isExtensionEnabled("mtc");
        defaultMtcAwsCredentials = getConfigPropertyAsText(CONFIG_MTC_CREDENTIALS);
        defaultMtcAwsRegion = getConfigPropertyAsText(CONFIG_MTC_REGION);
    }

    @AfterEach
    void resetState() {
        DataManager.overrideConfigProperty(CONFIG_MTC_ENABLED, Boolean.toString(defaultMtcEnabled));
        DataManager.overrideConfigProperty(CONFIG_MTC_CREDENTIALS, defaultMtcAwsCredentials);
        DataManager.overrideConfigProperty(CONFIG_MTC_REGION, defaultMtcAwsRegion);
    }

    @Test
    void canGetDefaultBucketUrlForKey() {
        final String FILENAME = "public/index.html";

        assertEquals(
            "https://bucket-name.s3.amazonaws.com/public/index.html",
            S3Utils.getDefaultBucketUrlForKey(FILENAME)
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void canUseCorrectMtcCredentials(boolean overwriteRegion) {
        DataManager.overrideConfigProperty(CONFIG_MTC_ENABLED, "true");
        if (overwriteRegion) {
            DataManager.overrideConfigProperty(CONFIG_MTC_REGION, "us-west-2");
        }
        String credentialsFile = TestUtils.class.getResource("mock-aws-credentials").getFile();
        DataManager.overrideConfigProperty(CONFIG_MTC_CREDENTIALS, credentialsFile);
        List<String> configRegions = List.of("extensions.mtc.s3_region", "application.data.s3_region");
        S3Utils.S3ClientBuild build = S3Utils.buildS3Client(CONFIG_MTC_CREDENTIALS, configRegions);

        // Either application.data.s3_region or extensions.mtc.s3_region from server.yml.tmp.
        // null denotes the us-east-1 region.
        assertEquals(overwriteRegion ? "us-west-2" : null, build.s3Client.getRegion().getFirstRegionId());

        // The credentials should reflect the content in the mock-aws-credentials file referenced above.
        AWSCredentials credentials = build.credentials.getCredentials();
        assertEquals("mock-id-123", credentials.getAWSAccessKeyId());
        assertEquals("mock-secret-123", credentials.getAWSSecretKey());
    }
}
