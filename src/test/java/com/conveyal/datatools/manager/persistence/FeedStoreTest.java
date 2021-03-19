package com.conveyal.datatools.manager.persistence;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.models.FeedVersion;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import static com.conveyal.datatools.TestUtils.getGtfsResourcePath;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FeedStoreTest extends UnitTest {
    private static final Logger LOG = LoggerFactory.getLogger(FeedStoreTest.class);

    @BeforeAll
    public static void setUp() throws Exception {
        DatatoolsTest.setUp();
        LOG.info("{} setup", FeedStoreTest.class.getSimpleName());
    }

    /**
     * Verify that {@link FeedStore} will write a temp file with the input file name.
     */
    @Test
    public void canCreateTempGtfsFile() throws IOException {
        final String gtfsFileName = "gtfs.zip";
        File gtfsFile = new File(getGtfsResourcePath("bart_new.zip"));
        FileInputStream fileInputStream = new FileInputStream(gtfsFile);
        File tempFile = FeedVersion.feedStore.createTempFile(gtfsFileName, fileInputStream);
        LOG.info("Feed store wrote temp file to: {}", tempFile.getAbsolutePath());
        assertEquals(tempFile.getName(), gtfsFileName);
    }
}
