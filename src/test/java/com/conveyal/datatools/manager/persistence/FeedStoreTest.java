package com.conveyal.datatools.manager.persistence;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.models.FeedVersion;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import static com.conveyal.datatools.TestUtils.getGtfsResourcePath;

public class FeedStoreTest extends UnitTest {
    private static final Logger LOG = LoggerFactory.getLogger(FeedStoreTest.class);

    @BeforeClass
    public static void setUp() throws Exception {
        DatatoolsTest.setUp();
        LOG.info("{} setup", FeedStoreTest.class.getSimpleName());
    }

    @Test
    public void canCreateTempGtfsFile() throws IOException {
        File gtfsFile = new File(getGtfsResourcePath("bart_new.zip"));
        FileInputStream fileInputStream = new FileInputStream(gtfsFile);
        File tempFile = FeedVersion.feedStore.createTempFile("gtfs.zip", fileInputStream);
        Assert.assertTrue(tempFile.getAbsolutePath().endsWith("gtfs.zip"));
        LOG.info(tempFile.getAbsolutePath());
    }
}
