package com.conveyal.datatools;

import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.conveyal.datatools.TestUtils.createFeedVersionFromGtfsZip;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Run test to handle a corrupt GTFS file gracefully.
 */
public class HandleCorruptGTFSFileTest {
    private static FeedSource mockFeedSource;
    private static FeedVersion corruptFeedVersion;

    @BeforeAll
    public static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();
        mockFeedSource = new FeedSource("Corrupt");
        Persistence.feedSources.create(mockFeedSource);
        corruptFeedVersion = createFeedVersionFromGtfsZip(mockFeedSource, "corrupt-gtfs-file.zip");
    }

    @AfterAll
    public static void tearDown() {
        mockFeedSource.delete();
    }

    @Test
    public void canHandleCorruptGTFSFile() {
        assertEquals(corruptFeedVersion.feedLoadResult.fatalException, "java.util.zip.ZipException: error in opening zip file");
    }
}
