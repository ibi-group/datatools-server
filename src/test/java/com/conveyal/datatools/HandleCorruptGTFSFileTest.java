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
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Run test to handle a corrupt GTFS file gracefully.
 */
public class HandleCorruptGTFSFileTest {
    private static FeedSource mockFeedSource;

    @BeforeAll
    public static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();
    }

    @AfterAll
    public static void tearDown() {
        mockFeedSource.delete();
    }

    @Test
    public void canHandleCorruptGTFSFile() {
        mockFeedSource = new FeedSource("Corrupt");
        Persistence.feedSources.create(mockFeedSource);
        FeedVersion corruptFeedVersion = createFeedVersionFromGtfsZip(mockFeedSource, "corrupt-gtfs-file.zip");
        assertEquals("java.util.zip.ZipException: error in opening zip file", corruptFeedVersion.feedLoadResult.fatalException);
        assertNull(corruptFeedVersion.validationResult);
        assertNull(corruptFeedVersion.feedTransformResult);
        assertNull(corruptFeedVersion.namespace);
    }
}
