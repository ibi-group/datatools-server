package com.conveyal.datatools;

import com.conveyal.datatools.manager.jobs.ProcessSingleFeedJob;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class TestUtils {
    /**
     * Parse a json string into an unmapped JsonNode object
     */
    public static JsonNode parseJson(String jsonString) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(jsonString);
    }

    /**
     * Utility function to create a feed version during tests. Note: this is intended to run the job in the same thread,
     * so that tasks can run synchronously.
     */
    public static FeedVersion createFeedVersion(FeedSource source, String gtfsFileName) {
        File gtfsFile = new File(TestUtils.class.getResource(gtfsFileName).getFile());
        return createFeedVersion(source, gtfsFile);
    }

    /**
     * Utility function to create a feed version during tests. Note: this is intended to run the job in the same thread,
     * so that tasks can run synchronously.
     */
    public static FeedVersion createFeedVersion(FeedSource source, File gtfsFile) {
        FeedVersion version = new FeedVersion(source);
        InputStream is;
        try {
            is = new FileInputStream(gtfsFile);
            version.newGtfsFile(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
        ProcessSingleFeedJob processSingleFeedJob = new ProcessSingleFeedJob(version, "test", true);
        // Run in same thread to keep things synchronous.
        processSingleFeedJob.run();
        return version;
    }

}
