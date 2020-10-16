package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

import static com.conveyal.datatools.TestUtils.createFeedVersionFromGtfsZip;

public class FetchSingleFeedJobTest extends DatatoolsTest {
    private static final Logger LOG = LoggerFactory.getLogger(GisExportJobTest.class);
    private static Project project;
    private static FeedVersion calTrainVersion;
    private static FeedSource caltrain;

    @BeforeClass
    public static void setUp() throws IOException {
        DatatoolsTest.setUp();
        LOG.info("{} setup", GisExportJobTest.class.getSimpleName());

        // Create a project, feed sources, and feed versions to merge.
        project = new Project();
        project.name = String.format("Test %s", new Date().toString());
        Persistence.projects.create(project);
        caltrain = new FeedSource("Caltrain");
        Persistence.feedSources.create(caltrain);
    }

    @Test
    public void canFetchGtfsFile() {
        calTrainVersion = createFeedVersionFromGtfsZip(caltrain, "fake-gtfs-txt-file.zip");
        // Check that notification is produced?
    }
}
