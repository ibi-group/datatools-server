package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.TestUtils;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedRetrievalMethod;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test class for checking job chaining functionality.
 */
class JobChainTest extends DatatoolsTest {
    private static final Logger LOG = LoggerFactory.getLogger(JobChainTest.class);
    private static final Auth0UserProfile user = Auth0UserProfile.createTestAdminUser();
    private static FeedSource mockFeedSource;
    private static Project project;

    @BeforeAll
    public static void setUp() throws IOException {
        DatatoolsTest.setUp();
        LOG.info("{} setup", JobChainTest.class.getSimpleName());

        // Create a project, feed sources to pass to jobs.
        project = new Project();
        project.name = String.format("Test %s", new Date());
        Persistence.projects.create(project);
        mockFeedSource = new FeedSource("Mock Feed Source", project.id, FeedRetrievalMethod.MANUALLY_UPLOADED);
        Persistence.feedSources.create(mockFeedSource);
    }

    @AfterAll
    public static void tearDown() {
        mockFeedSource.delete();
        project.delete();
    }

    @Test
    void shouldPopulateParentJobIdAndTypeInSubjobs() {
        FeedVersion f = TestUtils.createMockFeedVersion(mockFeedSource.id);

        // Define a simple job chain.
        // (The data for these jobs is arbitrary, the jobs are not actually executed.)
        ProcessSingleFeedJob parentJob = new ProcessSingleFeedJob(f, user, true);
        FetchSingleFeedJob subJob = new FetchSingleFeedJob(mockFeedSource, user, true);
        parentJob.addNextJob(subJob);

        assertEquals(parentJob.jobId, subJob.parentJobId);
        assertEquals(parentJob.type, subJob.parentJobType);
    }
}
