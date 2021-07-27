package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.TestUtils;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.FetchSingleFeedJob;
import com.conveyal.datatools.manager.jobs.ProcessSingleFeedJob;
import com.conveyal.datatools.manager.models.FeedRetrievalMethod;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.HttpUtils;
import com.conveyal.datatools.manager.utils.JobUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.http.HttpResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test class that checks endpoints from StatusController.
 */
class StatusControllerTest extends UnitTest {
    private static final Logger LOG = LoggerFactory.getLogger(StatusControllerTest.class);
    private static FeedSource mockFeedSource;
    private static Project project;
    private static final Auth0UserProfile user = Auth0UserProfile.createTestAdminUser();

    /**
     * Prepare and start a testing-specific web server
     */
    @BeforeAll
    public static void setUp() throws Exception {
        DatatoolsTest.setUp();
        Auth0Connection.setAuthDisabled(true);
        LOG.info("{} setup", StatusControllerTest.class.getSimpleName());

        // Create a project, feed sources to pass to jobs.
        project = new Project();
        project.name = String.format("Test %s", new Date());
        Persistence.projects.create(project);
        mockFeedSource = new FeedSource("Mock Feed Source", project.id, FeedRetrievalMethod.MANUALLY_UPLOADED);
        mockFeedSource.url = new URL("https://example.com");
        Persistence.feedSources.create(mockFeedSource);
    }

    @AfterAll
    public static void tearDown() {
        Auth0Connection.setAuthDisabled(Auth0Connection.getDefaultAuthDisabled());
        mockFeedSource.delete();
        project.delete();
    }

    /**
     * Ensures the jobs endpoint returns parent job id and type for child jobs.
     */
    @Test
    void canReturnJobStatuses() throws IOException {
        // Create a simple job chain, e.g. create a snapshot published as a new feed version.
//        String feedId = mockFeedSource.id;
/*
        HttpResponse startJobsResponse = TestUtils.makeRequest(
            String.format("/api/editor/secure/snapshot?feedId=%s&publishNewVersion=true&dryRun=true", feedId),
            String.format("{\"feedId\":\"%s\", \"name\":\"Test Snapshot\", \"comment\":null}", feedId),
            HttpUtils.REQUEST_METHOD.POST
        );
*/
        FeedVersion f = TestUtils.createMockFeedVersion(mockFeedSource.id);
        // Define a simple job chain.
        // (The data for these jobs is arbitrary, the jobs are not actually executed.)
        ProcessSingleFeedJob parentJob = new ProcessSingleFeedJob(f, user, true);
        FetchSingleFeedJob subJob = new FetchSingleFeedJob(mockFeedSource, user, true);
        parentJob.addNextJob(subJob);
        JobUtils.heavyExecutor.execute(parentJob);

        // Call the jobs endpoint immediately
/*
        // Parse the first startup JSON and grab the job id.
        JsonNode startJobJson = mapper.readTree(startJobsResponse.getEntity().getContent());
        assertEquals("Creating snapshot.", startJobJson.get("message").asText());
        String jobId = startJobJson.get("jobId").asText();
*/
        HttpResponse getJobsResponse = TestUtils.makeRequest(
            "/api/manager/secure/status/jobs",
            null,
            HttpUtils.REQUEST_METHOD.GET
        );

        // There should be two jobs, one for the parent job and one for the subjob.
        // The order in which they appear varies from a test run to the next.
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode statusJson = (ArrayNode)mapper.readTree(getJobsResponse.getEntity().getContent());
        assertEquals(2, statusJson.size());
        JsonNode firstJob = statusJson.get(0);
        JsonNode secondJob = statusJson.get(1);

        // One of the jobs should be of the id we extracted above.
        String jobId = parentJob.jobId;
        JsonNode subJobJson = null;
        if (jobId.equals(firstJob.get("jobId").asText())) {
            assertEquals(parentJob.type.name(), firstJob.get("type").asText());
            subJobJson = secondJob;
        } else if (jobId.equals(secondJob.get("jobId").asText())) {
            assertEquals(parentJob.type.name(), secondJob.get("type").asText());
            subJobJson = firstJob;
        }

        // The sub job's parent should be the main job.
        assertNotNull(subJob);
        assertEquals(jobId, subJobJson.get("parentJobId").asText());
        assertEquals(parentJob.type.name(), subJobJson.get("parentJobType").asText());
        assertEquals(subJob.type.name(), subJobJson.get("type").asText());
    }
}
