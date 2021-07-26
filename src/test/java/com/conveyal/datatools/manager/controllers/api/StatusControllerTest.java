package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.TestUtils;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.models.FeedRetrievalMethod;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.HttpUtils;
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
        Persistence.feedSources.create(mockFeedSource);
    }

    @AfterAll
    public static void tearDown() {
        Auth0Connection.setAuthDisabled(false);
        mockFeedSource.delete();
        project.delete();
    }

    /**
     * Ensures the jobs endpoint returns parent job id and type for child jobs.
     */
    @Test
    void canReturnJobStatuses() throws IOException {
        // Create a simple job chain, e.g. create a snapshot published as a new feed version.
        String feedId = mockFeedSource.id;
        HttpResponse startJobsResponse = TestUtils.makeRequest(
            String.format("/api/editor/secure/snapshot?feedId=%s&publishNewVersion=true&testDelayedStart=true", feedId),
            String.format("{\"feedId\":\"%s\", \"name\":\"Test Snapshot\", \"comment\":null}", feedId),
            HttpUtils.REQUEST_METHOD.POST
        );

        // Call the jobs endpoint immediately
        HttpResponse getJobsResponse = TestUtils.makeRequest(
            "/api/manager/secure/status/jobs",
            null,
            HttpUtils.REQUEST_METHOD.GET
        );

        // Parse the first startup JSON and grab the job id.
        ObjectMapper mapper = new ObjectMapper();
        JsonNode startJobJson = mapper.readTree(startJobsResponse.getEntity().getContent());
        assertEquals("Creating snapshot.", startJobJson.get("message").asText());
        String jobId = startJobJson.get("jobId").asText();


        // There should be 2 jobs, one for creating the snapshot, one for creating a new feed version from snapshot.
        // The order in which they appear varies from a test run to the next.
        ArrayNode statusJson = (ArrayNode)mapper.readTree(getJobsResponse.getEntity().getContent());
        assertEquals(2, statusJson.size());
        JsonNode firstJob = statusJson.get(0);
        JsonNode secondJob = statusJson.get(1);

        // One of the jobs should be of the id we extracted above.
        JsonNode subJob = null;
        if (jobId.equals(firstJob.get("jobId").asText())) {
            assertEquals("CREATE_SNAPSHOT", firstJob.get("type").asText());
            subJob = secondJob;
        } else if (jobId.equals(secondJob.get("jobId").asText())) {
            assertEquals("CREATE_SNAPSHOT", secondJob.get("type").asText());
            subJob = firstJob;
        }

        // The sub job's parent should be the main job.
        assertNotNull(subJob);
        assertEquals(jobId, subJob.get("parentJobId").asText());
        assertEquals("CREATE_SNAPSHOT", subJob.get("parentJobType").asText());
        assertEquals("CREATE_FEEDVERSION_FROM_SNAPSHOT", subJob.get("type").asText());
    }
}
