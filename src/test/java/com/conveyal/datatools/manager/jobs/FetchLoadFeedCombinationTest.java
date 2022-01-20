package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.connections.ConnectionResponse;
import com.github.tomakehurst.wiremock.WireMockServer;
import io.restassured.response.Response;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.Date;
import java.util.List;

import static com.conveyal.datatools.TestUtils.createFeedVersionFromGtfsZip;
import static com.conveyal.datatools.manager.models.FeedRetrievalMethod.FETCHED_AUTOMATICALLY;
import static com.conveyal.datatools.manager.models.FeedRetrievalMethod.MANUALLY_UPLOADED;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static com.mongodb.client.model.Filters.eq;
import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for the various combinations of {@link FetchSingleFeedJob} and {@link LoadFeedJob} cases.
 */
public class FetchLoadFeedCombinationTest extends UnitTest {
    private static final Auth0UserProfile user = Auth0UserProfile.createTestAdminUser();
    private static Project project;
    private static final String MOCKED_HOST = "fakehost.com";
    private static final String MOCKED_FETCH_URL = "/dev/schedules/google_transit.zip";

    private static WireMockServer wireMockServer;
    private FeedSource feedSource;

    /**
     * Prepare and start a testing-specific web server
     */
    @BeforeAll
    public static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();

        // Create a project and feed sources.
        project = new Project();
        project.name = String.format("Test %s", new Date());
        Persistence.projects.create(project);

        // This sets up a mock server that accepts requests and sends predefined responses to mock a GTFS file download.
        configureFor(MOCKED_HOST, 80);
        wireMockServer = new WireMockServer(
            options()
                .usingFilesUnderDirectory("src/test/resources/com/conveyal/datatools/gtfs/")
        );
        wireMockServer.start();
    }

    @AfterAll
    public static void tearDown() {
        wireMockServer.stop();
        if (project != null) {
            project.delete();
        }
    }

    @BeforeEach
    public void setUpEach() {
        feedSource = new FeedSource("Feed source", project.id, MANUALLY_UPLOADED);
        Persistence.feedSources.create(feedSource);

        // Create wiremock stub for gtfs download.
        wireMockServer.stubFor(
            get(urlPathEqualTo(MOCKED_FETCH_URL))
                .willReturn(
                    aResponse()
                        .withBodyFile("bart_new_lite.zip")
                )
        );
    }

    /**
     * Refetching should be allowed in the following scenario:
     * 1. Feed is fetched as Version 1.
     * 2. Another feed version is uploaded as Version 2.
     * 3. Feed is refetched as Version 3.
     */
    @Test
    void shouldRefetchAfterFetchAndManualUpload() {
        // Simulate the first job for the initial fetch.
        simulateFetch();

        // Assert Version 1 is created.
        assertVersionCount(1);

        // Create the second job for manual upload.
        FeedVersion uploadedFeedVersion = createFeedVersionFromGtfsZip(feedSource, "bart_old_lite.zip");
        new LoadFeedJob(uploadedFeedVersion, user, true).run();

        // Assert Version 2 is created.
        assertVersionCount(2);

        // Simulate the third job for the refetch.
        simulateFetch();

        // Assert Version 3 is created.
        assertVersionCount(3);
    }

    /**
     * Refetching should not happen when doing two successive fetches (existing functionality).
     * 1. Feed is fetched as Version 1.
     * 2. Feed is refetched but no version is created, because either
     *    304 NOT MODIFIED was returned, or the exact same file was downloaded again.
     */
    @Test
    void shouldNotLoadVersionIdenticalToPrevious() {
        // Simulate the first fetch.
        simulateFetch();

        // Assert Version 1 is created.
        assertVersionCount(1);

        // Simulate the second fetch with the response as "unchanged".
        simulateFetch();

        // Assert no version 2 is created.
        assertVersionCount(1);

        // Some servers support a 304 (not modified) response,
        // and that should also result in no new version created.
        wireMockServer.stubFor(
            get(urlPathEqualTo(MOCKED_FETCH_URL))
                .willReturn(
                    aResponse()
                        .withStatus(HttpURLConnection.HTTP_NOT_MODIFIED)
                )
        );

        // Simulate the re-fetch with the response as "unchanged".
        simulateFetch();

        // Assert no version 2 is created.
        assertVersionCount(1);
    }

    /**
     * Simulates a fetch on the feed source.
     */
    private void simulateFetch() {
        MockConnectionResponse response = new MockConnectionResponse(
            given()
                .get(MOCKED_FETCH_URL)
                .then()
                .extract()
                .response()
        );
        FeedVersion newVersion = new FeedVersion(feedSource, FETCHED_AUTOMATICALLY);
        newVersion = feedSource.processFetchResponse(
            new MonitorableJob.Status(),
            null,
            newVersion,
            feedSource.retrieveLatest(),
            response
        );
        if (newVersion != null) {
            new ProcessSingleFeedJob(newVersion, user, true).run();
        }
    }

    /**
     * Assert feed version count.
     */
    private void assertVersionCount(int size) {
        // Fetch versions.
        List<FeedVersion> versions = Persistence.feedVersions.getFiltered(
            eq("feedSourceId", feedSource.id)
        );
        assertEquals(size, versions.size());
    }

    /**
     * Simulates a {@link ConnectionResponse} instance sent to FeedSource
     * from a mock {@link Response} instance.
     * TODO: Handle mock redirects.
     */
    public static class MockConnectionResponse implements ConnectionResponse {
        private final Response response;

        public MockConnectionResponse(Response resp) {
            this.response = resp;
        }

        public int getResponseCode() {
            return response.statusCode();
        }

        public InputStream getInputStream() {
            return response.asInputStream();
        }

        public Long getLastModified() {
            return response.time();
        }

        public String getResponseMessage() {
            return response.statusLine();
        }

        @Override
        public String getRedirectUrl() {
            return response.getHeader("Location");
        }
    }
}
