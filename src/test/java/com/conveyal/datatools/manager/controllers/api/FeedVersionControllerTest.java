package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.TestUtils;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.FeedVersionSummary;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.HttpUtils;
import com.conveyal.datatools.manager.utils.SimpleHttpResponse;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import static org.eclipse.jetty.http.HttpStatus.OK_200;
import static org.junit.jupiter.api.Assertions.assertEquals;

class FeedVersionControllerTest extends DatatoolsTest {
    private static final String NAMESPACE = "published_namespace";

    private static Project project;
    private static FeedSource feedSource;

    /** Initialize application for tests to run. */
    @BeforeAll
    static void settingUp() throws Exception {
        // start server if it isn't already running
        DatatoolsTest.setUp();
        Auth0Connection.setAuthDisabled(true);

        // set up project
        project = new Project();
        project.name = String.format("Test project %s", new Date());
        Persistence.projects.create(project);

        feedSource = new FeedSource("Test feed source");
        feedSource.projectId = project.id;
        feedSource.publishedVersionId = NAMESPACE;
        Persistence.feedSources.create(feedSource);
    }

    @AfterAll
    static void tearDown() {
        Auth0Connection.setAuthDisabled(Auth0Connection.getDefaultAuthDisabled());
        if (project != null) {
            project.delete();
        }
    }

    /**
     * For {@link FeedVersionSummary}, field feedSourcePublishedVersionId from {@link FeedSource::publishedVersionId}
     * should be populated when calling the feedversionsummaries endpoint.
     */
    @Test
    void shouldPopulateFeedSourcePublishedVersionId() throws IOException {
        // Create a feed version linked to the feed source.
        FeedVersion feedVersion1 = new FeedVersion(feedSource);
        feedVersion1.namespace = NAMESPACE;
        feedVersion1.feedSourceId = feedSource.id;
        Persistence.feedVersions.create(feedVersion1);

        SimpleHttpResponse feedVersionSummaryResponse = TestUtils.makeRequest("/api/manager/secure/feedversionsummaries?feedSourceId=" + feedSource.id,
            null,
            HttpUtils.REQUEST_METHOD.GET
        );
        assertEquals(OK_200, feedVersionSummaryResponse.status);

        List<FeedVersionSummary> summaries = JsonUtil.getPOJOFromJSONAsList(
            JsonUtil.getJsonNodeFromResponse(feedVersionSummaryResponse),
            FeedVersionSummary.class
        );

        assertEquals(1, summaries.size());
        assertEquals(NAMESPACE, summaries.get(0).feedSourcePublishedVersionId);
    }
}
