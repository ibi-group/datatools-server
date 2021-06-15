package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.TestUtils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.HttpUtils;
import com.conveyal.datatools.manager.utils.StringUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.conveyal.datatools.TestUtils.createFeedVersion;
import static com.conveyal.datatools.TestUtils.zipFolderFiles;
import static org.eclipse.jetty.http.HttpStatus.OK_200;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FeedSourceSpecificDeploymentControllerTest {
    private static Project project = null;
    private static FeedSource feedSource = null;
    private static FeedVersion feedVersion = null;
    private static ObjectMapper objectMapper = new ObjectMapper();

    @BeforeAll
    public static void setUp() throws IOException {
        DatatoolsTest.setUp();
        Auth0Connection.setAuthDisabled(true);
        project = new Project();
        project.name = "ProjectOne";
        Persistence.projects.create(project);
        feedSource = new FeedSource("FeedSourceOne");
        feedSource.projectId = project.id;
        Persistence.feedSources.create(feedSource);
        feedVersion = createFeedVersion(
                feedSource,
                zipFolderFiles("fake-agency-with-calendar-and-calendar-dates")
        );
    }

    @AfterAll
    public static void tearDown() {
        Auth0Connection.setAuthDisabled(Auth0Connection.getDefaultAuthDisabled());
        if (project != null) {
            project.delete();
        }
    }

    @Test
    public void canCreateFeedSourceSpecificDeploymentWithDefaultRouter() throws IOException {
        // Create a feed source specific deployment.
        HttpResponse createDeploymentResponse = TestUtils.makeRequest( "/api/manager/secure/deployments/fromfeedsource/" + feedSource.id,
                null,
                HttpUtils.REQUEST_METHOD.POST
        );
        Deployment deployment = objectMapper.readValue(createDeploymentResponse.getEntity().getContent(), Deployment.class);

        assertEquals(OK_200, createDeploymentResponse.getStatusLine().getStatusCode());
        assertEquals(null, deployment.routerId );
    }
}
