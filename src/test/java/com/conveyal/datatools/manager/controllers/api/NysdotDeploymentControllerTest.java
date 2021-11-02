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
import com.conveyal.datatools.manager.utils.SimpleHttpResponse;
import com.conveyal.datatools.manager.utils.StringUtils;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.conveyal.datatools.TestUtils.createFeedVersion;
import static com.conveyal.datatools.TestUtils.zipFolderFiles;
import static org.eclipse.jetty.http.HttpStatus.OK_200;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class NysdotDeploymentControllerTest {
    private static Project project = null;
    private static FeedSource feedSource = null;
    private static FeedVersion feedVersion = null;
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static String nysdotEnabledField = "extensions.nysdot.enabled";
    private static String prevNysdotEnabled = DataManager.getConfigPropertyAsText(nysdotEnabledField);

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
        DataManager.overrideConfigProperty(nysdotEnabledField, "true");
    }

    @AfterAll
    public static void tearDown() {
        Auth0Connection.setAuthDisabled(Auth0Connection.getDefaultAuthDisabled());
        if (project != null) {
            project.delete();
        }
        DataManager.overrideConfigProperty(nysdotEnabledField, prevNysdotEnabled);
    }

    /**
     * Create a feed source specific deployment for NYSDOT (i.e. nysdot enabled, router for each feed source)
     * @see FeedSourceSpecificDeploymentControllerTest#canCreateFeedSourceSpecificDeploymentWithDefaultRouter()
     */
    @Test
    public void canCreateFeedSourceSpecificDeploymentForNysdot() throws IOException {
        SimpleHttpResponse createDeploymentResponse = TestUtils.makeRequest( "/api/manager/secure/deployments/fromfeedsource/" + feedSource.id,
                null,
                HttpUtils.REQUEST_METHOD.POST
        );
        Deployment deployment = JsonUtil.getPOJOFromResponse(createDeploymentResponse, Deployment.class);

        assertEquals(OK_200, createDeploymentResponse.status);
        assertEquals(StringUtils.getCleanName(feedSource.name) + "_" + feedSource.id, deployment.routerId);
    }
}
