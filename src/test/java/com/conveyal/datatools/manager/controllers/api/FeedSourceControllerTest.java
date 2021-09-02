package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.TestUtils;
import com.conveyal.datatools.common.utils.Scheduler;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.models.FeedRetrievalMethod;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FetchFrequency;
import com.conveyal.datatools.manager.models.Label;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.HttpUtils;
import com.conveyal.datatools.manager.utils.SimpleHttpResponse;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static org.eclipse.jetty.http.HttpStatus.BAD_REQUEST_400;
import static org.eclipse.jetty.http.HttpStatus.OK_200;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


public class FeedSourceControllerTest extends DatatoolsTest {
    private static Project project = null;
    private static Project projectToBeDeleted = null;
    private static FeedSource feedSourceWithUrl = null;
    private static FeedSource feedSourceWithNoUrl = null;
    private static FeedSource feedSourceWithLabels = null;
    private static FeedSource feedSourceWithInvalidLabels = null;
    private static Label publicLabel = null;
    private static Label adminOnlyLabel = null;

    @BeforeAll
    public static void setUp() throws IOException {
        DatatoolsTest.setUp();
        Auth0Connection.setAuthDisabled(true);
        project = new Project();
        project.name = "ProjectOne";
        project.autoFetchFeeds = true;
        Persistence.projects.create(project);

        projectToBeDeleted = new Project();
        projectToBeDeleted.name = "ProjectTwo";
        projectToBeDeleted.autoFetchFeeds = false;
        Persistence.projects.create(projectToBeDeleted);

        feedSourceWithUrl = createFeedSource("FeedSourceOne", new URL("http://www.feedsource.com"), project);
        feedSourceWithNoUrl = createFeedSource("FeedSourceTwo", null, project);
        feedSourceWithLabels = createFeedSource("FeedSourceThree", null, projectToBeDeleted);
        feedSourceWithInvalidLabels = createFeedSource("FeedSourceFour", null, project);

        adminOnlyLabel = createLabel("Admin Only Label");
        adminOnlyLabel.adminOnly = true;
        publicLabel = createLabel("Public Label");
    }

    @AfterAll
    public static void tearDown() {
        Auth0Connection.setAuthDisabled(Auth0Connection.getDefaultAuthDisabled());
        if (project != null) {
            Persistence.projects.removeById(project.id);
        }
        if (projectToBeDeleted != null) {
            Persistence.projects.removeById(projectToBeDeleted.id);
        }
        if (feedSourceWithUrl != null) {
            Persistence.feedSources.removeById(feedSourceWithUrl.id);
        }
        if (feedSourceWithNoUrl != null) {
            Persistence.feedSources.removeById(feedSourceWithNoUrl.id);
        }
        if (publicLabel != null) {
            Persistence.labels.removeById(publicLabel.id);
        }
        if (adminOnlyLabel != null) {
            Persistence.labels.removeById(adminOnlyLabel.id);
        }
    }

    /**
     * Manipulate a feed source and confirm that it is correctly scheduled:
     *  1. Create a feed source with auto fetch enabled and confirm it is scheduled.
     *  2. Update the same feed source turning off auth fetch and confirm it is no longer scheduled.
     *  3. Update the same feed source turning auth fetch back on and confirm it is scheduled once more.
     */
    @Test
    public void createFeedSourceWithUrlTest() {
        // create a feed source.
        SimpleHttpResponse createFeedSourceResponse = TestUtils.makeRequest("/api/manager/secure/feedsource",
            JsonUtil.toJson(feedSourceWithUrl),
            HttpUtils.REQUEST_METHOD.POST
        );
        assertEquals(OK_200, createFeedSourceResponse.status);
        assertEquals(1, jobCountForFeed(feedSourceWithUrl.id));

        // update feed source to disable feed fetch.
        feedSourceWithUrl.retrievalMethod = FeedRetrievalMethod.MANUALLY_UPLOADED;
        SimpleHttpResponse updateFeedSourceResponse
            = TestUtils.makeRequest(String.format("/api/manager/secure/feedsource/%s", feedSourceWithUrl.id),
            JsonUtil.toJson(feedSourceWithUrl),
            HttpUtils.REQUEST_METHOD.PUT
        );
        assertEquals(OK_200, updateFeedSourceResponse.status);
        assertEquals(0, jobCountForFeed(feedSourceWithUrl.id));

        // update feed source to enable auth fetch once more.
        feedSourceWithUrl.retrievalMethod = FeedRetrievalMethod.FETCHED_AUTOMATICALLY;
        updateFeedSourceResponse
            = TestUtils.makeRequest(String.format("/api/manager/secure/feedsource/%s", feedSourceWithUrl.id),
            JsonUtil.toJson(feedSourceWithUrl),
            HttpUtils.REQUEST_METHOD.PUT
        );
        assertEquals(OK_200, updateFeedSourceResponse.status);
        assertEquals(1, jobCountForFeed(feedSourceWithUrl.id));
    }


    /**
     * Create a feed source without defining the feed source url. Confirm that the feed source is not scheduled.
     */
    @Test
    public void createFeedSourceWithNoUrlTest() {
        SimpleHttpResponse createFeedSourceResponse = TestUtils.makeRequest("/api/manager/secure/feedsource",
            JsonUtil.toJson(feedSourceWithNoUrl),
            HttpUtils.REQUEST_METHOD.POST
        );
        assertEquals(OK_200, createFeedSourceResponse.status);
        assertEquals(0, jobCountForFeed(feedSourceWithNoUrl.id));
    }


    /**
     * Create some labels, add them to the feed source make them admin only, and check that they don't appear if not an admin
     */
    @Test
    public void createFeedSourceWithLabels() throws IOException {
        // Create labels
        SimpleHttpResponse createFirstLabelResponse = TestUtils.makeRequest("/api/manager/secure/label",
                JsonUtil.toJson(publicLabel),
                HttpUtils.REQUEST_METHOD.POST
        );
        assertEquals(OK_200, createFirstLabelResponse.status);
        SimpleHttpResponse createSecondLabelResponse = TestUtils.makeRequest("/api/manager/secure/label",
                JsonUtil.toJson(adminOnlyLabel),
                HttpUtils.REQUEST_METHOD.POST
        );
        assertEquals(OK_200, createSecondLabelResponse.status);

        String firstLabelId = publicLabel.id;
        String secondLabelId = adminOnlyLabel.id;

        feedSourceWithLabels.labelIds.add(firstLabelId);
        feedSourceWithLabels.labelIds.add(secondLabelId);

        // Create feed source with invalid labels
        feedSourceWithInvalidLabels.labelIds.add("does not exist");
        SimpleHttpResponse createInvalidFeedSourceResponse = TestUtils.makeRequest("/api/manager/secure/feedsource",
                JsonUtil.toJson(feedSourceWithInvalidLabels),
                HttpUtils.REQUEST_METHOD.POST
        );
        assertEquals(BAD_REQUEST_400, createInvalidFeedSourceResponse.status);
        // Create feed source with labels
        SimpleHttpResponse createFeedSourceResponse = TestUtils.makeRequest("/api/manager/secure/feedsource",
                JsonUtil.toJson(feedSourceWithLabels),
                HttpUtils.REQUEST_METHOD.POST
        );
        assertEquals(OK_200, createFeedSourceResponse.status);


        // Test that they are assigned properly
        assertEquals(2, labelCountForFeed(feedSourceWithLabels.id));
        // Test that project shows only correct labels based on user auth
        assertEquals(2, labelCountforProject(feedSourceWithLabels.projectId, true));
        assertEquals(1, labelCountforProject(feedSourceWithLabels.projectId, false));

        // Test that feed source shows only correct labels based on user auth
        List<String> labelsSeenByAdmin = FeedSourceController.cleanFeedSourceForNonAdmins(feedSourceWithLabels, true).labelIds;
        List<String> labelsSeenByViewOnlyUser = FeedSourceController.cleanFeedSourceForNonAdmins(feedSourceWithLabels, false).labelIds;

        assertEquals(2, labelsSeenByAdmin.size());
        assertEquals(1, labelsSeenByViewOnlyUser.size());

        // Test that after deleting a label, it's deleted from the feed source and project
        SimpleHttpResponse deleteSecondLabelResponse = TestUtils.makeRequest("/api/manager/secure/label/" + adminOnlyLabel.id,
                null,
                HttpUtils.REQUEST_METHOD.DELETE
        );
        assertEquals(OK_200, deleteSecondLabelResponse.status);
        assertEquals(1, labelCountForFeed(feedSourceWithLabels.id));
        assertEquals(1, labelCountforProject(feedSourceWithLabels.projectId, true));

        // Test that labels are removed when deleting project
        assertEquals(1, Persistence.labels.getFiltered(eq("projectId", projectToBeDeleted.id)).size());

        projectToBeDeleted.delete();
        assertNull(Persistence.projects.getById(projectToBeDeleted.id));
        assertEquals(0, Persistence.labels.getFiltered(eq("projectId", projectToBeDeleted.id)).size());
    }

    /**
     * Helper method to create feed source.
     */
    private static FeedSource createFeedSource(String name, URL url, Project project) {
        FeedSource feedSource = new FeedSource();
        feedSource.fetchFrequency = FetchFrequency.MINUTES;
        feedSource.fetchInterval = 1;
        feedSource.deployable = false;
        feedSource.name = name;
        feedSource.projectId = project.id;
        feedSource.retrievalMethod = FeedRetrievalMethod.FETCHED_AUTOMATICALLY;
        feedSource.url = url;
        return feedSource;
    }

    /**
     * Helper method to create label
     */
    private static Label createLabel(String name) {
        return new Label(name, "A label used during testing", "#123", false, projectToBeDeleted.id);
    }

    /**
     * Provide the job count for a given feed source.
     */
    private int jobCountForFeed(String feedSourceId) {
        return Scheduler.scheduledJobsForFeedSources.get(feedSourceId).size();
    }

    /**
     * Provide the label count for a given feed source.
     */
    private int labelCountForFeed(String feedSourceId) {
        return Persistence.feedSources.getById(feedSourceId).labelIds.size();
    }

    /**
     * Provide the label count for a given project
     */
    private int labelCountforProject(String projectId, boolean isAdmin) {
        return Persistence.projects.getById(projectId).retrieveProjectLabels(isAdmin).size();
    }
}
