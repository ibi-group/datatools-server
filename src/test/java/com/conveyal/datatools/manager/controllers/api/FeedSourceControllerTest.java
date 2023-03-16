package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.TestUtils;
import com.conveyal.datatools.common.utils.Scheduler;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.FeedRetrievalMethod;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.FetchFrequency;
import com.conveyal.datatools.manager.models.Label;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.HttpUtils;
import com.conveyal.datatools.manager.utils.SimpleHttpResponse;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.conveyal.gtfs.validator.ValidationResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static org.eclipse.jetty.http.HttpStatus.BAD_REQUEST_400;
import static org.eclipse.jetty.http.HttpStatus.OK_200;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

    private static Project projectWithLatestDeployment = null;
    private static FeedSource feedSourceWithLatestDeploymentFeedVersion = null;
    private static FeedVersion feedVersionFromLatestDeployment = null;
    private static Deployment deploymentLatest = null;
    private static Deployment deploymentSuperseded = null;

    private static Project projectWithPinnedDeployment = null;
    private static FeedSource feedSourceWithPinnedDeploymentFeedVersion = null;
    private static FeedVersion feedVersionFromPinnedDeployment = null;
    private static Deployment deploymentPinned = null;

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

        // Feed version from latest deployment.
        projectWithLatestDeployment = new Project();
        projectWithLatestDeployment.id = "project-with-latest-deployment";
        Persistence.projects.create(projectWithLatestDeployment);
        feedSourceWithLatestDeploymentFeedVersion = createFeedSource(
            "feed-source-with-latest-deployment-feed-version",
            "FeedSource",
            null,
            projectWithLatestDeployment,
            true
        );
        LocalDate deployedSuperseded = LocalDate.of(2020, Month.MARCH, 12);
        LocalDate deployedEndDate = LocalDate.of(2021, Month.MARCH, 12);
        LocalDate deployedStartDate = LocalDate.of(2021, Month.MARCH, 1);
        feedVersionFromLatestDeployment = createFeedVersion(
            "feed-version-from-latest-deployment",
            feedSourceWithLatestDeploymentFeedVersion.id,
            deployedStartDate,
            deployedEndDate
        );
        deploymentSuperseded = createDeployment(
            "deployment-superseded",
            projectWithLatestDeployment,
            feedVersionFromLatestDeployment.id,
            deployedSuperseded
        );
        deploymentLatest = createDeployment(
            "deployment-latest",
            projectWithLatestDeployment,
            feedVersionFromLatestDeployment.id,
            deployedEndDate
        );

        // Feed version from pinned deployment.
        projectWithPinnedDeployment = new Project();
        projectWithPinnedDeployment.id = "project-with-pinned-deployment";
        Persistence.projects.create(projectWithPinnedDeployment);
        feedSourceWithPinnedDeploymentFeedVersion = createFeedSource(
            "feed-source-with-pinned-deployment-feed-version",
            "FeedSourceWithPinnedFeedVersion",
            null,
            projectWithPinnedDeployment,
            true
        );
        feedVersionFromPinnedDeployment = createFeedVersion(
            "feed-version-from-pinned-deployment",
            feedSourceWithPinnedDeploymentFeedVersion.id,
            LocalDate.of(2022, Month.NOVEMBER, 2)
        );
        deploymentPinned = createDeployment(
            "deployment-pinned",
            projectWithPinnedDeployment,
            feedVersionFromPinnedDeployment.id,
            deployedEndDate
        );
        projectWithPinnedDeployment.pinnedDeploymentId = deploymentPinned.id;
        Persistence.projects.replace(projectWithPinnedDeployment.id, projectWithPinnedDeployment);
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
        tearDownDeployedFeedVersion();
    }

    /**
     * These entities are removed separately so that if the need arises they can be kept.
     * This would then allow the Mongo queries defined in FeedSource#getFeedVersionFromLatestDeployment and
     * FeedSource#getFeedVersionFromPinnedDeployment to be tested.
     */
    private static void tearDownDeployedFeedVersion() {
        if (projectWithPinnedDeployment != null) {
            Persistence.projects.removeById(projectWithPinnedDeployment.id);
        }
        if (projectWithLatestDeployment != null) {
            Persistence.projects.removeById(projectWithLatestDeployment.id);
        }
        if (feedSourceWithLatestDeploymentFeedVersion != null) {
            Persistence.feedSources.removeById(feedSourceWithLatestDeploymentFeedVersion.id);
        }
        if (feedSourceWithPinnedDeploymentFeedVersion != null) {
            Persistence.feedSources.removeById(feedSourceWithPinnedDeploymentFeedVersion.id);
        }
        if (feedVersionFromLatestDeployment != null) {
            Persistence.feedVersions.removeById(feedVersionFromLatestDeployment.id);
        }
        if (feedVersionFromPinnedDeployment != null) {
            Persistence.feedVersions.removeById(feedVersionFromPinnedDeployment.id);
        }
        if (deploymentPinned != null) {
            Persistence.deployments.removeById(deploymentPinned.id);
        }
        if (deploymentLatest != null) {
            Persistence.deployments.removeById(deploymentLatest.id);
        }
        if (deploymentSuperseded != null) {
            Persistence.deployments.removeById(deploymentSuperseded.id);
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
    public void createFeedSourceWithLabels() {
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

    @Test
    void canRetrieveDeployedFeedVersionFromLatestDeployment() throws IOException {
        SimpleHttpResponse response = TestUtils.makeRequest(
            String.format(
                "/api/manager/secure/feedsource/%s",
                feedSourceWithLatestDeploymentFeedVersion.id
            ),
            null,
            HttpUtils.REQUEST_METHOD.GET
        );
        assertEquals(OK_200, response.status);
        FeedSource feedSource =
            JsonUtil.getPOJOFromResponse(
                response,
                FeedSource.class
            );
        assertNotNull(feedSource);
        assertEquals(feedSourceWithLatestDeploymentFeedVersion.id, feedSource.id);
        assertEquals(feedVersionFromLatestDeployment.id, feedSource.getDeployedFeedVersionId());
        assertEquals(feedVersionFromLatestDeployment.validationSummary().startDate, feedSource.getDeployedFeedVersionStartDate());
        assertEquals(feedVersionFromLatestDeployment.validationSummary().endDate, feedSource.getDeployedFeedVersionEndDate());
    }

    @Test
    void canRetrieveDeployedFeedVersionFromPinnedDeployment() throws IOException {
        SimpleHttpResponse response = TestUtils.makeRequest(
            String.format(
                "/api/manager/secure/feedsource/%s",
                feedSourceWithPinnedDeploymentFeedVersion.id
            ),
            null,
            HttpUtils.REQUEST_METHOD.GET
        );
        assertEquals(OK_200, response.status);
        FeedSource feedSource =
            JsonUtil.getPOJOFromResponse(
                response,
                FeedSource.class
            );
        assertNotNull(feedSource);
        assertEquals(feedSourceWithPinnedDeploymentFeedVersion.id, feedSource.id);
        assertEquals(feedVersionFromPinnedDeployment.id, feedSource.getDeployedFeedVersionId());
        assertEquals(feedVersionFromPinnedDeployment.validationSummary().endDate, feedSource.getDeployedFeedVersionEndDate());
        assertEquals(feedVersionFromPinnedDeployment.validationSummary().startDate, feedSource.getDeployedFeedVersionStartDate());
    }

    private static FeedSource createFeedSource(String name, URL url, Project project) {
        return createFeedSource(null, name, url, project, false);
    }

    /**
     * Helper method to create feed source.
     */
    private static FeedSource createFeedSource(String id, String name, URL url, Project project, boolean persist) {
        FeedSource feedSource = new FeedSource();
        if (id != null) feedSource.id = id;
        feedSource.fetchFrequency = FetchFrequency.MINUTES;
        feedSource.fetchInterval = 1;
        feedSource.deployable = false;
        feedSource.name = name;
        feedSource.projectId = project.id;
        feedSource.retrievalMethod = FeedRetrievalMethod.FETCHED_AUTOMATICALLY;
        feedSource.url = url;
        if (persist) Persistence.feedSources.create(feedSource);
        return feedSource;
    }

    /**
     * Helper method to create a deployment.
     */
    private static Deployment createDeployment(
        String id,
        Project project,
        String feedVersionId,
        LocalDate dateCreated
    ) {
        Deployment deployment = new Deployment();
        deployment.dateCreated = Date.from(dateCreated.atStartOfDay(ZoneId.systemDefault()).toInstant());
        deployment.feedVersionIds = Collections.singletonList(feedVersionId);
        deployment.projectId = project.id;
        deployment.id = id;
        Persistence.deployments.create(deployment);
        return deployment;
    }

    /**
     * Helper method to create a feed version with no start date.
     */
    private static FeedVersion createFeedVersion(String id, String feedSourceId, LocalDate endDate) {
        return createFeedVersion(id, feedSourceId, null, endDate);
    }

    /**
     * Helper method to create a feed version.
     */
    private static FeedVersion createFeedVersion(String id, String feedSourceId, LocalDate startDate, LocalDate endDate) {
        FeedVersion feedVersion = new FeedVersion();
        feedVersion.id = id;
        feedVersion.feedSourceId = feedSourceId;
        ValidationResult validationResult = new ValidationResult();
        validationResult.firstCalendarDate = startDate;
        validationResult.lastCalendarDate = endDate;
        feedVersion.validationResult = validationResult;
        Persistence.feedVersions.create(feedVersion);
        return feedVersion;
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
