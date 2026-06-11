package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.TestUtils;
import com.conveyal.datatools.common.utils.Scheduler;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.gtfsplus.GtfsPlusValidation;
import com.conveyal.datatools.manager.gtfsplus.ValidationIssue;
import com.conveyal.datatools.manager.jobs.ProcessSingleFeedJob;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.FeedRetrievalMethod;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedSourceSummary;
import com.conveyal.datatools.manager.models.FeedValidationResultSummary;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.FeedVersionSummary;
import com.conveyal.datatools.manager.models.FetchFrequency;
import com.conveyal.datatools.manager.models.Label;
import com.conveyal.datatools.manager.models.Note;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.models.PublishState;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.HttpUtils;
import com.conveyal.datatools.manager.utils.SimpleHttpResponse;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.conveyal.gtfs.validator.ValidationResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Stream;

import static com.conveyal.datatools.TestUtils.createFeedVersionFromGtfsZip;
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
    private static FeedVersion feedVersionFromLatestDeploymentVersion2 = null;
    private static FeedVersion feedVersionPublishedFromLatestDeployment = null;

    private static Project projectWithPinnedDeployment = null;
    private static FeedSource feedSourceWithPinnedDeploymentFeedVersion = null;
    private static FeedVersion feedVersionFromPinnedDeployment = null;

    @BeforeAll
    public static void setUp() throws IOException {
        DatatoolsTest.setUp();
        Auth0Connection.setAuthDisabled(true);
        ProcessSingleFeedJob.ENABLE_ADDITIONAL_VALIDATION = false;

        project = createProject("ProjectOne", true);
        projectToBeDeleted = createProject("ProjectTwo", false);

        feedSourceWithUrl = createFeedSource("FeedSourceOne", new URL("http://www.feedsource.com"), project);
        feedSourceWithNoUrl = createFeedSource("FeedSourceTwo", null, project);
        feedSourceWithLabels = createFeedSource("FeedSourceThree", new URL("http://www.feedsource.com"), projectToBeDeleted);
        feedSourceWithInvalidLabels = createFeedSource("FeedSourceFour", new URL("http://www.feedsource.com"), project);

        adminOnlyLabel = createLabel("Admin Only Label", projectToBeDeleted.id);
        adminOnlyLabel.adminOnly = true;
        publicLabel = createLabel("Public Label", projectToBeDeleted.id);

        setUpFeedVersionFromLatestDeployment();
        setUpFeedVersionFromPinnedDeployment();
    }

    /**
     * Create all the required objects to test a feed version from the latest deployment.
     */
    private static void setUpFeedVersionFromLatestDeployment() throws MalformedURLException {
        projectWithLatestDeployment = new Project();
        projectWithLatestDeployment.id = "project-with-latest-deployment";
        projectWithLatestDeployment.organizationId = "project-with-latest-deployment-org-id";
        Persistence.projects.create(projectWithLatestDeployment);

        Label feedSourceWithLatestDeploymentAdminOnlyLabel = createLabel(
            "label-id-latest-deployment", "Admin Only Label", projectWithLatestDeployment.id
        );
        Note feedSourceWithLatestDeploymentAdminOnlyNote = createNote("note-id-latest-deployment", "A test note");

        feedSourceWithLatestDeploymentFeedVersion = createFeedSource(
            "feed-source-with-latest-deployment-feed-version",
            "FeedSource",
            new URL("http://www.feedsource.com"),
            projectWithLatestDeployment,
            true,
            List.of(feedSourceWithLatestDeploymentAdminOnlyLabel.id),
            List.of(feedSourceWithLatestDeploymentAdminOnlyNote.id)
        );

        LocalDate deployedSuperseded = LocalDate.of(2020, Month.MARCH, 12);
        LocalDate deployedEndDate = LocalDate.of(2021, Month.MARCH, 12);
        LocalDate deployedStartDate = LocalDate.of(2021, Month.MARCH, 1);
        // Create feed version 1 to be superseded by feed version 2 to confirm that the newer version is the one retrieved.
        createFeedVersion(
            "feed-version-from-latest-deployment-1",
            feedSourceWithLatestDeploymentFeedVersion.id,
            deployedStartDate,
            deployedEndDate,
            null,
            1
        );
        feedVersionFromLatestDeploymentVersion2 = createFeedVersion(
            "feed-version-from-latest-deployment-2",
            feedSourceWithLatestDeploymentFeedVersion.id,
            deployedStartDate,
            deployedEndDate,
            null,
            2
        );

        FeedVersion feedVersionFromGtfsZip = createFeedVersionFromGtfsZip(
            feedSourceWithLatestDeploymentFeedVersion,
            "bart_old.zip"
        );
        // Update the feed version namespace to match that created from the import.
        feedVersionFromLatestDeploymentVersion2.namespace = feedVersionFromGtfsZip.namespace;

        // Remove the imported feed version so it does not conflict with the latest deployment feed version.
        Persistence.feedVersions.removeById(feedVersionFromGtfsZip.id);
        Persistence.feedVersions.replace(feedVersionFromLatestDeploymentVersion2.id, feedVersionFromLatestDeploymentVersion2);

        feedVersionPublishedFromLatestDeployment = createFeedVersion(
            "published-feed-version-from-latest-deployment",
            feedSourceWithLatestDeploymentFeedVersion.id,
            LocalDate.of(2022, Month.NOVEMBER, 2),
            LocalDate.of(2022, Month.NOVEMBER, 3),
            feedSourceWithLatestDeploymentFeedVersion.publishedVersionId,
            0
        );
        createDeployment(
            "deployment-superseded",
            projectWithLatestDeployment,
            feedVersionFromLatestDeploymentVersion2.id,
            deployedSuperseded
        );
        createDeployment(
            "deployment-latest",
            projectWithLatestDeployment,
            feedVersionFromLatestDeploymentVersion2.id,
            deployedEndDate
        );
    }

    /**
     * Create all the required objects to test a feed version from a pinned deployment.
     */
    private static void setUpFeedVersionFromPinnedDeployment() throws MalformedURLException {
        projectWithPinnedDeployment = new Project();
        projectWithPinnedDeployment.id = "project-with-pinned-deployment";
        projectWithPinnedDeployment.organizationId = "project-with-pinned-deployment-org-id";
        Persistence.projects.create(projectWithPinnedDeployment);

        Label feedSourceWithPinnedDeploymentAdminOnlyLabel = createLabel("label-id-pinned-deployment", "Admin Only Label", projectWithPinnedDeployment.id);
        Note feedSourceWithPinnedDeploymentAdminOnlyNote = createNote("note-id-pinned-deployment", "A test note");

        feedSourceWithPinnedDeploymentFeedVersion = createFeedSource(
            "feed-source-with-pinned-deployment-feed-version",
            "FeedSourceWithPinnedFeedVersion",
            new URL("http://www.feedsource.com"),
            projectWithPinnedDeployment,
            true,
            List.of(feedSourceWithPinnedDeploymentAdminOnlyLabel.id),
            List.of(feedSourceWithPinnedDeploymentAdminOnlyNote.id)
        );
        feedVersionFromPinnedDeployment = createFeedVersion(
            "feed-version-from-pinned-deployment",
            feedSourceWithPinnedDeploymentFeedVersion.id,
            LocalDate.of(2022, Month.NOVEMBER, 2)
        );
        Deployment deploymentPinned = createDeployment(
            "deployment-pinned",
            projectWithPinnedDeployment,
            feedVersionFromPinnedDeployment.id,
            LocalDate.of(2021, Month.MARCH, 12)
        );
        projectWithPinnedDeployment.pinnedDeploymentId = deploymentPinned.id;
        Persistence.projects.replace(projectWithPinnedDeployment.id, projectWithPinnedDeployment);
    }

    @AfterAll
    public static void tearDown() {
        Auth0Connection.setAuthDisabled(Auth0Connection.getDefaultAuthDisabled());
        if (project != null) {
            project.delete();
        }
        if (projectToBeDeleted != null) {
            projectToBeDeleted.delete();
        }
        tearDownDeployedFeedVersion();
        ProcessSingleFeedJob.ENABLE_ADDITIONAL_VALIDATION = true;
    }

    /**
     * These projects are removed separately so that if the need arises they can be kept.
     * This would then allow the Mongo queries defined in FeedSource#getFeedVersionFromLatestDeployment and
     * FeedSource#getFeedVersionFromPinnedDeployment to be tested.
     */
    private static void tearDownDeployedFeedVersion() {
        if (projectWithPinnedDeployment != null) {
            projectWithPinnedDeployment.delete();
        }
        if (projectWithLatestDeployment != null) {
            projectWithLatestDeployment.delete();
        }
        // Orphan feed version must be explicitly deleted.
        if (feedVersionPublishedFromLatestDeployment != null) {
            Persistence.feedVersions.removeById(feedVersionPublishedFromLatestDeployment.id);
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
        assertEquals(2, labelCountForProject(feedSourceWithLabels.projectId, true));
        assertEquals(1, labelCountForProject(feedSourceWithLabels.projectId, false));

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
        assertEquals(1, labelCountForProject(feedSourceWithLabels.projectId, true));

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
                "/api/manager/secure/feedsourceSummaries?projectId=%s",
                feedSourceWithLatestDeploymentFeedVersion.projectId
            ),
            null,
            HttpUtils.REQUEST_METHOD.GET
        );
        assertEquals(OK_200, response.status);

        List<FeedSourceSummary> feedSourceSummaries =
            JsonUtil.getPOJOFromJSONAsList(
                JsonUtil.getJsonNodeFromResponse(response),
                FeedSourceSummary.class
            );

        assertNotNull(feedSourceSummaries);
        FeedSourceSummary firstSummary = feedSourceSummaries.get(0);
        FeedValidationResultSummary expectedValidationSummary = feedVersionFromLatestDeploymentVersion2.validationSummary();
        assertEquals(feedSourceWithLatestDeploymentFeedVersion.id, firstSummary.id);
        assertEquals(feedSourceWithLatestDeploymentFeedVersion.projectId, firstSummary.projectId);
        assertEquals(feedSourceWithLatestDeploymentFeedVersion.labelIds, firstSummary.labelIds);
        assertEquals(feedSourceWithLatestDeploymentFeedVersion.url.toString(), firstSummary.url);
        assertEquals(feedSourceWithLatestDeploymentFeedVersion.noteIds, firstSummary.noteIds);
        assertEquals(feedSourceWithLatestDeploymentFeedVersion.organizationId(), firstSummary.organizationId);
        assertEquals(feedVersionFromLatestDeploymentVersion2.id, firstSummary.deployedFeedVersionId);
        assertEquals(expectedValidationSummary.startDate, firstSummary.deployedFeedVersionStartDate);
        assertEquals(expectedValidationSummary.endDate, firstSummary.deployedFeedVersionEndDate);
        assertEquals(expectedValidationSummary.errorCount, firstSummary.deployedFeedVersionIssues);
        assertEquals(feedVersionFromLatestDeploymentVersion2.id, firstSummary.latestValidation.feedVersionId);
        assertEquals(expectedValidationSummary.startDate, firstSummary.latestValidation.startDate);
        assertEquals(expectedValidationSummary.endDate, firstSummary.latestValidation.endDate);
        assertEquals(expectedValidationSummary.errorCount, firstSummary.latestValidation.errorCount);
        assertEquals(feedVersionFromLatestDeploymentVersion2.sentToExternalPublisher, firstSummary.latestSentToExternalPublisher);
        assertEquals(PublishState.PUBLISH_BLOCKED, firstSummary.publishState);
        assertEquals(feedSourceWithLatestDeploymentFeedVersion.publishedVersionId, firstSummary.publishedVersionId);
        assertEquals(feedVersionPublishedFromLatestDeployment.validationResult.errorCount, firstSummary.publishedValidationSummary.errorCount);
        assertEquals(feedVersionPublishedFromLatestDeployment.validationResult.firstCalendarDate, firstSummary.publishedValidationSummary.startDate);
        assertEquals(feedVersionPublishedFromLatestDeployment.validationResult.lastCalendarDate, firstSummary.publishedValidationSummary.endDate);
    }

    @Test
    void canRetrieveDeployedFeedVersionFromPinnedDeployment() throws IOException {
        SimpleHttpResponse response = TestUtils.makeRequest(
            String.format(
                "/api/manager/secure/feedsourceSummaries?projectId=%s",
                feedSourceWithPinnedDeploymentFeedVersion.projectId
            ),
            null,
            HttpUtils.REQUEST_METHOD.GET
        );
        assertEquals(OK_200, response.status);

        List<FeedSourceSummary> feedSourceSummaries =
            JsonUtil.getPOJOFromJSONAsList(
                JsonUtil.getJsonNodeFromResponse(response),
                FeedSourceSummary.class
            );
        assertNotNull(feedSourceSummaries);
        FeedSourceSummary firstSummary = feedSourceSummaries.get(0);
        FeedValidationResultSummary expectedValidationSummary = feedVersionFromPinnedDeployment.validationSummary();

        assertEquals(feedSourceWithPinnedDeploymentFeedVersion.id, firstSummary.id);
        assertEquals(feedSourceWithPinnedDeploymentFeedVersion.projectId, firstSummary.projectId);
        assertEquals(feedSourceWithPinnedDeploymentFeedVersion.labelIds, firstSummary.labelIds);
        assertEquals(feedSourceWithPinnedDeploymentFeedVersion.url.toString(), firstSummary.url);
        assertEquals(feedSourceWithPinnedDeploymentFeedVersion.noteIds, firstSummary.noteIds);
        assertEquals(feedSourceWithPinnedDeploymentFeedVersion.organizationId(), firstSummary.organizationId);
        assertEquals(feedVersionFromPinnedDeployment.id, firstSummary.deployedFeedVersionId);
        assertEquals(expectedValidationSummary.startDate, firstSummary.deployedFeedVersionStartDate);
        assertEquals(expectedValidationSummary.endDate, firstSummary.deployedFeedVersionEndDate);
        assertEquals(expectedValidationSummary.errorCount, firstSummary.deployedFeedVersionIssues);
        assertEquals(feedVersionFromPinnedDeployment.id, firstSummary.latestValidation.feedVersionId);
        assertEquals(expectedValidationSummary.startDate, firstSummary.latestValidation.startDate);
        assertEquals(expectedValidationSummary.endDate, firstSummary.latestValidation.endDate);
        assertEquals(expectedValidationSummary.errorCount, firstSummary.latestValidation.errorCount);
        assertEquals(feedVersionFromPinnedDeployment.sentToExternalPublisher, firstSummary.latestSentToExternalPublisher);
        assertEquals(PublishState.PUBLISH_BLOCKED, firstSummary.publishState);
        assertEquals(feedSourceWithPinnedDeploymentFeedVersion.publishedVersionId, firstSummary.publishedVersionId);
    }

    @ParameterizedTest
    @MethodSource("createPublishStates")
    void canDeterminePublishState(
        boolean isPublished,
        boolean isPublishing,
        boolean isPublishBlocked,
        boolean isFeedLoading,
        PublishState expectedPublishState
    ) {
        FeedSourceSummary feedSourceSummary = new FeedSourceSummary();
        FeedVersionSummary feedVersionSummary = new FeedVersionSummary();
        FeedVersion.setDateOverrideForTesting(LocalDate.now());

        if (isPublished) {
            feedVersionSummary.namespace = feedVersionSummary.feedSourcePublishedVersionId = "namespace";
        }
        if (isPublishing) {
            feedVersionSummary.sentToExternalPublisher = new Date();
            feedVersionSummary.processedByExternalPublisher = null;
        }
        if (isPublishBlocked) {
            feedVersionSummary.gtfsPlusValidation = null;
        }
        if (!isPublished && !isPublishing && !isPublishBlocked && !isFeedLoading) {
            // Ready to publish case.
            FeedVersionSummary.setHasBlockingIssueForPublishingOverrideForTesting(false);
            passPublishBlockedCheck(feedVersionSummary);

            feedVersionSummary.validationResult.errorCount = 1;
            feedVersionSummary.id = "feed-version-id";
            feedSourceSummary.id = "feed-source-id";
        }
        PublishState publishState = feedVersionSummary.getPublishState();
        assertEquals(expectedPublishState, publishState);
        FeedVersionSummary.setHasBlockingIssueForPublishingOverrideForTesting(null);
    }

    /**
     * Set up a feed version summary to pass the publish blocked check.
     */
    private static void passPublishBlockedCheck(FeedVersionSummary feedVersionSummary) {
        feedVersionSummary.gtfsPlusValidation = new GtfsPlusValidation();
        feedVersionSummary.gtfsPlusValidation.issues = new ArrayList<>();
        feedVersionSummary.gtfsPlusValidation.published = true;
        feedVersionSummary.validationResult = new ValidationResult();
        feedVersionSummary.validationResult.lastCalendarDate = LocalDate.now().plusDays(1);
        FeedVersion.setDateOverrideForTesting(LocalDate.now());
    }

    private static Stream<Arguments> createPublishStates() {
        return Stream.of(
            Arguments.of(
                true, false, false, false, PublishState.PUBLISHED
            ),
            Arguments.of(
                false, true, false, false, PublishState.PUBLISHING
            ),
            Arguments.of(
                false, false, true, false, PublishState.PUBLISH_BLOCKED
            ),
            Arguments.of(
                false, false, false, false, PublishState.READY_TO_PUBLISH
            )
        );
    }

    private static Project createProject(String name, boolean autoFetchFeeds) {
        Project project = new Project();
        project.name = name;
        project.autoFetchFeeds = autoFetchFeeds;
        Persistence.projects.create(project);
        return project;
    }

    private static FeedSource createFeedSource(String name, URL url, Project project) {
        return createFeedSource(null, name, url, project, false);
    }

    /**
     * Helper method to create feed source.
     */
    private static FeedSource createFeedSource(String id, String name, URL url, Project project, boolean persist) {
        return createFeedSource(id, name, url, project, persist, null, null);
    }
    private static FeedSource createFeedSource(
        String id,
        String name,
        URL url,
        Project project,
        boolean persist,
        List<String> labels,
        List<String> notes
    ) {
        FeedSource feedSource = new FeedSource();
        if (id != null) feedSource.id = id;
        feedSource.fetchFrequency = FetchFrequency.MINUTES;
        feedSource.fetchInterval = 1;
        feedSource.deployable = false;
        feedSource.name = name;
        feedSource.projectId = project.id;
        feedSource.retrievalMethod = FeedRetrievalMethod.FETCHED_AUTOMATICALLY;
        feedSource.url = url;
        feedSource.publishedVersionId = "published-version-id-1";
        if (labels != null) feedSource.labelIds = labels;
        if (notes != null) feedSource.noteIds = notes;
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
        return createFeedVersion(id, feedSourceId, null, endDate, null, 0);
    }

    /**
     * Helper method to create a feed version.
     */
    private static FeedVersion createFeedVersion(
        String id,
        String feedSourceId,
        LocalDate startDate,
        LocalDate endDate,
        String namespace,
        int version
    ) {
        FeedVersion feedVersion = new FeedVersion();
        feedVersion.id = id;
        feedVersion.feedSourceId = feedSourceId;
        ValidationResult validationResult = new ValidationResult();
        validationResult.firstCalendarDate = startDate;
        validationResult.lastCalendarDate = endDate;
        validationResult.errorCount = 5 + (int)(Math.random() * ((1000 - 5) + 1));
        feedVersion.validationResult = validationResult;
        feedVersion.processedByExternalPublisher = new Date();
        feedVersion.sentToExternalPublisher = new Date();
        List<ValidationIssue> issues = List.of(
            new ValidationIssue("Test issue 1", "stops.txt", 1, "stop_id"),
            new ValidationIssue("Test issue 2", "stops.txt", 2, "stop_id")
        );
        feedVersion.gtfsPlusValidation = new GtfsPlusValidation(id, true, issues);
        feedVersion.namespace = namespace != null ? namespace : "feed-version-namespace";
        feedVersion.version = version;
        Persistence.feedVersions.create(feedVersion);
        return feedVersion;
    }

    /**
     * Helper method to create note.
     */
    private static Note createNote(String id, String body) {
        Note note = new Note(id, body, false);
        Persistence.notes.create(note);
        return note;
    }

    /**
     * Helper method to create label. If the id is provided save the label now if not defer to test to save.
     */
    private static Label createLabel(String id, String name, String projectId) {
        Label label;
        if (id != null) {
            label = new Label(id, name, "A label used during testing", "#123", false, projectId);
            Persistence.labels.create(label);
        } else {
            label = new Label(name, "A label used during testing", "#123", false, projectId);
        }
        return label;
    }

    /**
     * Helper method to create label.
     */
    private static Label createLabel(String name, String projectId) {
        return createLabel(null, name, projectId);
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
    private int labelCountForProject(String projectId, boolean isAdmin) {
        return Persistence.projects.getById(projectId).retrieveProjectLabels(isAdmin).size();
    }
}
