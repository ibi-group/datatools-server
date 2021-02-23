package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.FeedRetrievalMethod;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.google.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;

import static com.conveyal.datatools.TestUtils.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AutoDeployFeedJobTest extends DatatoolsTest {
    private static final Logger LOG = LoggerFactory.getLogger(AutoDeployFeedJobTest.class);
    private static OtpServer server;
    private static Deployment deployment;
    private static Project project;
    private static FeedSource mockFeedSource;

    // These parameters are used solely by the failAutoDeployIfFetchStillInProgress which fails CI (on GitHub) if unique
    // parameters are not used.
    private static OtpServer fetchStillInProgress_server;
    private static Deployment fetchStillInProgress_deployment;
    private static Project fetchStillInProgress_project;
    private static FeedSource fetchStillInProgress_mockFeedSourceFakeJob;
    private static FeedSource fetchStillInProgress_mockFeedSource;
    private static FeedVersion fetchStillInProgress_feedVersion;
    private final String FEED_VERSION_ZIP_FOLDER_NAME = "fake-agency-expire-in-2099";

    @BeforeClass
    public static void setUp() throws IOException {
        DatatoolsTest.setUp();
        Auth0Connection.setAuthDisabled(true);
        LOG.info("{} setup", AutoDeployFeedJobTest.class.getSimpleName());

        server = createOtpServer();
        DeployJob.DeploySummary deploySummary = createDeploymentSummary(server.id);
        project = createProject();
        deployment = createDeployment(deploySummary, project.id);
        mockFeedSource = createFeedSource("Mock Feed Source", project.id);

        fetchStillInProgress_server = createOtpServer();
        DeployJob.DeploySummary deploySummaryFetchInProgress = createDeploymentSummary(fetchStillInProgress_server.id);
        fetchStillInProgress_project = createProject();
        fetchStillInProgress_deployment = createDeployment(deploySummaryFetchInProgress, fetchStillInProgress_project.id);
        fetchStillInProgress_mockFeedSourceFakeJob =
            createFeedSource("Mock Feed Source For Fetch In Progress Fake Job", fetchStillInProgress_project.id);
        fetchStillInProgress_mockFeedSource =
            createFeedSource("Mock Feed Source For Fetch In Progress", fetchStillInProgress_project.id);
    }

    @AfterClass
    public static void tearDown() {
        Auth0Connection.setAuthDisabled(Auth0Connection.getDefaultAuthDisabled());
        server.delete();
        deployment.delete();
        mockFeedSource.delete();
        Persistence.projects.removeById(project.id);

        fetchStillInProgress_mockFeedSource.delete();
        fetchStillInProgress_server.delete();
        fetchStillInProgress_deployment.delete();
        Persistence.projects.removeById(fetchStillInProgress_project.id);
        // Part of a job that is not started so the feed source has to be deleted straight from DB.
        Persistence.feedSources.removeById(fetchStillInProgress_mockFeedSourceFakeJob.id);
        if (fetchStillInProgress_feedVersion != null) {
            Persistence.feedVersions.removeById(fetchStillInProgress_feedVersion.id);
        }
    }

    @Test
    public void skipJobIfFeedSourceNotDeployable() throws IOException {
        setFeedSourceDeployable(mockFeedSource, false);
        setProjectAutoDeploy(project,true);
        setProjectPinnedDeploymentId(project, deployment.id);
        ProcessSingleFeedJob processSingleFeedJob = triggerProcessSingleFeedJob(mockFeedSource, FEED_VERSION_ZIP_FOLDER_NAME);
        MonitorableJob lastJob = getLastJob(processSingleFeedJob);
        // Verify that the auto deploy job was not added as a subjob.
        assertFalse(lastJob instanceof AutoDeployFeedJob);
    }

    @Test
    public void failIfDeploymentNotPinned() throws IOException {
        setFeedSourceDeployable(mockFeedSource, true);
        setProjectAutoDeploy(project,true);
        setProjectPinnedDeploymentId(project, null);
        ProcessSingleFeedJob processSingleFeedJob = triggerProcessSingleFeedJob(mockFeedSource, FEED_VERSION_ZIP_FOLDER_NAME);
        MonitorableJob lastJob = getLastJob(processSingleFeedJob);
        assertTrue(lastJob instanceof AutoDeployFeedJob);
        assertThat(lastJob.status.message, equalTo("Pinned deployment does not exist. Cancelling auto-deploy."));
    }

    @Test
    public void skipJobIfProjectNotAutoDeploy() throws IOException {
        setFeedSourceDeployable(mockFeedSource, true);
        setProjectAutoDeploy(project, false);
        setProjectPinnedDeploymentId(project, deployment.id);
        ProcessSingleFeedJob processSingleFeedJob = triggerProcessSingleFeedJob(mockFeedSource, FEED_VERSION_ZIP_FOLDER_NAME);
        MonitorableJob lastJob = getLastJob(processSingleFeedJob);
        // Verify that the auto deploy job was not added as a subjob.
        assertFalse(lastJob instanceof AutoDeployFeedJob);
    }

    @Test
    public void canAutoDeployFeedVersion() throws IOException {
        setFeedSourceDeployable(mockFeedSource, true);
        setProjectAutoDeploy(project, true);
        setProjectPinnedDeploymentId(project, deployment.id);
        ProcessSingleFeedJob processSingleFeedJob = triggerProcessSingleFeedJob(mockFeedSource, FEED_VERSION_ZIP_FOLDER_NAME);
        MonitorableJob lastJob = getLastJob(processSingleFeedJob);
        assertTrue(lastJob instanceof AutoDeployFeedJob);
        assertThat(lastJob.status.message, equalTo(String.format("New deploy job initiated for %s", server.name)));
    }

    @Test
    public void failIfFeedVersionHasHighSeverityErrorTypes() throws IOException {
        setFeedSourceDeployable(mockFeedSource,true);
        setProjectAutoDeploy(project, true);
        setProjectPinnedDeploymentId(project, deployment.id);
        ProcessSingleFeedJob processSingleFeedJob =
            triggerProcessSingleFeedJob(mockFeedSource,
                "fake-agency-with-only-calendar-expire-in-2099-with-unused-route");
        MonitorableJob lastJob = getLastJob(processSingleFeedJob);
        assertTrue(lastJob instanceof AutoDeployFeedJob);
        assertThat(lastJob.status.message, equalTo("Feed version has critical errors or is out of date. Cancelling auto-deploy."));
    }

    @Test
    public void failAutoDeployIfFetchStillInProgress() throws IOException {
        setFeedSourceDeployable(fetchStillInProgress_mockFeedSource, true);
        setProjectAutoDeploy(fetchStillInProgress_project, true);
        setProjectPinnedDeploymentId(fetchStillInProgress_project, fetchStillInProgress_deployment.id);

        // Create fake processing job for mock feed (don't actually start it, to keep it in the userJobsMap
        // indefinitely).
        File zipFile = zipFolderFiles(FEED_VERSION_ZIP_FOLDER_NAME);
        fetchStillInProgress_feedVersion = getFeedVersionFromGTFSFile(fetchStillInProgress_mockFeedSourceFakeJob, zipFile);
        Auth0UserProfile user = Auth0UserProfile.createTestAdminUser();
        Set<MonitorableJob> userJobs = Sets.newConcurrentHashSet();
        userJobs.add(new ProcessSingleFeedJob(fetchStillInProgress_feedVersion, user, true));
        DataManager.userJobsMap.put(user.getUser_id(), userJobs);

        // Add mock feed 1 to the deployment so that it is detected in the Deployment#hasFeedFetchesInProgress check
        // (called during mock feed 2's processing).
        Persistence.feedVersions.create(fetchStillInProgress_feedVersion);
        Collection<String> feedVersionIds = new ArrayList<>();
        feedVersionIds.add(fetchStillInProgress_feedVersion.id);
        fetchStillInProgress_deployment.feedVersionIds = feedVersionIds;
        Persistence.deployments.replace(fetchStillInProgress_deployment.id, fetchStillInProgress_deployment);

        // Process single feed job.
        ProcessSingleFeedJob processSingleFeedJob =
            triggerProcessSingleFeedJob(fetchStillInProgress_mockFeedSource, FEED_VERSION_ZIP_FOLDER_NAME);
        MonitorableJob lastJob = getLastJob(processSingleFeedJob);
        assertTrue(lastJob instanceof AutoDeployFeedJob);
        assertThat(lastJob.status.message, equalTo("Auto-deploy skipped because of feed fetches in progress."));
    }

    private void setFeedSourceDeployable(FeedSource feedSource, boolean deployable) {
        feedSource.deployable = deployable;
        Persistence.feedSources.replace(feedSource.id, feedSource);
    }

    private void setProjectAutoDeploy(Project project, boolean autoDeploy) {
        project.autoDeploy = autoDeploy;
        Persistence.projects.replace(project.id, project);
    }

    private void setProjectPinnedDeploymentId(Project project, String deploymentId) {
        project.pinnedDeploymentId = deploymentId;
        Persistence.projects.replace(project.id, project);
    }

    /**
     * Create and run a {@link ProcessSingleFeedJob}.
     */
    private ProcessSingleFeedJob triggerProcessSingleFeedJob(FeedSource feedSource, String zipFolderName) throws IOException {
        File zipFile = zipFolderFiles(zipFolderName);
        ProcessSingleFeedJob processSingleFeedJob = createProcessSingleFeedJob(feedSource, zipFile);
        processSingleFeedJob.run();
        return processSingleFeedJob;
    }

    private MonitorableJob getLastJob(ProcessSingleFeedJob processSingleFeedJob) {
        List<MonitorableJob> subJobs = processSingleFeedJob.getSubJobs();
        return subJobs.get(subJobs.size() - 1);
    }

    private static FeedSource createFeedSource(String name, String projectId) {
        FeedSource mockFeedSource = new FeedSource(name, projectId, FeedRetrievalMethod.MANUALLY_UPLOADED);
        Persistence.feedSources.create(mockFeedSource);
        return mockFeedSource;
    }

    private static OtpServer createOtpServer() {
        OtpServer server = new OtpServer();
        server.name = String.format("Test Server %s", new Date().toString());
        Persistence.servers.create(server);
        return server;
    }

    private static DeployJob.DeploySummary createDeploymentSummary(String serverId) {
        DeployJob.DeploySummary deploySummary = new DeployJob.DeploySummary();
        deploySummary.serverId  = serverId;
        return deploySummary;
    }

    private static Project createProject() {
        Project project = new Project();
        project.name = String.format("Test Project %s", new Date().toString());
        Persistence.projects.create(project);
        return project;
    }

    private static Deployment createDeployment(DeployJob.DeploySummary deploySummary, String projectId) {
        Deployment deployment = new Deployment();
        deployment.name = String.format("Test Deployment %s", new Date().toString());
        deployment.deployJobSummaries.add(deploySummary);
        deployment.projectId = projectId;
        Persistence.deployments.create(deployment);
        return deployment;
    }
}
