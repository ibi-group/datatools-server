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
    private static FeedSource mockFeedSource1;
    private static FeedSource mockFeedSource2;
    private static FeedSource mockFeedSource3;
    private static FeedSource mockFeedSource4;
    private static FeedSource mockFeedSource5;

    private static OtpServer serverFetchInProgress;
    private static Deployment deploymentFetchInProgress;
    private static Project projectFetchInProgress;
    private static FeedSource mockFeedSourceFetchInProgressFakeJob;
    private static FeedSource mockFeedSourceFetchInProgress;
    private static FeedVersion feedVersionFetchInProgress;
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
        mockFeedSource1 = createFeedSource("Mock Feed Source 1", project.id);
        mockFeedSource2 = createFeedSource("Mock Feed Source 2", project.id);
        mockFeedSource3 = createFeedSource("Mock Feed Source 3", project.id);
        mockFeedSource4 = createFeedSource("Mock Feed Source 4", project.id);
        mockFeedSource5 = createFeedSource("Mock Feed Source 5", project.id);

        serverFetchInProgress = createOtpServer();
        DeployJob.DeploySummary deploySummaryFetchInProgress = createDeploymentSummary(server.id);
        projectFetchInProgress = createProject();
        deploymentFetchInProgress = createDeployment(deploySummaryFetchInProgress, project.id);
        mockFeedSourceFetchInProgressFakeJob =
            createFeedSource("Mock Feed Source For Fetch In Progress Fake Job", projectFetchInProgress.id);
        mockFeedSourceFetchInProgress =
            createFeedSource("Mock Feed Source For Fetch In Progress", projectFetchInProgress.id);
    }

    @AfterClass
    public static void tearDown() {
        Auth0Connection.setAuthDisabled(Auth0Connection.getDefaultAuthDisabled());
        server.delete();
        deployment.delete();
        mockFeedSource1.delete();
        mockFeedSource2.delete();
        mockFeedSource3.delete();
        mockFeedSource4.delete();
        mockFeedSource5.delete();
        Persistence.projects.removeById(project.id);

        mockFeedSourceFetchInProgress.delete();
        serverFetchInProgress.delete();
        deploymentFetchInProgress.delete();
        Persistence.projects.removeById(projectFetchInProgress.id);
        // Part of a job that is not started so the feed source has to be deleted straight from DB.
        Persistence.feedSources.removeById(mockFeedSourceFetchInProgressFakeJob.id);
        if (feedVersionFetchInProgress != null) {
            Persistence.feedVersions.removeById(feedVersionFetchInProgress.id);
        }
    }

    @Test
    public void skipJobIfFeedSourceNotDeployable() throws IOException {
        setFeedSourceDeployable(mockFeedSource1, false);
        setProjectAutoDeploy(project,true);
        setProjectPinnedDeploymentId(project, deployment.id);
        ProcessSingleFeedJob processSingleFeedJob = triggerProcessSingleFeedJob(mockFeedSource1, FEED_VERSION_ZIP_FOLDER_NAME);
        MonitorableJob lastJob = getLastJob(processSingleFeedJob);
        // Verify that the auto deploy job was not added as a subjob.
        assertFalse(lastJob instanceof AutoDeployFeedJob);
    }

    @Test
    public void failIfDeploymentNotPinned() throws IOException {
        setFeedSourceDeployable(mockFeedSource2, true);
        setProjectAutoDeploy(project,true);
        setProjectPinnedDeploymentId(project, null);
        ProcessSingleFeedJob processSingleFeedJob = triggerProcessSingleFeedJob(mockFeedSource2, FEED_VERSION_ZIP_FOLDER_NAME);
        MonitorableJob lastJob = getLastJob(processSingleFeedJob);
        assertTrue(lastJob instanceof AutoDeployFeedJob);
        assertThat(lastJob.status.message, equalTo("Pinned deployment does not exist. Cancelling auto-deploy."));
    }

    @Test
    public void skipJobIfProjectNotAutoDeploy() throws IOException {
        setFeedSourceDeployable(mockFeedSource3, true);
        setProjectAutoDeploy(project, false);
        setProjectPinnedDeploymentId(project, deployment.id);
        ProcessSingleFeedJob processSingleFeedJob = triggerProcessSingleFeedJob(mockFeedSource3, FEED_VERSION_ZIP_FOLDER_NAME);
        MonitorableJob lastJob = getLastJob(processSingleFeedJob);
        // Verify that the auto deploy job was not added as a subjob.
        assertFalse(lastJob instanceof AutoDeployFeedJob);
    }

    @Test
    public void canAutoDeployFeedVersion() throws IOException {
        setFeedSourceDeployable(mockFeedSource4, true);
        setProjectAutoDeploy(project, true);
        setProjectPinnedDeploymentId(project, deployment.id);
        ProcessSingleFeedJob processSingleFeedJob = triggerProcessSingleFeedJob(mockFeedSource4, FEED_VERSION_ZIP_FOLDER_NAME);
        MonitorableJob lastJob = getLastJob(processSingleFeedJob);
        assertTrue(lastJob instanceof AutoDeployFeedJob);
        assertThat(lastJob.status.message, equalTo(String.format("New deploy job initiated for %s", server.name)));
    }

    @Test
    public void failIfFeedVersionHasHighSeverityErrorTypes() throws IOException {
        setFeedSourceDeployable(mockFeedSource5,true);
        setProjectAutoDeploy(project, true);
        setProjectPinnedDeploymentId(project, deployment.id);
        ProcessSingleFeedJob processSingleFeedJob =
            triggerProcessSingleFeedJob(mockFeedSource5,
                "fake-agency-with-only-calendar-expire-in-2099-with-unused-route");
        MonitorableJob lastJob = getLastJob(processSingleFeedJob);
        assertTrue(lastJob instanceof AutoDeployFeedJob);
        assertThat(lastJob.status.message, equalTo("Feed version has critical errors or is out of date. Cancelling auto-deploy."));
    }

    @Test
    public void failAutoDeployIfFetchStillInProgress() throws IOException {
        setFeedSourceDeployable(mockFeedSourceFetchInProgress, true);
        setProjectAutoDeploy(projectFetchInProgress, true);
        setProjectPinnedDeploymentId(projectFetchInProgress, deploymentFetchInProgress.id);

        // Create fake processing job for mock feed (don't actually start it, to keep it in the userJobsMap
        // indefinitely).
        File zipFile = zipFolderFiles(FEED_VERSION_ZIP_FOLDER_NAME);
        feedVersionFetchInProgress = getFeedVersionFromGTFSFile(mockFeedSourceFetchInProgressFakeJob, zipFile);
        Auth0UserProfile user = Auth0UserProfile.createTestAdminUser();
        Set<MonitorableJob> userJobs = Sets.newConcurrentHashSet();
        userJobs.add(new ProcessSingleFeedJob(feedVersionFetchInProgress, user, true));
        DataManager.userJobsMap.put(user.getUser_id(), userJobs);

        // Add mock feed 1 to the deployment so that it is detected in the Deployment#hasFeedFetchesInProgress check
        // (called during mock feed 2's processing).
        Persistence.feedVersions.create(feedVersionFetchInProgress);
        Collection<String> feedVersionIds = new ArrayList<>();
        feedVersionIds.add(feedVersionFetchInProgress.id);
        deploymentFetchInProgress.feedVersionIds = feedVersionIds;
        Persistence.deployments.replace(deploymentFetchInProgress.id, deploymentFetchInProgress);

        // Process single feed job.
        ProcessSingleFeedJob processSingleFeedJob =
            triggerProcessSingleFeedJob(mockFeedSourceFetchInProgress, FEED_VERSION_ZIP_FOLDER_NAME);
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
