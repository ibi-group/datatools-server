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
    private static FeedSource mockFeedSource6;
    private static FeedSource mockFeedSourceForFakeJob;
    private static FeedVersion feedVersion;
    private final String FEED_VERSION_ZIP_FOLDER_NAME = "fake-agency-expire-in-2099";

    @BeforeClass
    public static void setUp() throws IOException {
        DatatoolsTest.setUp();
        Auth0Connection.setAuthDisabled(true);
        LOG.info("{} setup", AutoDeployFeedJobTest.class.getSimpleName());
        String testName = String.format("Test %s", new Date().toString());

        // OTP server instance required by deployment.
        server = new OtpServer();
        server.name = testName;
        Persistence.servers.create(server);

        // deployment.latest() is required to return the latest server id
        DeployJob.DeploySummary deploySummary = new DeployJob.DeploySummary();
        deploySummary.serverId  = server.id;

        // a project is required so the pinned deployment id can be defined
        project = new Project();
        project.name = testName;
        Persistence.projects.create(project);

        deployment = new Deployment();
        deployment.name = "Test deployment";
        deployment.deployJobSummaries.add(deploySummary);
        deployment.projectId = project.id;
        Persistence.deployments.create(deployment);

        mockFeedSource1 = createFeedSource("Mock Feed Source 1");
        mockFeedSource2 = createFeedSource("Mock Feed Source 2");
        mockFeedSource3 = createFeedSource("Mock Feed Source 3");
        mockFeedSource4 = createFeedSource("Mock Feed Source 4");
        mockFeedSource5 = createFeedSource("Mock Feed Source 5");
        mockFeedSource6 = createFeedSource("Mock Feed Source 6");
        mockFeedSourceForFakeJob = createFeedSource("Mock Feed Source For Fake Job");
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
        mockFeedSource6.delete();
        Persistence.projects.removeById(project.id);
        // Part of a job that is not started so the feed source has to be deleted straight from DB.
        Persistence.feedSources.removeById(mockFeedSourceForFakeJob.id);
        if (feedVersion != null) {
            Persistence.feedVersions.removeById(feedVersion.id);
        }
    }

    @Test
    public void skipJobIfFeedSourceNotDeployable() throws IOException {
        setFeedSourceDeployable(mockFeedSource1, false);
        setProjectAutoDeploy(true);
        setProjectPinnedDeploymentId(deployment.id);
        ProcessSingleFeedJob processSingleFeedJob = triggerProcessSingleFeedJob(mockFeedSource1, FEED_VERSION_ZIP_FOLDER_NAME);
        MonitorableJob lastJob = getLastJob(processSingleFeedJob);
        // Verify that the auto deploy job was not added as a subjob.
        assertFalse(lastJob instanceof AutoDeployFeedJob);
    }

    @Test
    public void failIfDeploymentNotPinned() throws IOException {
        setFeedSourceDeployable(mockFeedSource2, true);
        setProjectAutoDeploy(true);
        setProjectPinnedDeploymentId(null);
        ProcessSingleFeedJob processSingleFeedJob = triggerProcessSingleFeedJob(mockFeedSource2, FEED_VERSION_ZIP_FOLDER_NAME);
        MonitorableJob lastJob = getLastJob(processSingleFeedJob);
        assertTrue(lastJob instanceof AutoDeployFeedJob);
        assertThat(lastJob.status.message, equalTo("Pinned deployment does not exist. Cancelling auto-deploy."));
    }

    @Test
    public void skipJobIfProjectNotAutoDeploy() throws IOException {
        setFeedSourceDeployable(mockFeedSource3, true);
        setProjectAutoDeploy(false);
        setProjectPinnedDeploymentId(deployment.id);
        ProcessSingleFeedJob processSingleFeedJob = triggerProcessSingleFeedJob(mockFeedSource3, FEED_VERSION_ZIP_FOLDER_NAME);
        MonitorableJob lastJob = getLastJob(processSingleFeedJob);
        // Verify that the auto deploy job was not added as a subjob.
        assertFalse(lastJob instanceof AutoDeployFeedJob);
    }

    @Test
    public void canAutoDeployFeedVersion() throws IOException {
        setFeedSourceDeployable(mockFeedSource4, true);
        setProjectAutoDeploy(true);
        setProjectPinnedDeploymentId(deployment.id);
        ProcessSingleFeedJob processSingleFeedJob = triggerProcessSingleFeedJob(mockFeedSource4, FEED_VERSION_ZIP_FOLDER_NAME);
        MonitorableJob lastJob = getLastJob(processSingleFeedJob);
        assertTrue(lastJob instanceof AutoDeployFeedJob);
        assertThat(lastJob.status.message, equalTo(String.format("New deploy job initiated for %s", server.name)));
    }

    @Test
    public void failAutoDeployIfFetchStillInProgress() throws IOException {
        setFeedSourceDeployable(mockFeedSource5, true);
        setProjectAutoDeploy(true);
        setProjectPinnedDeploymentId(deployment.id);

        // Create fake processing job for mock feed (don't actually start it, to keep it in the userJobsMap
        // indefinitely).
        File zipFile = zipFolderFiles(FEED_VERSION_ZIP_FOLDER_NAME);
        feedVersion = getFeedVersionFromGTFSFile(mockFeedSourceForFakeJob, zipFile);
        Auth0UserProfile user = Auth0UserProfile.createTestAdminUser();
        Set<MonitorableJob> userJobs = Sets.newConcurrentHashSet();
        userJobs.add(new ProcessSingleFeedJob(feedVersion, user, true));
        DataManager.userJobsMap.put(user.getUser_id(), userJobs);

        // Add mock feed 1 to the deployment so that it is detected in the Deployment#hasFeedFetchesInProgress check
        // (called during mock feed 2's processing).
        Persistence.feedVersions.create(feedVersion);
        Collection<String> feedVersionIds = new ArrayList<>();
        feedVersionIds.add(feedVersion.id);
        deployment.feedVersionIds = feedVersionIds;
        Persistence.deployments.replace(deployment.id, deployment);

        // Process single feed job.
        ProcessSingleFeedJob processSingleFeedJob =
            triggerProcessSingleFeedJob(mockFeedSource5, FEED_VERSION_ZIP_FOLDER_NAME);
        MonitorableJob lastJob = getLastJob(processSingleFeedJob);
        assertTrue(lastJob instanceof AutoDeployFeedJob);
        assertThat(lastJob.status.message, equalTo("Auto-deploy skipped because of feed fetches in progress."));
    }

    @Test
    public void failIfFeedVersionHasHighSeverityErrorTypes() throws IOException {
        setFeedSourceDeployable(mockFeedSource6,true);
        setProjectAutoDeploy(true);
        setProjectPinnedDeploymentId(deployment.id);
        ProcessSingleFeedJob processSingleFeedJob =
            triggerProcessSingleFeedJob(mockFeedSource6,
                "fake-agency-with-only-calendar-expire-in-2099-with-unused-route");
        MonitorableJob lastJob = getLastJob(processSingleFeedJob);
        assertTrue(lastJob instanceof AutoDeployFeedJob);
        assertThat(lastJob.status.message, equalTo("Feed version has critical errors or is out of date. Cancelling auto-deploy."));
    }

    private void setFeedSourceDeployable(FeedSource feedSource, boolean deployable) {
        feedSource.deployable = deployable;
        Persistence.feedSources.replace(feedSource.id, feedSource);
    }

    private void setProjectAutoDeploy(boolean autoDeploy) {
        project.autoDeploy = autoDeploy;
        Persistence.projects.replace(project.id, project);
    }

    private void setProjectPinnedDeploymentId(String deploymentId) {
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

    private static FeedSource createFeedSource(String name) {
        FeedSource mockFeedSource = new FeedSource(name, project.id, FeedRetrievalMethod.MANUALLY_UPLOADED);
        Persistence.feedSources.create(mockFeedSource);
        return mockFeedSource;
    }
}
