package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.TestUtils;
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

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.Set;

import static com.conveyal.datatools.TestUtils.getFeedVersionFromGTFSFile;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class AutoDeployFeedJobTest extends DatatoolsTest {
    private static final Logger LOG = LoggerFactory.getLogger(AutoDeployFeedJobTest.class);
    private static Auth0UserProfile user;
    private static OtpServer server;
    private static Deployment deployment;
    private static Project project;
    private static FeedSource mockFeedSource;
    private static FeedVersion feedVersion;
    private static FeedVersion feedVersionExpireIn2099;
    private static FeedVersion feedVersionExpireIn2099UnusedRoute;

    // These parameters are used solely by the failAutoDeployIfFetchStillInProgress which fails CI (on GitHub) if unique
    // parameters are not used.
    private static OtpServer fetchStillInProgress_server;
    private static Deployment fetchStillInProgress_deployment;
    private static Project fetchStillInProgress_project;
    private static FeedSource fetchStillInProgress_mockFeedSourceFakeJob;
    private static FeedSource fetchStillInProgress_mockFeedSource;
    private static FeedVersion fetchStillInProgress_feedVersion;

    @BeforeClass
    public static void setUp() throws IOException {
        DatatoolsTest.setUp();
        Auth0Connection.setAuthDisabled(true);
        LOG.info("{} setup", AutoDeployFeedJobTest.class.getSimpleName());
        user = Auth0UserProfile.createTestAdminUser();

        server = createOtpServer();
        DeployJob.DeploySummary deploySummary = createDeploymentSummary(server.id);
        project = createProject();
        deployment = createDeployment(deploySummary, project.id);
        mockFeedSource = createFeedSource("Mock Feed Source", project.id);

        feedVersion = createFeedVersion(mockFeedSource);
        feedVersionExpireIn2099 = TestUtils.createFeedVersion(mockFeedSource,
            TestUtils.zipFolderFiles("fake-agency-expire-in-2099"));
        feedVersionExpireIn2099UnusedRoute = TestUtils.createFeedVersion(mockFeedSource,
            TestUtils.zipFolderFiles("fake-agency-with-only-calendar-expire-in-2099-with-unused-route"));

        fetchStillInProgress_server = createOtpServer();
        DeployJob.DeploySummary deploySummaryFetchInProgress = createDeploymentSummary(fetchStillInProgress_server.id);
        fetchStillInProgress_project = createProject();
        fetchStillInProgress_deployment = createDeployment(deploySummaryFetchInProgress, fetchStillInProgress_project.id);
        fetchStillInProgress_mockFeedSourceFakeJob =
            createFeedSource("Mock Feed Source For Fetch In Progress Fake Job", fetchStillInProgress_project.id);
        fetchStillInProgress_mockFeedSource =
            createFeedSource("Mock Feed Source For Fetch In Progress", fetchStillInProgress_project.id);
        fetchStillInProgress_feedVersion = getFeedVersionFromGTFSFile(fetchStillInProgress_mockFeedSourceFakeJob,
            TestUtils.zipFolderFiles("fake-agency-expire-in-2099"));
        Persistence.feedVersions.create(fetchStillInProgress_feedVersion);
    }

    @AfterClass
    public static void tearDown() {
        Auth0Connection.setAuthDisabled(Auth0Connection.getDefaultAuthDisabled());
        server.delete();
        deployment.delete();
        mockFeedSource.delete();
        feedVersionExpireIn2099.delete();
        feedVersionExpireIn2099UnusedRoute.delete();

        fetchStillInProgress_mockFeedSource.delete();
        fetchStillInProgress_server.delete();
        fetchStillInProgress_deployment.delete();
        fetchStillInProgress_mockFeedSourceFakeJob.delete();
        fetchStillInProgress_feedVersion.delete();

        Persistence.projects.removeById(fetchStillInProgress_project.id);
        Persistence.projects.removeById(project.id);
        Persistence.feedVersions.removeById(feedVersion.id);
        Persistence.feedVersions.removeById(fetchStillInProgress_feedVersion.id);
    }

    // TODO: AutoDeployFeedJob now processes multiple projects. It is not possible to set the job status in
    //  relation to a single project success or failure. This means asserting on a change of state to the class project
    //  e.g. lastAutoDeploy (as below). But this doesn't seem right and doesn't have the required granularity.
    //  A few options come to mind:
    //  1) New String autoDeployStatus param in the Project class which is updated by AutoDeployFeedJob. May have downstream benefits?
    //  2) Append (potentially many) messages to the job status and use assertThat(x, containString("y")). Messy.
    //  3) New MonitorableJob.Status or similar class in the Project class.
    @Test
    public void canAutoDeployFeedVersionForProject() {
        setProjectPinnedDeploymentId(project, deployment.id);
        deployment.feedVersionIds = Collections.singletonList(feedVersionExpireIn2099.id);
        Persistence.deployments.replace(deployment.id, deployment);
        AutoDeployFeedJob autoDeployFeedJob = new AutoDeployFeedJob(project, user);
        autoDeployFeedJob.run();
        assertNotNull(project.lastAutoDeploy);
    }

    @Test
    public void failAutoDeployFeedVersionWithHighSeverityErrorTypes() {
        setProjectLastAutoDeployToNull(project);
        setProjectPinnedDeploymentId(project, deployment.id);
        deployment.feedVersionIds = Collections.singletonList(feedVersionExpireIn2099UnusedRoute.id);
        Persistence.deployments.replace(deployment.id, deployment);
        AutoDeployFeedJob autoDeployFeedJob = new AutoDeployFeedJob(project, user);
        autoDeployFeedJob.run();
        assertNull(project.lastAutoDeploy);

    }

    @Test
    public void failAutoDeployIfFetchStillInProgress() {
        setProjectPinnedDeploymentId(fetchStillInProgress_project, fetchStillInProgress_deployment.id);

        // Create fake processing job for mock feed (don't actually start it, to keep it in the userJobsMap
        // indefinitely).
        Set<MonitorableJob> userJobs = Sets.newConcurrentHashSet();
        userJobs.add(new ProcessSingleFeedJob(fetchStillInProgress_feedVersion, user, true));
        DataManager.userJobsMap.put(user.getUser_id(), userJobs);

        // Add mock feed 1 to the deployment so that it is detected in the Deployment#hasFeedFetchesInProgress check
        // (called during auto deploy).
        fetchStillInProgress_deployment.feedVersionIds = Collections.singletonList(feedVersion.id);
        Persistence.deployments.replace(fetchStillInProgress_deployment.id, fetchStillInProgress_deployment);

        AutoDeployFeedJob autoDeployFeedJob = new AutoDeployFeedJob(fetchStillInProgress_project, user);
        autoDeployFeedJob.run();
        assertNull(fetchStillInProgress_project.lastAutoDeploy);
    }

    private void setProjectLastAutoDeployToNull(Project project) {
        project.lastAutoDeploy = null;
        Persistence.projects.replace(project.id, project);
    }

    private void setProjectPinnedDeploymentId(Project project, String deploymentId) {
        project.pinnedDeploymentId = deploymentId;
        Persistence.projects.replace(project.id, project);
    }

    private static FeedSource createFeedSource(String name, String projectId) {
        FeedSource mockFeedSource = new FeedSource(name, projectId, FeedRetrievalMethod.MANUALLY_UPLOADED);
        mockFeedSource.deployable = true;
        Persistence.feedSources.create(mockFeedSource);
        return mockFeedSource;
    }

    private static FeedVersion createFeedVersion(FeedSource feedSource) {
        FeedVersion feedVersion = new FeedVersion(feedSource);
        Persistence.feedVersions.create(feedVersion);
        return feedVersion;
    }

    private static OtpServer createOtpServer() {
        OtpServer server = new OtpServer();
        server.name = String.format("Test Server %s", new Date().toString());
        Persistence.servers.create(server);
        return server;
    }

    private static DeployJob.DeploySummary createDeploymentSummary(String serverId) {
        DeployJob.DeploySummary deploySummary = new DeployJob.DeploySummary();
        deploySummary.serverId = serverId;
        return deploySummary;
    }

    private static Project createProject() {
        Project project = new Project();
        project.name = String.format("Test Project %s", new Date().toString());
        project.autoDeploy = true;
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
