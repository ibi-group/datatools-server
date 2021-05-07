package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.TestUtils;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.FeedRetrievalMethod;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.JobUtils;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.Set;

import static com.conveyal.datatools.TestUtils.getFeedVersionFromGTFSFile;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class AutoDeployJobTest extends DatatoolsTest {
    private static final Logger LOG = LoggerFactory.getLogger(AutoDeployJobTest.class);
    private static Auth0UserProfile user;
    private static OtpServer server;

    private static Deployment deploymentA;
    private static Project projectA;
    private static FeedSource mockFeedSourceA;
    private static FeedVersion mockFeedVersionA;

    private static Deployment deploymentB;
    private static Project projectB;
    private static FeedSource mockFeedSourceB;

    private static Deployment deploymentC;
    private static Project projectC;
    private static FeedSource mockFeedSourceC;
    private static FeedVersion feedVersionC;
    private static FeedSource mockFeedSourceD;
    private static Project projectD;

    @BeforeAll
    public static void setUp() throws IOException {
        DatatoolsTest.setUp();
        Auth0Connection.setAuthDisabled(true);
        LOG.info("{} setup", AutoDeployJobTest.class.getSimpleName());
        user = Auth0UserProfile.createTestAdminUser();

        // used by multiple tests.
        server = createOtpServer();
        DeployJob.DeploySummary deploySummary = createDeploymentSummary(server.id);

        projectA = createProject();
        deploymentA = createDeployment(deploySummary, projectA.id);
        mockFeedSourceA = createFeedSource("Mock feed source A", projectA.id);
        mockFeedVersionA = TestUtils.createFeedVersion(mockFeedSourceA,
            TestUtils.zipFolderFiles("fake-agency-expire-in-2099"));

        // create a "newer" feed version so that there is a newer feed version to deploy
        TestUtils.createFeedVersion(mockFeedSourceA,
            TestUtils.zipFolderFiles("fake-agency-expire-in-2099"));

        projectB = createProject();
        deploymentB = createDeployment(deploySummary, projectB.id);
        mockFeedSourceB = createFeedSource("Mock feed source B", projectB.id);
        TestUtils.createFeedVersion(mockFeedSourceB,
            TestUtils.zipFolderFiles("fake-agency-with-only-calendar-expire-in-2099-with-failed-referential-integrity"));

        projectC = createProject();
        deploymentC = createDeployment(deploySummary, projectC.id);
        mockFeedSourceC = createFeedSource("Mock feed source C", projectC.id);
        feedVersionC = getFeedVersionFromGTFSFile(mockFeedSourceC,
            TestUtils.zipFolderFiles("fake-agency-expire-in-2099"));
        Persistence.feedVersions.create(feedVersionC);

        projectD = createProject();
        mockFeedSourceD = createFeedSource("Mock feed source D", projectD.id);
        TestUtils.createFeedVersion(mockFeedSourceD,
            TestUtils.zipFolderFiles("fake-agency-expire-in-2099"));
    }

    @AfterAll
    public static void tearDown() {
        Auth0Connection.setAuthDisabled(Auth0Connection.getDefaultAuthDisabled());
        server.delete();

        projectA.delete();
        mockFeedSourceA.delete();
        deploymentA.delete();

        projectB.delete();
        mockFeedSourceB.delete();
        deploymentB.delete();

        projectC.delete();
        mockFeedSourceC.delete();
        deploymentC.delete();

        projectD.delete();
        mockFeedSourceD.delete();
        Persistence.feedVersions.removeById(feedVersionC.id);
    }

    @Test
    public void canAutoDeployFeedVersionForProject() {
        projectA.pinnedDeploymentId = deploymentA.id;
        Persistence.projects.replace(projectA.id, projectA);
        deploymentA.feedVersionIds.add(mockFeedVersionA.id);
        Persistence.deployments.replace(deploymentA.id, deploymentA);
        AutoDeployJob autoDeployFeedJob = new AutoDeployJob(projectA, user);
        autoDeployFeedJob.run();
        // This will not be null because all pre-deployment tests will be passed and project successfully deployed.
        assertNotNull(projectA.lastAutoDeploy);
    }

    @Test
    public void failAutoDeployFeedVersionWithHighSeverityErrorTypes() {
        projectB.pinnedDeploymentId = deploymentB.id;
        Persistence.projects.replace(projectB.id, projectB);
        for (FeedVersion feedVersion : mockFeedSourceB.retrieveFeedVersions()) {
            deploymentB.feedVersionIds.add(feedVersion.id);
        }
        Persistence.deployments.replace(deploymentB.id, deploymentB);
        AutoDeployJob autoDeployFeedJob = new AutoDeployJob(projectB, user);
        autoDeployFeedJob.run();
        // This will be null because the feed version contains a referential integrity error which is detected by
        // FeedVersion#hasHighSeverityErrorTypes.
        assertNull(projectB.lastAutoDeploy);
    }

    @Test
    public void failAutoDeployIfFetchStillInProgress() {

        // Create fake processing job for mock feed (don't actually start it, to keep it in the userJobsMap
        // indefinitely).
        Set<MonitorableJob> userJobs = Sets.newConcurrentHashSet();
        userJobs.add(new ProcessSingleFeedJob(feedVersionC, user, true));
        JobUtils.userJobsMap.put(user.getUser_id(), userJobs);

        // Add mock feed 1 to the deployment so that it is detected in the Deployment#hasFeedFetchesInProgress check
        // (called during auto deploy).
        deploymentC.feedVersionIds = Collections.singletonList(feedVersionC.id);
        Persistence.deployments.replace(deploymentC.id, deploymentC);

        projectD.pinnedDeploymentId = deploymentC.id;
        Persistence.projects.replace(projectD.id, projectD);
        AutoDeployJob autoDeployFeedJob = new AutoDeployJob(projectD, user);
        autoDeployFeedJob.run();
        // This will be null because the fake processing job will be detected by Deployment#hasFeedFetchesInProgress
        // preventing the pre-deployment checks from passing and the project from being deployed.
        assertNull(projectD.lastAutoDeploy);
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
        project.autoDeployTypes.add(AutoDeployType.ON_PROCESS_FEED);
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

    private static FeedSource createFeedSource(String name, String projectId) {
        FeedSource mockFeedSource = new FeedSource(name, projectId, FeedRetrievalMethod.MANUALLY_UPLOADED);
        // Set mock feed source deployable to false so auto deploy is not triggered from ProcessSingleFeedJob when the
        // feed version is created.
        mockFeedSource.deployable = false;
        Persistence.feedSources.create(mockFeedSource);
        return mockFeedSource;
    }
}
