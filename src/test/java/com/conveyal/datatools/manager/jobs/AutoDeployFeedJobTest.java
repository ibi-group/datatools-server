package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.models.*;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;

import static com.conveyal.datatools.TestUtils.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class AutoDeployFeedJobTest extends DatatoolsTest {
    private static final Logger LOG = LoggerFactory.getLogger(AutoDeployFeedJobTest.class);
    private static OtpServer server;
    private static Deployment deployment;
    private static Project project;
    private static FeedSource mockFeedSource;

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
        deployment.deployJobSummaries.add(deploySummary);
        deployment.projectId = project.id;
        Persistence.deployments.create(deployment);

        mockFeedSource = new FeedSource("Mock Feed Source", project.id, FeedRetrievalMethod.MANUALLY_UPLOADED);
        Persistence.feedSources.create(mockFeedSource);
    }

    @AfterClass
    public static void tearDown() {
        Auth0Connection.setAuthDisabled(Auth0Connection.getDefaultAuthDisabled());
        server.delete();
        deployment.delete();
        project.delete();
    }

    @Test
    public void failIfFeedSourceNotDeployable() throws IOException {
        setFeedSourceDeployable(false);
        setProjectAutoDeploy(true);
        setProjectPinnedDeploymentId(deployment.id);
        ProcessSingleFeedJob processSingleFeedJob = triggerProcessSingleFeedJob("fake-agency-expire-in-2099");
        AutoDeployFeedJob autoDeployJob = getAutoDeployJobFromParent(processSingleFeedJob);
        assertThat(autoDeployJob.status.message, equalTo("Required conditions for auto deploy not met. Skipping auto-deploy"));
    }

    @Test
    public void failIfProjectNotPinned() throws IOException {
        setFeedSourceDeployable(true);
        setProjectAutoDeploy(true);
        ProcessSingleFeedJob processSingleFeedJob = triggerProcessSingleFeedJob("fake-agency-expire-in-2099");
        AutoDeployFeedJob autoDeployJob = getAutoDeployJobFromParent(processSingleFeedJob);
        assertThat(autoDeployJob.status.message, equalTo("Required conditions for auto deploy not met. Skipping auto-deploy"));
    }

    @Test
    public void failIfProjectNotAutoDeploy() throws IOException {
        setFeedSourceDeployable(true);
        setProjectAutoDeploy(false);
        setProjectPinnedDeploymentId(deployment.id);
        ProcessSingleFeedJob processSingleFeedJob = triggerProcessSingleFeedJob("fake-agency-expire-in-2099");
        AutoDeployFeedJob autoDeployJob = getAutoDeployJobFromParent(processSingleFeedJob);
        assertThat(autoDeployJob.status.message, equalTo("Required conditions for auto deploy not met. Skipping auto-deploy"));
    }

    @Test
    public void autoDeployFeedVersion() throws IOException {
        setFeedSourceDeployable(true);
        setProjectAutoDeploy(true);
        setProjectPinnedDeploymentId(deployment.id);
        ProcessSingleFeedJob processSingleFeedJob = triggerProcessSingleFeedJob("fake-agency-expire-in-2099");
        AutoDeployFeedJob autoDeployJob = getAutoDeployJobFromParent(processSingleFeedJob);
        assertThat(autoDeployJob.status.message, equalTo("Job complete!"));
    }

    @Test
    public void failIfFeedVersionHasHighSeverityErrorTypes() throws IOException {
        setFeedSourceDeployable(true);
        setProjectAutoDeploy(true);
        setProjectPinnedDeploymentId(deployment.id);
        ProcessSingleFeedJob processSingleFeedJob = triggerProcessSingleFeedJob("fake-agency-expire-in-2099-with-unused-route");
        AutoDeployFeedJob autoDeployJob = getAutoDeployJobFromParent(processSingleFeedJob);
        assertThat(autoDeployJob.status.message, equalTo("Required conditions for auto deploy not met. Skipping auto-deploy"));
    }

    private void setFeedSourceDeployable(boolean deployable) {
        mockFeedSource.deployable = deployable;
        Persistence.feedSources.replace(mockFeedSource.id, mockFeedSource);
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
    private ProcessSingleFeedJob triggerProcessSingleFeedJob(String zipFolderName) throws IOException {
        File zipFile = zipFolderFiles(zipFolderName);
        ProcessSingleFeedJob processSingleFeedJob = createProcessSingleFeedJob(mockFeedSource, zipFile);
        processSingleFeedJob.run();
        return processSingleFeedJob;
    }

    private AutoDeployFeedJob getAutoDeployJobFromParent(ProcessSingleFeedJob processSingleFeedJob) {
        List<MonitorableJob> subJobs = processSingleFeedJob.getSubJobs();
        MonitorableJob lastJob = subJobs.get(subJobs.size() - 1);
        if (lastJob instanceof AutoDeployFeedJob) {
            return (AutoDeployFeedJob) lastJob;
        }
        throw new IllegalArgumentException("No auto deploy job found as last job of ProcessSingleFeedJob!");
    }

}
