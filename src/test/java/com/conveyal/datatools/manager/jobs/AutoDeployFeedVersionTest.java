package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.manager.models.*;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

import static com.conveyal.datatools.TestUtils.*;
import static org.junit.Assert.*;

public class AutoDeployFeedVersionTest extends DatatoolsTest {
    private static final Logger LOG = LoggerFactory.getLogger(AutoDeployFeedVersionTest.class);
    private static OtpServer server;
    private static Deployment deployment;
    private static Project project;
    private static FeedSource mockFeedSource;
    private static FeedVersion mockFeedVersion;

    @BeforeClass
    public static void setUp() throws IOException {
        DatatoolsTest.setUp();
        LOG.info("{} setup", AutoDeployFeedVersionTest.class.getSimpleName());
        String testName = String.format("Test %s", new Date().toString());

        // OTP server instance required by deployment.
        server = new OtpServer();
        server.name = testName;
        Persistence.servers.create(server);

        // deployment.latest() is required to return the latest server id
        DeployJob.DeploySummary deploySummary = new DeployJob.DeploySummary();
        deploySummary.serverId  = server.id;

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
    public static void tearDown() throws InterruptedException {
        // FIXME: Sleep to allow sub-jobs (in child threads) to complete.
        Thread.sleep(1000);
        server.delete();
        deployment.delete();
        project.delete();
    }

    @Test
    public void failIfFeedSourceNotDeployable() throws IOException, InterruptedException {
        setFeedSourceDeployable(false);
        setProjectAutoDeploy(true);
        setProjectPinnedDeploymentId(deployment.id);
        mockFeedVersion = triggerCreateFeedVersion();
        Thread.sleep(1000);
        assertFalse(mockFeedVersion.autoDeployed);
    }

    @Test
    public void failIfProjectNotPinned() throws IOException, InterruptedException {
        setFeedSourceDeployable(true);
        setProjectAutoDeploy(true);
        mockFeedVersion = triggerCreateFeedVersion();
        Thread.sleep(1000);
        assertFalse(mockFeedVersion.autoDeployed);
    }

    @Test
    public void failIfProjectNotAutoDeploy() throws IOException, InterruptedException {
        setFeedSourceDeployable(true);
        setProjectAutoDeploy(false);
        setProjectPinnedDeploymentId(deployment.id);
        mockFeedVersion = triggerCreateFeedVersion();
        Thread.sleep(1000);
        assertFalse(mockFeedVersion.autoDeployed);
    }

    @Test
    public void autoDeployFeedVersion() throws IOException, InterruptedException {
        setFeedSourceDeployable(true);
        setProjectAutoDeploy(true);
        setProjectPinnedDeploymentId(deployment.id);
        mockFeedVersion = triggerCreateFeedVersion();
        Thread.sleep(1000);
        assertTrue(mockFeedVersion.autoDeployed);
    }

//    @Test
//    public void failIfFeedVersionHasHighSeverityErrorTypes() throws IOException {
//        //'COLUMN_NAME_UNSAFE', 'NO_SERVICE', 'REFERENTIAL_INTEGRITY', 'ROUTE_UNUSED', 'STOP_GEOGRAPHIC_OUTLIER', 'STOP_LOW_POPULATION_DENSITY', 'TABLE_IN_SUBDIRECTORY', 'TABLE_MISSING_COLUMN_HEADERS', 'TRAVEL_TIME_NEGATIVE', 'TRAVEL_TIME_ZERO', 'TRIP_EMPTY', 'VALIDATOR_FAILED'
//    }

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
     * Create a feed version and start processing a single feed job.
     */
    private FeedVersion triggerCreateFeedVersion() throws IOException {
        return createFeedVersion(
            mockFeedSource,
            zipFolderFiles("fake-agency-with-calendar-and-calendar-dates")
        );
    }
}
