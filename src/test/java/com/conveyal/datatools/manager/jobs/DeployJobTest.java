package com.conveyal.datatools.manager.jobs;

import com.amazonaws.services.ec2.model.Instance;
import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.common.utils.AWSUtils;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.EC2Info;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.conveyal.datatools.TestUtils.getBooleanEnvVar;
import static com.conveyal.datatools.manager.controllers.api.ServerController.getIds;
import static com.conveyal.datatools.manager.controllers.api.ServerController.terminateInstances;
import static com.zenika.snapshotmatcher.SnapshotMatcher.matchesSnapshot;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assume.assumeTrue;

/**
 * This test suite contains various unit and integration tests to make sure the a DeployJob works properly. The unit
 * tests are always ran as part of the unit test suite and don't require AWS access. However, some tests will use AWS
 * services to run an integrations test of deploying to AWS infrastructure. For the tests that require credentials on
 * the IBI AWS account, these requires changing server.yml#modules.deployment.ec2.enabled to true and also that the
 * RUN_AWS_DEPLOY_JOB_TESTS environment variable is set to "true".
 */
public class DeployJobTest extends UnitTest {
    private static final Logger LOG = LoggerFactory.getLogger(DeployJobTest.class);
    private static Project project;
    private static OtpServer server;
    private static Deployment deployment;

    /**
     * Add project, server, and deployment to prepare for tests.
     */
    @BeforeClass
    public static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();
        // Create a project, feed sources, and feed versions to merge.
        project = new Project();
        project.name = String.format("Test %s", new Date().toString());
        Persistence.projects.create(project);
        server = new OtpServer();
        server.projectId = project.id;
        server.s3Bucket = "datatools-dev";
        server.ec2Info = new EC2Info();
        server.ec2Info.instanceCount = 2;
        server.ec2Info.instanceType = "r5dn.xlarge";
        // FIXME: How should we include these private-ish values??
        server.ec2Info.securityGroupId = "YOUR_VALUE";
        server.ec2Info.subnetId = "YOUR_VALUE";
        server.ec2Info.keyName = "YOUR_VALUE";
        server.ec2Info.targetGroupArn = "YOUR_VALUE";
        server.ec2Info.iamInstanceProfileArn = "YOUR_VALUE";
        Persistence.servers.create(server);
        deployment = new Deployment();
        // the feedVersionIds variable does not get set during tests resulting in a null pointer exception. Set the
        // feedVersionIds to an empty list to avoid this. It is enough to run the tests of the user data generation with
        // an empty list of feed versions, so it is fine that it is empty.
        deployment.feedVersionIds = new ArrayList<>();
        deployment.projectId = project.id;
        deployment.name = "Test deployment";
        deployment.otpVersion = "otp-latest-trimet-dev";
        // add in custom build and router config with problematic characters to make sure they are properly escaped
        deployment.customRouterConfig = "{ \"hi\": \"th\ne'r\te\" }";
        deployment.customBuildConfig = "{ \"hello\": \"th\ne'r\te\" }";
        Persistence.deployments.create(deployment);
    }

    /**
     * Tests that the user data for a graph build + run server instance can be generated properly
     */
    @Test
    public void canMakeGraphBuildUserDataScript () {
        DeployJob deployJob = new DeployJob(
            deployment,
            Auth0UserProfile.createTestAdminUser(),
            server,
            "test-deploy",
            DeployJob.DeployType.REPLACE
        );
        assertThat(
            replaceNonce(
                deployJob.constructUserData(false),
                "canMakeGraphBuildUserDataScript"
            ),
            matchesSnapshot()
        );
    }

    /**
     * Tests that the user data for a run server only instance can be generated properly
     */
    @Test
    public void canMakeServerOnlyUserDataScript () {
        DeployJob deployJob = new DeployJob(
            deployment,
            Auth0UserProfile.createTestAdminUser(),
            server,
            "test-deploy",
            DeployJob.DeployType.REPLACE
        );
        assertThat(
            replaceNonce(
                deployJob.constructUserData(true),
                "canMakeServerOnlyUserDataScript"
            ),
            matchesSnapshot()
        );
    }

    /**
     * Replaces the nonce in the user data with a deterministic value
     */
    private String replaceNonce(String userData, String replacementNonce) {
        return userData.replaceFirst(
            "nonce\":\"[\\w-]*",
            String.format("nonce\":\"%s", replacementNonce)
        );
    }

    /**
     * Tests that Data Tools can run an ELB deployment.
     *
     * Note: this requires changing server.yml#modules.deployment.ec2.enabled to true and also that the
     * RUN_AWS_DEPLOY_JOB_TESTS environment variable is set to "true"
     */
    @Test
    public void canDeployFromPreloadedBundle () {
        assumeTrue(getBooleanEnvVar("RUN_AWS_DEPLOY_JOB_TESTS"));
        DeployJob deployJob = new DeployJob(deployment, Auth0UserProfile.createTestAdminUser(), server, "test-deploy", DeployJob.DeployType.USE_PRELOADED_BUNDLE);
        deployJob.run();
        // FIXME: Deployment will succeed even if one of the clone servers does not start up properly.
        assertFalse("Deployment did not fail.", deployJob.status.error);
    }

    /**
     * Tests that Data Tools can run an ELB deployment from a pre-built graph.
     *
     * Note: this requires changing server.yml#modules.deployment.ec2.enabled to true and also that the
     * RUN_AWS_DEPLOY_JOB_TESTS environment variable is set to "true"
     */
    @Test
    public void canDeployFromPrebuiltGraph () {
        assumeTrue(getBooleanEnvVar("RUN_AWS_DEPLOY_JOB_TESTS"));
        DeployJob deployJob = new DeployJob(deployment, Auth0UserProfile.createTestAdminUser(), server, "deploy-test", DeployJob.DeployType.USE_PREBUILT_GRAPH);
        deployJob.run();
        // FIXME: Deployment will succeed even if one of the clone servers does not start up properly.
        assertFalse("Deployment did not fail.", deployJob.status.error);
    }

    /**
     * Terminates any instances that were created during the tests.
     *
     * Note: this requires changing server.yml#modules.deployment.ec2.enabled to true and also that the
     * RUN_AWS_DEPLOY_JOB_TESTS environment variable is set to "true"
     */
    @AfterClass
    public static void cleanUp() {
        assumeTrue(getBooleanEnvVar("RUN_AWS_DEPLOY_JOB_TESTS"));
        List<Instance> instances = server.retrieveEC2Instances();
        List<String> ids = getIds(instances);
        terminateInstances(
            AWSUtils.getEC2ClientForRole(server.role, server.ec2Info == null ? null : server.ec2Info.region),
            ids
        );
    }

}
