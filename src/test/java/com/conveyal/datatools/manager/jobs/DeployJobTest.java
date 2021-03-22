package com.conveyal.datatools.manager.jobs;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.model.Instance;
import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.common.utils.aws.CheckedAWSException;
import com.conveyal.datatools.common.utils.aws.EC2Utils;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.EC2Info;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.conveyal.datatools.TestUtils.getBooleanEnvVar;
import static com.zenika.snapshotmatcher.SnapshotMatcher.matchesSnapshot;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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
    @BeforeAll
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
     * Tests that the otp-runner manifest and user data for a graph build + run server instance can be generated
     * properly
     */
    @Test
    public void canMakeGraphBuildAndServeManifestAndUserData () {
        DeployJob deployJob = new DeployJob(
            "Test deploy OTP 1 with build and server",
            deployment,
            Auth0UserProfile.createTestAdminUser(),
            server,
            "test-deploy",
            DeployJob.DeployType.REPLACE,
            true
        );
        OtpRunnerManifest buildAndServeManifest = deployJob.createAndUploadManifestAndConfigs(false);
        buildAndServeManifest.nonce = "canMakeGraphBuildUserDataScript";
        assertThat(buildAndServeManifest, matchesSnapshot());
        assertThat(deployJob.constructUserData(false), matchesSnapshot());
    }

    /**
     * Tests that the otp-runner manifest and user data for a graph build + run server instance can be generated
     * properly
     */
    @Test
    public void canMakeOtp2GraphBuildAndServeManifestAndUserData () {
        Deployment otp2Deployment = new Deployment();
        // the feedVersionIds variable does not get set during tests resulting in a null pointer exception. Set the
        // feedVersionIds to an empty list to avoid this. It is enough to run the tests of the user data generation with
        // an empty list of feed versions, so it is fine that it is empty.
        otp2Deployment.feedVersionIds = new ArrayList<>();
        otp2Deployment.projectId = project.id;
        otp2Deployment.name = "Test OTP 2 Deployment";
        otp2Deployment.otpVersion = "otp-latest-trimet-dev";
        // add in custom build and router config with problematic characters to make sure they are properly escaped
        otp2Deployment.customRouterConfig = "{ \"hi\": \"th\ne'r\te\" }";
        otp2Deployment.customBuildConfig = "{ \"hello\": \"th\ne'r\te\" }";
        otp2Deployment.tripPlannerVersion = Deployment.TripPlannerVersion.OTP_2;
        Persistence.deployments.create(otp2Deployment);
        DeployJob deployJob = new DeployJob(
            "Test deploy OTP 2 with build and server",
            otp2Deployment,
            Auth0UserProfile.createTestAdminUser(),
            server,
            "test-deploy",
            DeployJob.DeployType.REPLACE,
            true
        );
        OtpRunnerManifest buildAndServeManifest = deployJob.createAndUploadManifestAndConfigs(false);
        buildAndServeManifest.nonce = "canMakeOtp2GraphBuildUserDataScript";
        assertThat(buildAndServeManifest, matchesSnapshot());
        assertThat(deployJob.constructUserData(false), matchesSnapshot());
    }

    /**
     * Tests that the otp-runner manifest and user data for a run server only instance can be generated properly
     */
    @Test
    public void canMakeServerOnlyManifestAndUserData () {
        DeployJob deployJob = new DeployJob(
            "Test deploy OTP 1 with server only",
            deployment,
            Auth0UserProfile.createTestAdminUser(),
            server,
            "test-deploy",
            DeployJob.DeployType.REPLACE,
            true
        );
        OtpRunnerManifest serverOnlyManifest = deployJob.createAndUploadManifestAndConfigs(true);
        serverOnlyManifest.nonce = "canMakeServerOnlyUserDataScript";
        assertThat(serverOnlyManifest, matchesSnapshot());
        assertThat(deployJob.constructUserData(true), matchesSnapshot());
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
        assertFalse(deployJob.status.error, "Deployment did not fail.");
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
        assertFalse(deployJob.status.error, "Deployment did not fail.");
    }

    /**
     * Terminates any instances that were created during the tests.
     *
     * Note: this requires changing server.yml#modules.deployment.ec2.enabled to true and also that the
     * RUN_AWS_DEPLOY_JOB_TESTS environment variable is set to "true"
     */
    @AfterAll
    public static void cleanUp() throws AmazonServiceException, CheckedAWSException {
        assumeTrue(getBooleanEnvVar("RUN_AWS_DEPLOY_JOB_TESTS"));
        List<Instance> instances = server.retrieveEC2Instances();
        List<String> ids = EC2Utils.getIds(instances);
        EC2Utils.terminateInstances(
            EC2Utils.getEC2Client(server.role, server.ec2Info == null ? null : server.ec2Info.region),
            ids
        );
    }

}
