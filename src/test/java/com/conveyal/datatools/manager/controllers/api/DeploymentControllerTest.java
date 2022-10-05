package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.TestUtils;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.jobs.DeployJob;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.DeploymentSummary;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.HttpUtils;
import com.conveyal.datatools.manager.utils.SimpleHttpResponse;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeploymentControllerTest extends DatatoolsTest {
    private static Project project;
    private static OtpServer server;
    private static Deployment deployment;
    private static DeployJob.DeploySummary deploySummary;

    /**
     * Add project, server, deploy summary and deployment to prepare for test.
     */
    @BeforeAll
    public static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();
        Auth0Connection.setAuthDisabled(true);
        project = new Project();
        project.name = String.format("Test %s", new Date());
        Persistence.projects.create(project);

        server = new OtpServer();
        server.name = "Test Server";
        server.projectId = project.id;
        server.s3Bucket = "datatools-dev";
        Persistence.servers.create(server);

        deploySummary = new DeployJob.DeploySummary();
        deploySummary.serverId = server.id;

        deployment = new Deployment();
        deployment.feedVersionIds = Collections.singletonList("feedVersionId");
        deployment.projectId = project.id;
        deployment.name = "Test deployment";
        deployment.deployJobSummaries.add(deploySummary);
        deployment.deployedTo = server.id;
        deployment.routerId = "Test router id";
        Persistence.deployments.create(deployment);

        // Update project with pinned deployment.
        project.pinnedDeploymentId = deployment.id;
        Persistence.projects.replace(project.id, project);
    }

    /**
     * Remove objects from the database after test and reset auth.
     */
    @AfterAll
    public static void tearDown() {
        Auth0Connection.setAuthDisabled(Auth0Connection.getDefaultAuthDisabled());
        if (project != null) {
            Persistence.projects.removeById(project.id);
        }
        if (server != null) {
            Persistence.servers.removeById(server.id);
        }
        if (deployment != null) {
            Persistence.deployments.removeById(deployment.id);
        }
    }

    /**
     * Retrieve a deployment summary for the previously created project, and it's related objects.
     */
    @Test
    void canRetrieveDeploymentSummaries() throws IOException {
        SimpleHttpResponse response = TestUtils.makeRequest(
            String.format("/api/manager/secure/deploymentSummaries?projectId=%s", project.id),
            null,
            HttpUtils.REQUEST_METHOD.GET
        );
        List<DeploymentSummary> deploymentSummaries =
            JsonUtil.getPOJOFromJSONAsList(
                JsonUtil.getJsonNodeFromResponse(response),
                DeploymentSummary.class
            );
        assertEquals(deployment.id, deploymentSummaries.get(0).id);
        assertEquals(deployment.name, deploymentSummaries.get(0).name);
        assertEquals(deployment.dateCreated, deploymentSummaries.get(0).dateCreated);
        assertEquals(new Date(deploySummary.finishTime), deploymentSummaries.get(0).lastDeployed);
        assertEquals(server.id, deploymentSummaries.get(0).deployedTo);
        assertEquals(deployment.feedVersionIds.size(), deploymentSummaries.get(0).numberOfFeedVersions);
        assertTrue(deploymentSummaries.get(0).test);
        assertTrue(deploymentSummaries.get(0).isPinned);
        assertEquals(server.name, deploymentSummaries.get(0).serverName);
    }
}
