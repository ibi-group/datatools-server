package com.conveyal.datatools.manager.controllers.api;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.Instance;
import com.conveyal.datatools.common.utils.aws.CheckedAWSException;
import com.conveyal.datatools.common.utils.aws.EC2Utils;
import com.conveyal.datatools.common.utils.aws.EC2ValidationResult;
import com.conveyal.datatools.common.utils.aws.S3Utils;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.HaltException;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.conveyal.datatools.common.utils.SparkUtils.getPOJOFromRequestBody;
import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.options;
import static spark.Spark.post;
import static spark.Spark.put;

/**
 * Handlers for HTTP API requests that affect deployment Servers.
 * These methods are mapped to API endpoints by Spark.
 */
public class ServerController {
    private static final Logger LOG = LoggerFactory.getLogger(ServerController.class);

    /**
     * Gets the server specified by the request's id parameter and ensure that user has access to the
     * deployment. If the user does not have permission the Spark request is halted with an error.
     */
    private static OtpServer getServerWithPermissions(Request req) {
        Auth0UserProfile userProfile = req.attribute("user");
        String serverId = req.params("id");
        OtpServer server = Persistence.servers.getById(serverId);
        if (server == null) {
            logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Server does not exist.");
        } else {
            boolean isProjectAdmin = userProfile.canAdministerProject(server);
            if (!isProjectAdmin && !userProfile.getUser_id().equals(server.user())) {
                // If user is not a project admin and did not create the deployment, access to the deployment is denied.
                logMessageAndHalt(req, HttpStatus.UNAUTHORIZED_401, "User not authorized for deployment.");
            }
        }
        return server;
    }

    /** HTTP endpoint for deleting an {@link OtpServer}. */
    private static OtpServer deleteServer(Request req, Response res) throws CheckedAWSException {
        OtpServer server = getServerWithPermissions(req);
        // Ensure that there are no active EC2 instances associated with server. Halt deletion if so.
        List<Instance> activeInstances = server.retrieveEC2Instances().stream()
            .filter(instance -> "running".equals(instance.getState().getName()))
            .collect(Collectors.toList());
        if (activeInstances.size() > 0) {
            logMessageAndHalt(
                req,
                HttpStatus.BAD_REQUEST_400,
                "Cannot delete server with active EC2 instances: " + EC2Utils.getIds(activeInstances)
            );
        }
        server.delete();
        return server;
    }

    /** HTTP method for terminating EC2 instances associated with an ELB OTP server. */
    private static OtpServer terminateEC2InstancesForServer(Request req, Response res) throws CheckedAWSException {
        OtpServer server = getServerWithPermissions(req);
        List<Instance> instances = server.retrieveEC2Instances();
        List<String> ids = EC2Utils.getIds(instances);
        try {
            AmazonEC2 ec2Client = server.getEC2Client();
            EC2Utils.terminateInstances(ec2Client, ids);
        } catch (AmazonServiceException | CheckedAWSException e) {
            logMessageAndHalt(req, 500, "Failed to terminate instances!", e);
        }
        for (Deployment deployment : Deployment.retrieveDeploymentForServerAndRouterId(server.id, null)) {
            Persistence.deployments.updateField(deployment.id, "deployedTo", null);
        }
        return server;
    }

    /**
     * Create a new server for the project. All feed sources with a valid latest version are added to the new
     * deployment.
     */
    private static OtpServer createServer(Request req, Response res) throws IOException {
        Auth0UserProfile userProfile = req.attribute("user");
        OtpServer newServer = getPOJOFromRequestBody(req, OtpServer.class);
        // If server has no project ID specified, user must be an application admin to create it. Otherwise, they must
        // be a project admin.
        boolean allowedToCreate = newServer.projectId == null
            ? userProfile.canAdministerApplication()
            : userProfile.canAdministerProject(newServer);
        if (allowedToCreate) {
            validateFields(req, newServer);
            Persistence.servers.create(newServer);
            return newServer;
        } else {
            logMessageAndHalt(req, 403, "Not authorized to create a server for project " + newServer.projectId);
            return null;
        }
    }

    /**
     * HTTP controller to fetch all servers or servers assigned to a particular project. This should only be used for the
     * management of these servers. For checking servers that a project can deploy to, use {@link Project#availableOtpServers()}.
     */
    private static List<OtpServer> fetchServers (Request req, Response res) {
        String projectId = req.queryParams("projectId");
        Auth0UserProfile userProfile = req.attribute("user");
        if (projectId != null) {
            Project project = Persistence.projects.getById(projectId);
            if (project == null) logMessageAndHalt(req, 400, "Must provide a valid project ID.");
            else if (userProfile.canAdministerProject(project)) return project.availableOtpServers();
        }
        else if (userProfile.canAdministerApplication()) return Persistence.servers.getAll();
        return Collections.emptyList();
    }

    /**
     * HTTP controller to fetch a single server.
     */
    private static OtpServer fetchServer (Request req, Response res) {
        String serverId = req.params("id");
        Auth0UserProfile userProfile = req.attribute("user");
        OtpServer server = Persistence.servers.getById(serverId);
        if (
            server != null &&
            server.projectId != null &&
                !userProfile.canAdministerApplication() &&
                !userProfile.canAdministerProject(server)
        ) {
            logMessageAndHalt(req, 403, "Not authorized to view this server");
            return null;
        }
        return server;
    }

    /**
     * Update a single OTP server.
     */
    private static OtpServer updateServer(Request req, Response res) throws IOException {
        OtpServer serverToUpdate = getServerWithPermissions(req);
        OtpServer updatedServer = getPOJOFromRequestBody(req, OtpServer.class);
        Auth0UserProfile user = req.attribute("user");
        if ((serverToUpdate.admin || serverToUpdate.projectId == null) && !user.canAdministerApplication()) {
            logMessageAndHalt(req, HttpStatus.UNAUTHORIZED_401, "User cannot modify admin-only or application-wide server.");
        }
        validateFields(req, updatedServer);
        Persistence.servers.replace(serverToUpdate.id, updatedServer);
        return Persistence.servers.getById(updatedServer.id);
    }

    /**
     * Validate certain fields found in the document representing a server. This also currently modifies the document by
     * removing problematic date fields.
     */
    private static void validateFields(Request req, OtpServer server) throws HaltException {
        try {
            // Check that projectId is valid.
            if (server.projectId != null) {
                Project project = Persistence.projects.getById(server.projectId);
                if (project == null)
                    logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Must specify valid project ID.");
            }
            // If a server's ec2 info object is not null, it must pass a few validation checks on various fields related to
            // AWS. (e.g., target group ARN and instance type).
            if (server.ec2Info != null) {
                try {
                    EC2ValidationResult result = server.validateEC2Config();
                    if (!result.isValid()) {
                        logMessageAndHalt(req, 400, result.getMessage(), result.getException());
                    }
                } catch (Exception e) {
                    logMessageAndHalt(req, 500, "Failed to validate EC2 config", e);
                }
                if (server.ec2Info.instanceCount < 0) server.ec2Info.instanceCount = 0;
            }
            // Server must have name.
            if (StringUtils.isEmpty(server.name))
                logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Server must have valid name.");
            // Server must have an internal URL (for build graph over wire) or an s3 bucket (for auto deploy ec2).
            if (StringUtils.isEmpty(server.s3Bucket)) {
                if (server.internalUrl == null || server.internalUrl.size() == 0) {
                    logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Server must contain either internal URL(s) or s3 bucket name.");
                }
            } else {
                try {
                    S3Utils.verifyS3WritePermissions(S3Utils.getS3Client(server), server.s3Bucket);
                } catch (Exception e) {
                    logMessageAndHalt(
                        req,
                        400,
                        "Application cannot write to specified S3 bucket: " + server.s3Bucket,
                        e
                    );
                }
            }
        } catch (Exception e) {
            if (e instanceof HaltException) throw e;
            else logMessageAndHalt(req, 400, "Error encountered while validating server field", e);
        }
    }

    /**
     * Register HTTP methods with handler methods.
     */
    public static void register (String apiPrefix) {
        // Construct JSON managers which help serialize the response. Slim JSON is used for the fetchServers method
        // while the Full JSON also contains the ec2Instances field. This is to make sure no unnecessary requests to AWS
        // are made.
        JsonManager<OtpServer> slimJson = new JsonManager<>(OtpServer.class, JsonViews.UserInterface.class);
        JsonManager<OtpServer> fullJson = new JsonManager<>(OtpServer.class, JsonViews.UserInterface.class);
        slimJson.addMixin(OtpServer.class, OtpServer.OtpServerWithoutEc2Instances.class);

        options(apiPrefix + "secure/servers", (q, s) -> "");
        delete(apiPrefix + "secure/servers/:id", ServerController::deleteServer, fullJson::write);
        delete(apiPrefix + "secure/servers/:id/ec2", ServerController::terminateEC2InstancesForServer, fullJson::write);
        get(apiPrefix + "secure/servers", ServerController::fetchServers, slimJson::write);
        get(apiPrefix + "secure/servers/:id", ServerController::fetchServer, fullJson::write);
        post(apiPrefix + "secure/servers", ServerController::createServer, fullJson::write);
        put(apiPrefix + "secure/servers/:id", ServerController::updateServer, fullJson::write);
    }

}
