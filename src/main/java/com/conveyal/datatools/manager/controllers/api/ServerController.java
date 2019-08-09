package com.conveyal.datatools.manager.controllers.api;

import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancing;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancingv2.model.AmazonElasticLoadBalancingException;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeTargetGroupsRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetGroup;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.HaltException;
import spark.Request;
import spark.Response;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;
import static com.conveyal.datatools.manager.jobs.DeployJob.DEFAULT_INSTANCE_TYPE;
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
    private static JsonManager<OtpServer> json = new JsonManager<>(OtpServer.class, JsonViews.UserInterface.class);
    private static final Logger LOG = LoggerFactory.getLogger(ServerController.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Gets the server specified by the request's id parameter and ensure that user has access to the
     * deployment. If the user does not have permission the Spark request is halted with an error.
     */
    private static OtpServer checkServerPermissions(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        String serverId = req.params("id");
        OtpServer server = Persistence.servers.getById(serverId);
        if (server == null) {
            logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Server does not exist.");
        }
        boolean isProjectAdmin = userProfile.canAdministerProject(server.projectId, server.organizationId());
        if (!isProjectAdmin && !userProfile.getUser_id().equals(server.user())) {
            // If user is not a project admin and did not create the deployment, access to the deployment is denied.
            logMessageAndHalt(req, HttpStatus.UNAUTHORIZED_401, "User not authorized for deployment.");
        }
        return server;
    }

    private static OtpServer deleteServer(Request req, Response res) {
        OtpServer server = checkServerPermissions(req, res);
        server.delete();
        return server;
    }

    /**
     * Create a new server for the project. All feed sources with a valid latest version are added to the new
     * deployment.
     */
    private static OtpServer createServer(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        Document newServerFields = Document.parse(req.body());
        String projectId = newServerFields.getString("projectId");
        String organizationId = newServerFields.getString("organizationId");
        // If server has no project ID specified, user must be an application admin to create it. Otherwise, they must
        // be a project admin.
        boolean allowedToCreate = projectId == null
            ? userProfile.canAdministerApplication()
            : userProfile.canAdministerProject(projectId, organizationId);
        if (allowedToCreate) {
            validateFields(req, newServerFields);
            try {
                OtpServer newServer = mapper.readValue(newServerFields.toJson(), OtpServer.class);
                Persistence.servers.create(newServer);
                return newServer;
            } catch (IOException e) {
                logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Error parsing OTP server JSON.");
                return null;
            }
        } else {
            logMessageAndHalt(req, 403, "Not authorized to create a server for project " + projectId);
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
            else if (userProfile.canAdministerProject(projectId, null)) return project.availableOtpServers();
        }
        else if (userProfile.canAdministerApplication()) return Persistence.servers.getAll();
        return Collections.emptyList();
    }

    /**
     * Update a single server. If the server's feed versions are updated, checks to ensure that each
     * version exists and is a part of the same parent project are performed before updating.
     */
    private static OtpServer updateServer(Request req, Response res) {
        OtpServer serverToUpdate = checkServerPermissions(req, res);
        Document updateDocument = Document.parse(req.body());
        Auth0UserProfile user = req.attribute("user");
        if ((serverToUpdate.admin || serverToUpdate.projectId == null) && !user.canAdministerApplication()) {
            logMessageAndHalt(req, HttpStatus.UNAUTHORIZED_401, "User cannot modify admin-only or application-wide server.");
        }
        validateFields(req, updateDocument);
        OtpServer updatedServer = Persistence.servers.update(serverToUpdate.id, updateDocument);
        return updatedServer;
    }

    /**
     * Validate certain fields found in the document representing a server. This also currently modifies the document by
     * removing problematic date fields.
     */
    private static void validateFields(Request req, Document serverDocument) throws HaltException {
        // FIXME: There is an issue with updating a MongoDB record with JSON serialized date fields because these come
        // back as integers. MongoDB writes these values into the database fine, but when it comes to converting them
        // into POJOs, it throws exceptions about expecting date types. For now, we can just remove them here, but this
        // speaks to the fragility of this system currently.
        serverDocument.remove("lastUpdated");
        serverDocument.remove("dateCreated");
        // Check that projectId is valid.
        if (serverDocument.containsKey("projectId") && serverDocument.get("projectId") != null) {
            Project project = Persistence.projects.getById(serverDocument.get("projectId").toString());
            if (project == null) logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Must specify valid project ID.");
        }
        // If server has a target group specified, it must have a few other fields as well (e.g., instance type).
        if (serverDocument.containsKey("targetGroupArn") && serverDocument.get("targetGroupArn") != null) {
            // Validate that the Target Group ARN is valid.
            try {
                DescribeTargetGroupsRequest describeTargetGroupsRequest =
                    new DescribeTargetGroupsRequest().withTargetGroupArns(serverDocument.get("targetGroupArn").toString());
                AmazonElasticLoadBalancing elb = AmazonElasticLoadBalancingClient.builder().build();
                List<TargetGroup> targetGroups = elb.describeTargetGroups(describeTargetGroupsRequest).getTargetGroups();
                if (targetGroups.size() == 0) {
                    logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Invalid value for Target Group ARN. Could not locate Target Group.");
                }
            } catch (AmazonElasticLoadBalancingException e) {
                logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Invalid value for Target Group ARN.");
            }
            // Validate instance type
            if (serverDocument.get("instanceType") != null) {
                try {
                    InstanceType.fromValue(serverDocument.get("instanceType").toString());
                } catch (IllegalArgumentException e) {
                    String message = String.format("Must provide valid instance type (if none provided, defaults to %s).", DEFAULT_INSTANCE_TYPE);
                    logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, message);
                }
            }
        }
        // Server must have name.
        if (serverDocument.get("name") == null || "".equals(serverDocument.get("name").toString())) {
            logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Server must have valid name.");
        }
        // Server must have an internal URL (for build graph over wire) or an s3 bucket (for auto deploy ec2).
        if (serverDocument.get("s3Bucket") == null || "".equals(serverDocument.get("s3Bucket").toString())) {
            if (!serverDocument.containsKey("internalUrl") || ((List) serverDocument.get("internalUrl")).size() == 0) {
                logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Server must contain either internal URL(s) or s3 bucket name.");
            }
        } else {
            // Verify that application has permission to write to/delete from S3 bucket.
            String key = UUID.randomUUID().toString();
            String bucket = serverDocument.get("s3Bucket").toString();
            try {
                FeedStore.s3Client.putObject(bucket, key, File.createTempFile("test", ".zip"));
                FeedStore.s3Client.deleteObject(bucket, key);
            } catch (IOException | AmazonS3Exception e) {
                String message = "Cannot write to specified S3 bucket " + bucket;
                LOG.error(message, e);
                logMessageAndHalt(req, 400, message);
            }
        }
    }

    /**
     * Register HTTP methods with handler methods.
     */
    public static void register (String apiPrefix) {
        options(apiPrefix + "secure/servers", (q, s) -> "");
        delete(apiPrefix + "secure/servers/:id", ServerController::deleteServer, json::write);
        get(apiPrefix + "secure/servers", ServerController::fetchServers, json::write);
        post(apiPrefix + "secure/servers", ServerController::createServer, json::write);
        put(apiPrefix + "secure/servers/:id", ServerController::updateServer, json::write);
    }
}
