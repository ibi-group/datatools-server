package com.conveyal.datatools.manager.controllers.api;

import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancing;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancingv2.model.AmazonElasticLoadBalancingException;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeTargetGroupsRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetGroup;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import org.bson.Document;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.util.List;

import static com.conveyal.datatools.common.utils.SparkUtils.haltWithMessage;
import static spark.Spark.delete;
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

    /**
     * Gets the server specified by the request's id parameter and ensure that user has access to the
     * deployment. If the user does not have permission the Spark request is halted with an error.
     */
    private static OtpServer checkServerPermissions(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        String serverId = req.params("id");
        OtpServer server = Persistence.servers.getById(serverId);
        if (server == null) {
            haltWithMessage(req, HttpStatus.BAD_REQUEST_400, "Server does not exist.");
        }
        boolean isProjectAdmin = userProfile.canAdministerProject(server.projectId, server.organizationId());
        if (!isProjectAdmin && !userProfile.getUser_id().equals(server.user())) {
            // If user is not a project admin and did not create the deployment, access to the deployment is denied.
            haltWithMessage(req, HttpStatus.UNAUTHORIZED_401, "User not authorized for deployment.");
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
        // TODO error handling when request is bogus
        // TODO factor out user profile fetching, permissions checks etc.
        Auth0UserProfile userProfile = req.attribute("user");
        Document newServerFields = Document.parse(req.body());
        String projectId = newServerFields.getString("projectId");
        String organizationId = newServerFields.getString("organizationId");
        if (projectId == null) haltWithMessage(req, 400, "Must provide valid project ID");
        boolean allowedToCreate = userProfile.canAdministerProject(projectId, organizationId);

        if (allowedToCreate) {
            Project project = Persistence.projects.getById(projectId);
            OtpServer newServer = new OtpServer();
            validateFields(req, newServerFields);
            // FIXME: Here we are creating a deployment and updating it with the JSON string (two db operations)
            // We do this because there is not currently apply JSON directly to an object (outside of Mongo codec
            // operations)
            Persistence.servers.create(newServer);
            return Persistence.servers.update(newServer.id, req.body());
        } else {
            haltWithMessage(req, 403, "Not authorized to create a server for project " + projectId);
            return null;
        }
    }

    /**
     * Update a single server. If the server's feed versions are updated, checks to ensure that each
     * version exists and is a part of the same parent project are performed before updating.
     */
    private static OtpServer updateServer(Request req, Response res) {
        OtpServer serverToUpdate = checkServerPermissions(req, res);
        Document updateDocument = Document.parse(req.body());
        Auth0UserProfile user = req.attribute("user");
        if (serverToUpdate.admin && !user.canAdministerApplication()) {
            haltWithMessage(req, 401, "User cannot modify admin-only server.");
        }
        // FIXME use generic update hook, also feedVersions is getting serialized into MongoDB (which is undesirable)
        validateFields(req, updateDocument);
        OtpServer updatedServer = Persistence.servers.update(serverToUpdate.id, req.body());
        return updatedServer;
    }

    private static void validateFields(Request req, Document document) {
        if (document.containsKey("targetGroupArn") && document.get("targetGroupArn") != null) {
            // Validate that the Target Group ARN is valid.
            try {
                DescribeTargetGroupsRequest describeTargetGroupsRequest = new DescribeTargetGroupsRequest().withTargetGroupArns(document.get("targetGroupArn").toString());
                AmazonElasticLoadBalancing elb = AmazonElasticLoadBalancingClient.builder().build();
                List<TargetGroup> targetGroups = elb.describeTargetGroups(describeTargetGroupsRequest).getTargetGroups();
                if (targetGroups.size() == 0) {
                    haltWithMessage(req, 400, "Invalid value for Target Group ARN. Could not locate Target Group.");
                }
            } catch (AmazonElasticLoadBalancingException e) {
                haltWithMessage(req, 400, "Invalid value for Target Group ARN.");
            }

        }
    }

    /**
     * Register HTTP methods with handler methods. NOTE: there is no GET server endpoint because servers are fetched as
     * nested entities under Projects.
     */
    public static void register (String apiPrefix) {
        options(apiPrefix + "secure/servers", (q, s) -> "");
        delete(apiPrefix + "secure/servers/:id", ServerController::deleteServer, json::write);
        post(apiPrefix + "secure/servers", ServerController::createServer, json::write);
        put(apiPrefix + "secure/servers/:id", ServerController::updateServer, json::write);
    }
}
