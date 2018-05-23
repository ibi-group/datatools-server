package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.DeployJob;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static com.conveyal.datatools.common.utils.SparkUtils.haltWithMessage;
import static spark.Spark.*;
import static spark.Spark.get;

/**
 * Created by landon on 5/18/16.
 */
public class DeploymentController {
    private static ObjectMapper mapper = new ObjectMapper();
    private static JsonManager<Deployment> json =
            new JsonManager<Deployment>(Deployment.class, JsonViews.UserInterface.class);

    private static JsonManager<DeployJob.DeployStatus> statusJson =
            new JsonManager<DeployJob.DeployStatus>(DeployJob.DeployStatus.class, JsonViews.UserInterface.class);

    private static final Logger LOG = LoggerFactory.getLogger(DeploymentController.class);

    private static HashMap<String, DeployJob> deploymentJobsByServer = new HashMap<String, DeployJob>();

    public static Object getDeployment (Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        String id = req.params("id");
        Deployment d = Persistence.deployments.getById(id);
        if (d == null) {
            halt(400, "Deployment does not exist.");
            return null;
        }
        if (!userProfile.canAdministerProject(d.projectId, d.organizationId()) && !userProfile.getUser_id().equals(d.user()))
            halt(401);
        else
            return d;

        return null;
    }

    public static Object deleteDeployment (Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        String id = req.params("id");
        Deployment d = Persistence.deployments.getById(id);
        if (d == null) {
            halt(400, "Deployment does not exist.");
            return null;
        }
        if (!userProfile.canAdministerProject(d.projectId, d.organizationId()) && !userProfile.getUser_id().equals(d.user()))
            haltWithMessage(401, "User not authorized to delete deployment");
        else {
            Persistence.deployments.removeById(id);
            return d;
        }
        return null;
    }

    /** Download all of the GTFS files in the feed */
    public static Object downloadDeployment (Request req, Response res) throws IOException {
        Auth0UserProfile userProfile = req.attribute("user");
        String id = req.params("id");
        System.out.println(id);
        Deployment d = Persistence.deployments.getById(id);

        if (d == null) {
            haltWithMessage(400, "Deployment does not exist.");
            return null;
        }

        if (!userProfile.canAdministerProject(d.projectId, d.organizationId()) && !userProfile.getUser_id().equals(d.user()))
            haltWithMessage(401, "User not authorized to download deployment");

        File temp = File.createTempFile("deployment", ".zip");
        // just include GTFS, not any of the ancillary information
        d.dump(temp, false, false, false);

        FileInputStream fis = new FileInputStream(temp);

        res.type("application/zip");
        res.header("Content-Disposition", "attachment;filename=" + d.name.replaceAll("[^a-zA-Z0-9]", "") + ".zip");

        // will not actually be deleted until download has completed
        // http://stackoverflow.com/questions/24372279
        temp.delete();

        return fis;
    }

    public static Object getAllDeployments (Request req, Response res) throws JsonProcessingException {
        Auth0UserProfile userProfile = req.attribute("user");
        String projectId = req.queryParams("projectId");
        Project project = Persistence.projects.getById(projectId);
        if (!userProfile.canAdministerProject(projectId, project.organizationId))
            halt(401);

        if (projectId != null) {
            Project p = Persistence.projects.getById(projectId);
            return p.retrieveDeployments();
        } else {
            return Persistence.deployments.getAll();
        }
    }

    public static Object createDeployment (Request req, Response res) throws IOException {
        // TODO error handling when request is bogus
        // TODO factor out user profile fetching, permissions checks etc.
        Auth0UserProfile userProfile = req.attribute("user");
        Document newDeploymentFields = Document.parse(req.body());
        String projectId = newDeploymentFields.getString("projectId");
        String organizationId = newDeploymentFields.getString("organizationId");

        boolean allowedToCreate = userProfile.canAdministerProject(projectId, organizationId);

        if (allowedToCreate) {
            Project project = Persistence.projects.getById(projectId);
            Deployment newDeployment = new Deployment(project);

            // FIXME: Here we are creating a deployment and updating it with the JSON string (two db operations)
            // We do this because there is not currently apply JSON directly to an object (outside of Mongo codec
            // operations)
            Persistence.deployments.create(newDeployment);
            return Persistence.deployments.update(newDeployment.id, req.body());
        } else {
            haltWithMessage(403, "Not authorized to create a deployment for project " + projectId);
            return null;
        }
    }

    /**
     * Create a deployment for a particular feedsource
     * @throws JsonProcessingException
     */
    public static Object createDeploymentFromFeedSource (Request req, Response res) throws JsonProcessingException {
        Auth0UserProfile userProfile = req.attribute("user");
        String id = req.params("id");
        FeedSource s = Persistence.feedSources.getById(id);

        // three ways to have permission to do this:
        // 1) be an admin
        // 2) be the autogenerated user associated with this feed
        // 3) have access to this feed through project permissions
        // if all fail, the user cannot do this.
        if (
                !userProfile.canAdministerProject(s.projectId, s.organizationId())
                        && !userProfile.getUser_id().equals(s.user())
//                        && !userProfile.hasWriteAccess(s.id)
                )
            halt(401);

        // never loaded
        if (s.latestVersionId() == null)
            halt(400);

        Deployment d = new Deployment(s);
        d.storeUser(userProfile);
        Persistence.deployments.create(d);
        return d;
    }

//    @BodyParser.Of(value=BodyParser.Json.class, maxLength=1024*1024)
    public static Object updateDeployment (Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        String deploymentId = req.params("id");
        Deployment deploymentToUpdate = Persistence.deployments.getById(deploymentId);
        Document updateDocument = Document.parse(req.body());
        if (deploymentToUpdate == null)
            halt(404);

        boolean allowedToUpdate = userProfile.canAdministerProject(deploymentToUpdate.projectId, deploymentToUpdate.organizationId())
                || userProfile.getUser_id().equals(deploymentToUpdate.user());

        // FIXME use generic update hook, also feedVersions is getting serialized into MongoDB (which is undesirable)
        // Check that feed versions in request body are OK to add to deployment, i.e., they exist and are a part of
        // this project.
        if (updateDocument.containsKey("feedVersions")) {
            List<Document> versions = (List<Document>) updateDocument.get("feedVersions");
            ArrayList<FeedVersion> versionsToInsert = new ArrayList<>(versions.size());
            for (Document version : versions) {
                if (!version.containsKey("id")) {
                    halt(400, SparkUtils.formatJSON("Version not supplied"));
                }
                FeedVersion v = null;
                try {
                    v = Persistence.feedVersions.getById(version.getString("id"));
                } catch (Exception e) {
                    haltWithMessage(404, "Version not found");
                }
                if (v == null) {
                    haltWithMessage(404, "Version not found");
                }
                // check that the version belongs to the correct project
                if (v.parentFeedSource().projectId.equals(deploymentToUpdate.projectId)) {
                    versionsToInsert.add(v);
                }
            }

            // Update deployment feedVersionIds field.
            Persistence.deployments.updateField(deploymentId, "feedVersionIds", versionsToInsert.stream()
                    .map(v -> v.id)
                    .collect(Collectors.toList())
            );
        }



        if (allowedToUpdate) {
            Deployment updatedDeployment = Persistence.deployments.update(deploymentId, req.body());
            return updatedDeployment;
        } else {
            haltWithMessage(403, "Not authorized to update deployment " + deploymentId);
            return null;
        }
    }

    /**
     * Create a deployment bundle, and push it to OTP
     */
    public static Object deploy (Request req, Response res) throws IOException {
        Auth0UserProfile userProfile = req.attribute("user");
        String target = req.params("target");
        String id = req.params("id");
        Deployment d = Persistence.deployments.getById(id);
        Project p = Persistence.projects.getById(d.projectId);

        if (!userProfile.canAdministerProject(d.projectId, d.organizationId()) && !userProfile.getUser_id().equals(d.user()))
            halt(401);

        if (!userProfile.canAdministerProject(d.projectId, d.organizationId()) && p.retrieveServer(target).admin)
            halt(401);

        // check if we can deploy
        if (deploymentJobsByServer.containsKey(target)) {
            DeployJob currentJob = deploymentJobsByServer.get(target);
            if (currentJob != null && !currentJob.status.completed) {
                // send a 503 service unavailable as it is not possible to deploy to this target right now;
                // someone else is deploying
                halt(202, "Deployment currently in progress for target: " + target);
                LOG.warn("Deployment currently in progress for target: " + target);
            }
        }
        OtpServer otpServer = p.retrieveServer(target);
        List<String> targetUrls = otpServer.internalUrl;

        Deployment oldDeployment = Deployment.retrieveDeploymentForServerAndRouterId(target, d.routerId);

        // If there was a previous deployment sent to the server/router combination, set that to null because this new
        // one will overwrite it.
        if (oldDeployment != null) {
            Persistence.deployments.updateField(oldDeployment.id, "deployedTo", null);
        }

        // Store the target server in the deployedTo field.
        Persistence.deployments.updateField(d.id, "deployedTo", target);

        DeployJob job = new DeployJob(d, userProfile.getUser_id(), targetUrls, otpServer.publicUrl, otpServer.s3Bucket, otpServer.s3Credentials);
        deploymentJobsByServer.put(target, job);

        DataManager.heavyExecutor.execute(job);

        halt(200, "{status: \"ok\"}");
        return null;
    }

    /**
     * The current status of a deployment, polled to update the progress dialog.
     * @throws JsonProcessingException
     */
    public static Object deploymentStatus (Request req, Response res) throws JsonProcessingException {
        // this is not access-controlled beyond requiring auth, which is fine
        // there's no good way to know who should be able to see this.
        String deploymentTarget = req.queryParams("target");

        if (!deploymentJobsByServer.containsKey(deploymentTarget))
            haltWithMessage(404, "Deployment target '" + deploymentTarget + "' not found");

        DeployJob deployJob = deploymentJobsByServer.get(deploymentTarget);

        if (deployJob == null)
            haltWithMessage(404, "No active job for " + deploymentTarget + " found");

        return deployJob.status;
    }

    public static void register (String apiPrefix) {
        post(apiPrefix + "secure/deployments/:id/deploy/:target", DeploymentController::deploy, json::write);
        options(apiPrefix + "secure/deployments", (q, s) -> "");
        get(apiPrefix + "secure/deployments/status/:target", DeploymentController::deploymentStatus, json::write);
        get(apiPrefix + "secure/deployments/:id/download", DeploymentController::downloadDeployment);
        get(apiPrefix + "secure/deployments/:id", DeploymentController::getDeployment, json::write);
        delete(apiPrefix + "secure/deployments/:id", DeploymentController::deleteDeployment, json::write);
        get(apiPrefix + "secure/deployments", DeploymentController::getAllDeployments, json::write);
        post(apiPrefix + "secure/deployments", DeploymentController::createDeployment, json::write);
        put(apiPrefix + "secure/deployments/:id", DeploymentController::updateDeployment, json::write);
        post(apiPrefix + "secure/deployments/fromfeedsource/:id", DeploymentController::createDeploymentFromFeedSource, json::write);
    }
}
