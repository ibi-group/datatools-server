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
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
        Deployment d = Deployment.get(id);
        if (d == null) {
            halt(400, "Deployment does not exist.");
            return null;
        }
        if (!userProfile.canAdministerProject(d.projectId, d.getOrganizationId()) && !userProfile.getUser_id().equals(d.getUser()))
            halt(401);
        else
            return d;

        return null;
    }

    public static Object deleteDeployment (Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        String id = req.params("id");
        Deployment d = Deployment.get(id);
        if (d == null) {
            halt(400, "Deployment does not exist.");
            return null;
        }
        if (!userProfile.canAdministerProject(d.projectId, d.getOrganizationId()) && !userProfile.getUser_id().equals(d.getUser()))
            halt(401);
        else {
            d.delete();
            return d;
        }

        return null;
    }

    /** Download all of the GTFS files in the feed */
    public static Object downloadDeployment (Request req, Response res) throws IOException {
        Auth0UserProfile userProfile = req.attribute("user");
        String id = req.params("id");
        System.out.println(id);
        Deployment d = Deployment.get(id);

        if (d == null) {
            halt(400, "Deployment does not exist.");
            return null;
        }

        if (!userProfile.canAdministerProject(d.projectId, d.getOrganizationId()) && !userProfile.getUser_id().equals(d.getUser()))
            halt(401);

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
        Project project = Project.get(projectId);
        if (!userProfile.canAdministerProject(projectId, project.organizationId))
            halt(401);

        if (projectId != null) {
            Project p = Project.get(projectId);
            return p.getProjectDeployments();
        }
        else {
            return Deployment.getAll();
        }
    }

    public static Object createDeployment (Request req, Response res) throws IOException {
        Auth0UserProfile userProfile = req.attribute("user");
        JsonNode params = mapper.readTree(req.body());

        // find the project
        Project p = Project.get(params.get("projectId").asText());

        if (!userProfile.canAdministerProject(p.id, p.organizationId))
            halt(401);

        Deployment d = new Deployment(p);
        d.setUser(userProfile);

        applyJsonToDeployment(d, params);

        d.save();

        return d;
    }

    /**
     * Create a deployment for a particular feedsource
     * @throws JsonProcessingException
     */
    public static Object createDeploymentFromFeedSource (Request req, Response res) throws JsonProcessingException {
        Auth0UserProfile userProfile = req.attribute("user");
        String id = req.params("id");
        FeedSource s = FeedSource.get(id);

        // three ways to have permission to do this:
        // 1) be an admin
        // 2) be the autogenerated user associated with this feed
        // 3) have access to this feed through project permissions
        // if all fail, the user cannot do this.
        if (
                !userProfile.canAdministerProject(s.projectId, s.getOrganizationId())
                        && !userProfile.getUser_id().equals(s.getUser())
//                        && !userProfile.hasWriteAccess(s.id)
                )
            halt(401);

        // never loaded
        if (s.getLatestVersionId() == null)
            halt(400);

        Deployment d = new Deployment(s);
        d.setUser(userProfile);
        d.save();

        return d;
    }

//    @BodyParser.Of(value=BodyParser.Json.class, maxLength=1024*1024)
    public static Object updateDeployment (Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        String id = req.params("id");
        Deployment d = Deployment.get(id);

        if (d == null)
            halt(404);
        
        if (!userProfile.canAdministerProject(d.projectId, d.getOrganizationId()) && !userProfile.getUser_id().equals(d.getUser()))
            halt(401);

        JsonNode params;
        try {
            params = mapper.readTree(req.body());
            applyJsonToDeployment(d, params);
            d.save();
            return d;
        } catch (IOException e) {
            e.printStackTrace();
            halt(400, SparkUtils.formatJSON("Could not read deployment"));
        }
        return null;
    }

    /**
     * Apply JSON params to a deployment.
     * @param d
     * @param params
     */
    private static void applyJsonToDeployment(Deployment d, JsonNode params) {
        Iterator<Map.Entry<String, JsonNode>> fieldsIter = params.fields();
        while (fieldsIter.hasNext()) {
            Map.Entry<String, JsonNode> entry = fieldsIter.next();
            if (entry.getKey() == "feedVersions") {
                JsonNode versions = entry.getValue();
                ArrayList<FeedVersion> versionsToInsert = new ArrayList<>(versions.size());
                for (JsonNode version : versions) {
                    if (!version.has("id")) {
                        halt(400, SparkUtils.formatJSON("Version not supplied"));
                    }
                    FeedVersion v = null;
                    try {
                        v = FeedVersion.get(version.get("id").asText());
                    } catch (Exception e) {
                        halt(404, SparkUtils.formatJSON("Version not found", 404));
                    }
                    if (v == null) {
                        halt(404, SparkUtils.formatJSON("Version not found", 404));
                    }
                    // check that the version belongs to the correct project
                    if (v.getFeedSource().projectId.equals(d.projectId)) {
                        versionsToInsert.add(v);
                    }
                }

                d.setFeedVersions(versionsToInsert);
            }
            if (entry.getKey() == "name") {
                d.name = entry.getValue().asText();
            }
        }
    }

    /**
     * Create a deployment bundle, and push it to OTP
     * @throws IOException
     */
    public static Object deploy (Request req, Response res) throws IOException {
        Auth0UserProfile userProfile = req.attribute("user");
        String target = req.params("target");
        String id = req.params("id");
        Deployment d = Deployment.get(id);
        Project p = Project.get(d.projectId);

        if (!userProfile.canAdministerProject(d.projectId, d.getOrganizationId()) && !userProfile.getUser_id().equals(d.getUser()))
            halt(401);

        if (!userProfile.canAdministerProject(d.projectId, d.getOrganizationId()) && p.getServer(target).admin)
            halt(401);

        // check if we can deploy
        if (deploymentJobsByServer.containsKey(target)) {
            DeployJob currentJob = deploymentJobsByServer.get(target);
            if (currentJob != null && !currentJob.getStatus().completed) {
                // send a 503 service unavailable as it is not possible to deploy to this target right now;
                // someone else is deploying
                halt(202, "Deployment currently in progress for target: " + target);
                LOG.warn("Deployment currently in progress for target: " + target);
            }
        }
        OtpServer otpServer = p.getServer(target);
        List<String> targetUrls = otpServer.internalUrl;

        Deployment oldD = Deployment.getDeploymentForServerAndRouterId(target, d.routerId);
        if (oldD != null) {
            oldD.deployedTo = null;
            oldD.save();
        }

        d.deployedTo = target;
        d.save();

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
        String target = req.queryParams("target");

        if (!deploymentJobsByServer.containsKey(target))
            halt(404, "Deployment target '"+target+"' not found");

        DeployJob j = deploymentJobsByServer.get(target);

        if (j == null)
            halt(404, "No active job for " + target + " found");

        return j.getStatus();
    }

    /**
     * The servers that it is possible to deploy to.
     */
//    public static Object deploymentTargets (Request req, Response res) {
//        Auth0UserProfile userProfile = req.attribute("user");
//        return DeploymentManager.getDeploymentNames(userProfile.canAdministerApplication());
//    }

    public static void register (String apiPrefix) {
        post(apiPrefix + "secure/deployments/:id/deploy/:target", DeploymentController::deploy, json::write);
        options(apiPrefix + "secure/deployments", (q, s) -> "");
        get(apiPrefix + "secure/deployments/status/:target", DeploymentController::deploymentStatus, json::write);
//        get(apiPrefix + "secure/deployments/targets", DeploymentController::deploymentTargets, json::write);
        get(apiPrefix + "secure/deployments/:id/download", DeploymentController::downloadDeployment);
        get(apiPrefix + "secure/deployments/:id", DeploymentController::getDeployment, json::write);
        delete(apiPrefix + "secure/deployments/:id", DeploymentController::deleteDeployment, json::write);
        get(apiPrefix + "secure/deployments", DeploymentController::getAllDeployments, json::write);
        post(apiPrefix + "secure/deployments", DeploymentController::createDeployment, json::write);
        put(apiPrefix + "secure/deployments/:id", DeploymentController::updateDeployment, json::write);
        post(apiPrefix + "secure/deployments/fromfeedsource/:id", DeploymentController::createDeploymentFromFeedSource, json::write);
    }
}
