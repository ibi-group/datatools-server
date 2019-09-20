package com.conveyal.datatools.manager.controllers.api;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.s3.AmazonS3URI;
import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.DeployJob;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.EC2InstanceSummary;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.mongodb.client.FindIterable;
import org.bson.Document;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.conveyal.datatools.common.utils.S3Utils.downloadFromS3;
import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.options;
import static spark.Spark.post;
import static spark.Spark.put;

/**
 * Handlers for HTTP API requests that affect Deployments.
 * These methods are mapped to API endpoints by Spark.
 */
public class DeploymentController {
    private static JsonManager<Deployment> json = new JsonManager<>(Deployment.class, JsonViews.UserInterface.class);
    private static final Logger LOG = LoggerFactory.getLogger(DeploymentController.class);
    private static Map<String, DeployJob> deploymentJobsByServer = new HashMap<>();
    private static final AmazonEC2 ec2 = AmazonEC2Client.builder().build();

    /**
     * Gets the deployment specified by the request's id parameter and ensure that user has access to the
     * deployment. If the user does not have permission the Spark request is halted with an error.
     */
    private static Deployment checkDeploymentPermissions (Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        String deploymentId = req.params("id");
        Deployment deployment = Persistence.deployments.getById(deploymentId);
        if (deployment == null) {
            logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Deployment does not exist.");
        }
        boolean isProjectAdmin = userProfile.canAdministerProject(deployment.projectId, deployment.organizationId());
        if (!isProjectAdmin && !userProfile.getUser_id().equals(deployment.user())) {
            // If user is not a project admin and did not create the deployment, access to the deployment is denied.
            logMessageAndHalt(req, HttpStatus.UNAUTHORIZED_401, "User not authorized for deployment.");
        }
        return deployment;
    }

    private static Deployment getDeployment (Request req, Response res) {
        return checkDeploymentPermissions(req, res);
    }

    private static Deployment deleteDeployment (Request req, Response res) {
        Deployment deployment = checkDeploymentPermissions(req, res);
        deployment.delete();
        return deployment;
    }

    /**
     * HTTP endpoint for downloading a build artifact (e.g., otp build log or Graph.obj) from S3.
     */
    private static String downloadBuildArtifact (Request req, Response res) {
        Deployment deployment = checkDeploymentPermissions(req, res);
        DeployJob.DeploySummary summaryToDownload = null;
        String uriString = null;
        // If a jobId query param is provided, find the matching job summary.
        String jobId = req.queryParams("jobId");
        if (jobId != null) {
            for (DeployJob.DeploySummary summary : deployment.deployJobSummaries) {
                if (summary.jobId.equals(jobId)) {
                    summaryToDownload = summary;
                    break;
                }
            }
        } else {
            summaryToDownload = deployment.latest();
        }
        if (summaryToDownload == null) {
            // Try to construct the URI string
            OtpServer server = Persistence.servers.getById(deployment.deployedTo);
            if (server == null) {
                uriString = String.format("s3://%s/bundles/%s/%s/%s", "S3_BUCKET", deployment.projectId, deployment.id, jobId);
                logMessageAndHalt(req, 400, "Cannot construct URI for build artifact. " + uriString);
                return null;
            }
            uriString = String.format("s3://%s/bundles/%s/%s/%s", server.s3Bucket, deployment.projectId, deployment.id, jobId);
            LOG.warn("Could not find deploy summary for job. Attempting to use {}", uriString);
        } else {
            uriString = summaryToDownload.buildArtifactsFolder;
        }
        AmazonS3URI uri = new AmazonS3URI(uriString);
        String filename = req.queryParams("filename");
        if (filename == null) {
            logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Must provide filename query param for build artifact.");
        }
        return downloadFromS3(FeedStore.s3Client, uri.getBucket(), String.join("/", uri.getKey(), filename), false, res);
    }

    /**
     * Download all of the GTFS files in the feed.
     *
     * TODO: Should there be an option to download the OSM network as well?
     */
    private static FileInputStream downloadDeployment (Request req, Response res) throws IOException {
        Deployment deployment = checkDeploymentPermissions(req, res);
        // Create temp file in order to generate input stream.
        File temp = File.createTempFile("deployment", ".zip");
        // just include GTFS, not any of the ancillary information
        deployment.dump(temp, false, false, false);
        FileInputStream fis = new FileInputStream(temp);
        String cleanName = deployment.name.replaceAll("[^a-zA-Z0-9]", "");
        res.type("application/zip");
        res.header("Content-Disposition", String.format("attachment;filename=%s.zip", cleanName));

        // Delete temp file to avoid filling up disk space.
        // Note: file will not actually be deleted until download has completed.
        // http://stackoverflow.com/questions/24372279
        if (temp.delete()) {
            LOG.info("Temp deployment file at {} successfully deleted.", temp.getAbsolutePath());
        } else {
            LOG.warn("Temp deployment file at {} could not be deleted. Disk space may fill up!", temp.getAbsolutePath());
        }

        return fis;
    }

    /**
     * Spark HTTP controller that returns a list of deployments for the entire application, a single project, or a single
     * feed source (test deployments) depending on the query parameters supplied (e.g., projectId or feedSourceId)
     */
    private static Collection<Deployment> getAllDeployments (Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        String projectId = req.queryParams("projectId");
        String feedSourceId = req.queryParams("feedSourceId");
        if (projectId != null) {
            // Return deployments for project
            Project project = Persistence.projects.getById(projectId);
            if (project == null) logMessageAndHalt(req, 400, "Must provide valid projectId value.");
            if (!userProfile.canAdministerProject(projectId, project.organizationId))
                logMessageAndHalt(req, 401, "User not authorized to view project deployments.");
            return project.retrieveDeployments();
        } else if (feedSourceId != null) {
            // Return test deployments for feed source (note: these only include test deployments specific to the feed
            // source and will not include all deployments that reference this feed source).
            FeedSource feedSource = Persistence.feedSources.getById(feedSourceId);
            if (feedSource == null) logMessageAndHalt(req, 400, "Must provide valid feedSourceId value.");
            Project project = feedSource.retrieveProject();
            if (!userProfile.canViewFeed(project.organizationId, project.id, feedSourceId))
                logMessageAndHalt(req, 401, "User not authorized to view feed source deployments.");
            return feedSource.retrieveDeployments();
        } else {
            // If no query parameter is supplied, return all deployments for application.
            if (!userProfile.canAdministerApplication())
                logMessageAndHalt(req, 401, "User not authorized to view application deployments.");
            return Persistence.deployments.getAll();
        }
    }

    /**
     * Create a new deployment for the project. All feed sources with a valid latest version are added to the new
     * deployment.
     */
    private static Deployment createDeployment (Request req, Response res) {
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
            logMessageAndHalt(req, 403, "Not authorized to create a deployment for project " + projectId);
            return null;
        }
    }

    /**
     * Create a deployment for a particular feed source.
     */
    private static Deployment createDeploymentFromFeedSource (Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        String id = req.params("id");
        FeedSource feedSource = Persistence.feedSources.getById(id);

        // three ways to have permission to do this:
        // 1) be an admin
        // 2) be the autogenerated user associated with this feed
        // 3) have access to this feed through project permissions
        // if all fail, the user cannot do this.
        if (
                !userProfile.canAdministerProject(feedSource.projectId, feedSource.organizationId()) &&
                !userProfile.getUser_id().equals(feedSource.user())
            )
            logMessageAndHalt(req, 401, "User not authorized to perform this action");

        if (feedSource.latestVersionId() == null)
            logMessageAndHalt(req, 400, "Cannot create a deployment from a feed source with no versions.");

        Deployment deployment = new Deployment(feedSource);
        deployment.storeUser(userProfile);
        Persistence.deployments.create(deployment);
        return deployment;
    }

//    @BodyParser.Of(value=BodyParser.Json.class, maxLength=1024*1024)

    /**
     * Update a single deployment. If the deployment's feed versions are updated, checks to ensure that each
     * version exists and is a part of the same parent project are performed before updating.
     */
    private static Object updateDeployment (Request req, Response res) {
        Deployment deploymentToUpdate = checkDeploymentPermissions(req, res);
        Document updateDocument = Document.parse(req.body());
        // FIXME use generic update hook, also feedVersions is getting serialized into MongoDB (which is undesirable)
        // Check that feed versions in request body are OK to add to deployment, i.e., they exist and are a part of
        // this project.
        if (updateDocument.containsKey("feedVersions")) {
            List<Document> versions = (List<Document>) updateDocument.get("feedVersions");
            ArrayList<FeedVersion> versionsToInsert = new ArrayList<>(versions.size());
            for (Document version : versions) {
                if (!version.containsKey("id")) {
                    logMessageAndHalt(req, 400, "Version not supplied");
                }
                FeedVersion feedVersion = null;
                try {
                    feedVersion = Persistence.feedVersions.getById(version.getString("id"));
                } catch (Exception e) {
                    logMessageAndHalt(req, 404, "Version not found");
                }
                if (feedVersion == null) {
                    logMessageAndHalt(req, 404, "Version not found");
                }
                // check that the version belongs to the correct project
                if (feedVersion.parentFeedSource().projectId.equals(deploymentToUpdate.projectId)) {
                    versionsToInsert.add(feedVersion);
                }
            }

            // Update deployment feedVersionIds field.
            List<String> versionIds = versionsToInsert.stream().map(v -> v.id).collect(Collectors.toList());
            Persistence.deployments.updateField(deploymentToUpdate.id, "feedVersionIds", versionIds);
        }
        Deployment updatedDeployment = Persistence.deployments.update(deploymentToUpdate.id, req.body());
        // TODO: Should updates to the deployment's fields trigger a notification to subscribers? This could get
        // very noisy.
        // Notify subscribed users of changes to deployment.
//            NotifyUsersForSubscriptionJob.createNotification(
//                    "deployment-updated",
//                    deploymentId,
//                    String.format("Deployment %s properties updated.", deploymentToUpdate.name)
//            );
        return updatedDeployment;
    }

    /**
     * HTTP controller to fetch information about provided EC2 machines that power ELBs running a trip planner.
     */
    private static List<EC2InstanceSummary> fetchEC2InstanceSummaries(Request req, Response res) {
        Deployment deployment = checkDeploymentPermissions(req, res);
        return deployment.retrieveEC2Instances();
    }

    /**
     * Fetches list of {@link EC2InstanceSummary} for all instances matching the provided filters.
     */
    public static List<EC2InstanceSummary> fetchEC2InstanceSummaries(Filter... filters) {
        return fetchEC2Instances(filters).stream().map(EC2InstanceSummary::new).collect(Collectors.toList());
    }

    /**
     * Fetch EC2 instances from AWS that match the provided set of filters (e.g., tags, instance ID, or other properties).
     */
    public static List<Instance> fetchEC2Instances(Filter... filters) {
        List<Instance> instances = new ArrayList<>();
        DescribeInstancesRequest request = new DescribeInstancesRequest().withFilters(filters);
        DescribeInstancesResult result = ec2.describeInstances(request);
        for (Reservation reservation : result.getReservations()) {
            instances.addAll(reservation.getInstances());
        }
        // Sort by launch time (most recent first).
        instances.sort(Comparator.comparing(Instance::getLaunchTime).reversed());
        return instances;
    }

    /**
     * Create a deployment bundle, and send it to the specified OTP target servers (or the specified s3 bucket).
     */
    private static String deploy (Request req, Response res) {
        // Check parameters supplied in request for validity.
        Auth0UserProfile userProfile = req.attribute("user");
        String target = req.params("target");
        Deployment deployment = checkDeploymentPermissions(req, res);
        Project project = Persistence.projects.getById(deployment.projectId);
        if (project == null)
            logMessageAndHalt(req, 400, "Internal reference error. Deployment's project ID is invalid");
        // Get server by ID
        OtpServer otpServer = Persistence.servers.getById(target);
        if (otpServer == null) {
            logMessageAndHalt(req, 400, "Must provide valid OTP server target ID.");
            return null;
        }

        // Check that permissions of user allow them to deploy to target.
        boolean isProjectAdmin = userProfile.canAdministerProject(deployment.projectId, deployment.organizationId());
        if (!isProjectAdmin && otpServer.admin) {
            logMessageAndHalt(req, 401, "User not authorized to deploy to admin-only target OTP server.");
        }

        // Check that we can deploy to the specified target. (Any deploy job for the target that is presently active will
        // cause a halt.)
        if (deploymentJobsByServer.containsKey(target)) {
            // There is a deploy job for the server. Check if it is active.
            DeployJob deployJob = deploymentJobsByServer.get(target);
            if (deployJob != null && !deployJob.status.completed) {
                // Job for the target is still active! Send a 202 to the requester to indicate that it is not possible
                // to deploy to this target right now because someone else is deploying.
                String message = String.format(
                        "Will not process request to deploy %s. Deployment currently in progress for target: %s",
                        deployment.name,
                        target);
                LOG.warn(message);
                logMessageAndHalt(req, HttpStatus.ACCEPTED_202, message);
            }
        }

        // Get the URLs to deploy to.
        List<String> targetUrls = otpServer.internalUrl;
        if ((targetUrls == null || targetUrls.isEmpty()) && (otpServer.s3Bucket == null || otpServer.s3Bucket.isEmpty())) {
            logMessageAndHalt(
                req,
                400,
                String.format("OTP server %s has no internal URL or s3 bucket specified.", otpServer.name)
            );
        }

        // For any previous deployments sent to the server/router combination, set deployedTo to null because
        // this new one will overwrite it. NOTE: deployedTo for the current deployment will only be updated after the
        // successful completion of the deploy job.
        for (Deployment oldDeployment : Deployment.retrieveDeploymentForServerAndRouterId(target, deployment.routerId)) {
            LOG.info("Setting deployment target to null id={}", oldDeployment.id);
            Persistence.deployments.updateField(oldDeployment.id, "deployedTo", null);
        }

        // Execute the deployment job and keep track of it in the jobs for server map.
        DeployJob job = new DeployJob(deployment, userProfile.getUser_id(), otpServer);
        DataManager.heavyExecutor.execute(job);
        deploymentJobsByServer.put(target, job);

        return SparkUtils.formatJobMessage(job.jobId, "Deployment initiating.");
    }

    public static void register (String apiPrefix) {
        post(apiPrefix + "secure/deployments/:id/deploy/:target", DeploymentController::deploy, json::write);
        post(apiPrefix + "secure/deployments/:id/deploy/", ((request, response) -> {
            logMessageAndHalt(request, 400, "Must provide valid deployment target name");
            return null;
        }), json::write);
        options(apiPrefix + "secure/deployments", (q, s) -> "");
        get(apiPrefix + "secure/deployments/:id/download", DeploymentController::downloadDeployment);
        get(apiPrefix + "secure/deployments/:id/artifact", DeploymentController::downloadBuildArtifact);
        get(apiPrefix + "secure/deployments/:id/ec2", DeploymentController::fetchEC2InstanceSummaries, json::write);
        // TODO: In the future, we may have need for terminating a single EC2 instance. For now, an admin using the AWS
        //  console should suffice.
//        delete(apiPrefix + "secure/deployments/:id/ec2", DeploymentController::terminateEC2Instance, json::write);
        get(apiPrefix + "secure/deployments/:id", DeploymentController::getDeployment, json::write);
        delete(apiPrefix + "secure/deployments/:id", DeploymentController::deleteDeployment, json::write);
        get(apiPrefix + "secure/deployments", DeploymentController::getAllDeployments, json::write);
        post(apiPrefix + "secure/deployments", DeploymentController::createDeployment, json::write);
        put(apiPrefix + "secure/deployments/:id", DeploymentController::updateDeployment, json::write);
        post(apiPrefix + "secure/deployments/fromfeedsource/:id", DeploymentController::createDeploymentFromFeedSource, json::write);
    }
}
