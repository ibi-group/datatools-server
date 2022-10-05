package com.conveyal.datatools.manager.controllers.api;

import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.common.utils.aws.CheckedAWSException;
import com.conveyal.datatools.common.utils.aws.EC2Utils;
import com.conveyal.datatools.common.utils.aws.S3Utils;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.DeployJob;
import com.conveyal.datatools.manager.jobs.GisExportJob;
import com.conveyal.datatools.manager.jobs.DeploymentGisExportJob;
import com.conveyal.datatools.manager.jobs.PeliasUpdateJob;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.DeploymentSummary;
import com.conveyal.datatools.manager.models.EC2InstanceSummary;
import com.conveyal.datatools.manager.models.FeedDownloadToken;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.JobUtils;
import com.conveyal.datatools.manager.utils.json.JsonManager;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;
import static com.conveyal.datatools.manager.DataManager.isExtensionEnabled;
import static com.conveyal.datatools.manager.jobs.DeployJob.bundlePrefix;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
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
    private static final Logger LOG = LoggerFactory.getLogger(DeploymentController.class);

    /**
     * Gets the deployment specified by the request's id parameter and ensure that user has access to the
     * deployment. If the user does not have permission the Spark request is halted with an error.
     */
    private static Deployment getDeploymentWithPermissions(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        String deploymentId = req.params("id");
        Deployment deployment = Persistence.deployments.getById(deploymentId);
        if (deployment == null) {
            logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Deployment does not exist.");
        }
        boolean isProjectAdmin = userProfile.canAdministerProject(deployment);
        if (!isProjectAdmin && !userProfile.getUser_id().equals(deployment.user())) {
            // If user is not a project admin and did not create the deployment, access to the deployment is denied.
            logMessageAndHalt(req, HttpStatus.UNAUTHORIZED_401, "User not authorized for deployment.");
        }
        return deployment;
    }

    private static Deployment getDeployment (Request req, Response res) {
        return getDeploymentWithPermissions(req, res);
    }

    private static Deployment deleteDeployment (Request req, Response res) {
        Deployment deployment = getDeploymentWithPermissions(req, res);
        deployment.delete();
        return deployment;
    }

    /**
     * HTTP endpoint for downloading a build artifact (e.g., otp build log or Graph.obj) from S3.
     */
    private static String downloadBuildArtifact (Request req, Response res) throws CheckedAWSException {
        Deployment deployment = getDeploymentWithPermissions(req, res);
        DeployJob.DeploySummary summaryToDownload = null;
        String role = null;
        String region = null;
        String uriString;
        String filename = req.queryParams("filename");
        if (filename == null) {
            logMessageAndHalt(req, HttpStatus.BAD_REQUEST_400, "Must provide filename query param for build artifact.");
        }
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
            // See if there is an ongoing job for the provided jobId.
            MonitorableJob job = JobUtils.getJobByJobId(jobId);
            if (job instanceof DeployJob) {
                uriString = ((DeployJob) job).getS3FolderURI().toString();
            } else {
                // Try to construct the URI string
                OtpServer server = Persistence.servers.getById(deployment.deployedTo);
                if (server == null) {
                    uriString = String.format("s3://%s/bundles/%s/%s/%s", "S3_BUCKET", deployment.projectId, deployment.id, jobId);
                    logMessageAndHalt(req, 400, "The deployment does not have job history or associated server information to construct URI for build artifact. " + uriString);
                    return null;
                }
                region = server.ec2Info == null ? null : server.ec2Info.region;
                uriString = String.format("s3://%s/bundles/%s/%s/%s", server.s3Bucket, deployment.projectId, deployment.id, jobId);
                LOG.warn("Could not find deploy summary for job. Attempting to use {}", uriString);
            }
        } else {
            // If summary is readily available, just use the ready-to-use build artifacts field.
            uriString = summaryToDownload.buildArtifactsFolder;
            role = summaryToDownload.role;
            region = summaryToDownload.ec2Info == null ? null : summaryToDownload.ec2Info.region;
        }
        AmazonS3URI uri = new AmazonS3URI(uriString);
        // Assume the alternative role if needed to download the deploy artifact.
        return S3Utils.downloadObject(
            S3Utils.getS3Client(role, region),
            uri.getBucket(),
            String.join("/", uri.getKey(), filename),
            false,
            req,
            res
        );
    }

    /**
     * Download all of the GTFS files in the feed.
     *
     * TODO: Should there be an option to download the OSM network as well?
     */
    private static FileInputStream downloadDeployment (Request req, Response res) throws IOException {
        Deployment deployment = getDeploymentWithPermissions(req, res);
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
     * Export all the GIS shapefiles for the deployment.
     */
    private static String downloadGIS (Request req, Response res) throws IOException {
        String type = req.queryParams("type");
        Auth0UserProfile userProfile = req.attribute("user");
        Deployment deployment = getDeploymentWithPermissions(req, res);

        GisExportJob.ExportType exportType = GisExportJob.ExportType.valueOf(type);
        String tempFileName = String.format("%s_%s", type, deployment.name); // e.g. ROUTES_DeploymentName
        File temp = File.createTempFile(tempFileName, ".zip");

        DeploymentGisExportJob gisExportJob = new DeploymentGisExportJob(exportType, deployment, temp, userProfile);
        JobUtils.heavyExecutor.execute(gisExportJob);

        // Do not use S3 to store the file, which should only be stored ephemerally (until requesting
        // user has downloaded file).
        FeedDownloadToken token = new FeedDownloadToken(gisExportJob);
        Persistence.tokens.create(token);

        return SparkUtils.formatJobMessage(gisExportJob.jobId, "Generating shapefile.");
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
            if (!userProfile.canAdministerProject(project)) {
                logMessageAndHalt(req, 401, "User not authorized to view project deployments.");
            }
            return project.retrieveDeployments();
        } else if (feedSourceId != null) {
            // Return test deployments for feed source (note: these only include test deployments specific to the feed
            // source and will not include all deployments that reference this feed source).
            FeedSource feedSource = Persistence.feedSources.getById(feedSourceId);
            if (feedSource == null) logMessageAndHalt(req, 400, "Must provide valid feedSourceId value.");
            if (!userProfile.canViewFeed(feedSource)) {
                logMessageAndHalt(req, 401, "User not authorized to view feed source deployments.");
            }
            return feedSource.retrieveDeployments();
        } else {
            // If no query parameter is supplied, return all deployments for application.
            if (!userProfile.canAdministerApplication()) {
                logMessageAndHalt(req, 401, "User not authorized to view application deployments.");
            }
            return Persistence.deployments.getAll();
        }
    }

    private static Collection<DeploymentSummary> getAllDeploymentSummaries(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        String projectId = req.queryParams("projectId");
        Project project = Persistence.projects.getById(projectId);
        if (project == null) {
            logMessageAndHalt(req, 400, "Must provide valid projectId value.");
        }
        if (!userProfile.canAdministerProject(project)) {
            logMessageAndHalt(req, 401, "User not authorized to view project deployments.");
        }
        return project.retrieveDeploymentSummaries();
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
        Project project = Persistence.projects.getById(projectId);
        boolean allowedToCreate = userProfile.canAdministerProject(project);

        if (allowedToCreate) {
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
                !userProfile.canAdministerProject(feedSource) &&
                !userProfile.getUser_id().equals(feedSource.user())
            )
            logMessageAndHalt(req, 401, "User not authorized to perform this action");

        if (feedSource.latestVersionId() == null)
            logMessageAndHalt(req, 400, "Cannot create a deployment from a feed source with no versions.");
        
        boolean useDefaultRouter = !isExtensionEnabled("nysdot");
        Deployment deployment = new Deployment(feedSource, useDefaultRouter);
        deployment.storeUser(userProfile);
        Persistence.deployments.create(deployment);
        return deployment;
    }

//    @BodyParser.Of(value=BodyParser.Json.class, maxLength=1024*1024)

    /**
     * Update a single deployment. If the deployment's feed versions are updated, checks to ensure that each
     * version exists and is a part of the same parent project are performed before updating.
     */
    private static Deployment updateDeployment (Request req, Response res) {
        Deployment deploymentToUpdate = getDeploymentWithPermissions(req, res);
        Document updateDocument = Document.parse(req.body());
        // FIXME use generic update hook, also feedVersions is getting serialized into MongoDB (which is undesirable)
        // Check that feed versions in request body are OK to add to deployment, i.e., they exist and are a part of
        // this project.
        if (updateDocument.containsKey("feedVersions")) {
            List<Document> versions = (List<Document>) updateDocument.get("feedVersions");
            ArrayList<FeedVersion> versionsToInsert = new ArrayList<>(versions.size());
            for (Document version : versions) {
                FeedVersion feedVersion = null;
                if (version.containsKey("feedSourceId") && version.containsKey("version")) {
                    String feedSourceId = version.getString("feedSourceId");
                    int versionNumber = version.getInteger("version");
                    try {
                        feedVersion = Persistence.feedVersions.getOneFiltered(
                            and(
                                eq("feedSourceId", feedSourceId),
                                eq("version", versionNumber)
                            )
                        );
                    } catch (Exception e) {
                        logMessageAndHalt(req, 404, "Version not found for " + feedSourceId + ":" + versionNumber);
                    }
                } else if (version.containsKey("id")) {
                    String id = version.getString("id");
                    try {
                        feedVersion = Persistence.feedVersions.getById(id);
                    } catch (Exception e) {
                        logMessageAndHalt(req, 404, "Version not found for id: " + id);
                    }
                } else {
                    logMessageAndHalt(req, 400, "Version not supplied with either id OR feedSourceId + version");
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

        // If updatedDocument has deleted a CSV file, also delete that CSV file from S3
        if (updateDocument.containsKey("peliasCsvFiles")) {
            List<String> csvUrls = (List<String>) updateDocument.get("peliasCsvFiles");
            removeDeletedCsvFiles(csvUrls, deploymentToUpdate, req);
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

    // TODO: Add some point it may be useful to refactor DeployJob to allow adding an EC2 instance to an existing job,
    //  but for now that can be achieved by using the AWS EC2 console: choose an EC2 instance to replicate and select
    //  "Run more like this". Then follow the prompts to replicate the instance.
//    private static Object addEC2InstanceToDeployment(Request req, Response res) {
//        Deployment deployment = getDeploymentWithPermissions(req, res);
//        List<EC2InstanceSummary> currentEC2Instances = deployment.retrieveEC2Instances();
//        EC2InstanceSummary ec2ToClone = currentEC2Instances.get(0);
//        RunInstancesRequest request = new RunInstancesRequest();
//        ec2.runInstances()
//        ec2ToClone.
//        DeployJob.DeploySummary latestDeployJob = deployment.latest();
//
//    }

    /**
     * Helper method for update steps which removes all removed csv files from s3.
     * @param csvUrls               The new list of csv files
     * @param deploymentToUpdate    An existing deployment, which contains csv files to check changes against
     * @param req                   A request object used to report failure
     */
    private static void removeDeletedCsvFiles(List<String> csvUrls, Deployment deploymentToUpdate, Request req) {
        // Only delete if the array differs
        if (deploymentToUpdate.peliasCsvFiles != null && !csvUrls.equals(deploymentToUpdate.peliasCsvFiles)) {
            for (String existingCsvUrl : deploymentToUpdate.peliasCsvFiles) {
                // Only delete if the file does not exist in the deployment
                if (!csvUrls.contains(existingCsvUrl)) {
                    try {
                        AmazonS3URI s3URIToDelete = new AmazonS3URI(existingCsvUrl);
                        S3Utils.getDefaultS3Client().deleteObject(new DeleteObjectRequest(s3URIToDelete.getBucket(), s3URIToDelete.getKey()));
                    } catch(Exception e) {
                        logMessageAndHalt(req, 500, "Failed to delete file from S3.", e);
                    }
                }
            }
        }
    }

    /**
     * HTTP endpoint to deregister and terminate a set of instance IDs that are associated with a particular deployment.
     * The intent here is to give the user a device by which they can terminate an EC2 instance that has started up, but
     * is not responding or otherwise failed to successfully become an OTP instance as part of an ELB deployment (or
     * perhaps two people somehow kicked off a deploy job for the same deployment simultaneously and one of the EC2
     * instances has out-of-date data).
     */
    private static boolean terminateEC2InstanceForDeployment(Request req, Response res)
        throws CheckedAWSException {
        Deployment deployment = getDeploymentWithPermissions(req, res);
        String instanceIds = req.queryParams("instanceIds");
        if (instanceIds == null) {
            logMessageAndHalt(req, 400, "Must provide one or more instance IDs.");
            return false;
        }
        List<String> idsToTerminate = Arrays.asList(instanceIds.split(","));
        // Ensure that request does not contain instance IDs which are not associated with this deployment.
        List<EC2InstanceSummary> instances = deployment.retrieveEC2Instances();
        List<String> instanceIdsForDeployment = instances.stream()
            .map(ec2InstanceSummary -> ec2InstanceSummary.instanceId)
            .collect(Collectors.toList());
        // Get the target group ARN from the latest deployment. Surround in a try/catch in case of NPEs.
        // TODO: Perhaps provide some other way to provide the target group ARN.
        DeployJob.DeploySummary latest = deployment.latest();
        if (latest == null || latest.ec2Info == null) {
            logMessageAndHalt(req, 400, "Latest deploy job does not exist or is missing target group ARN.");
            return false;
        }
        String targetGroupArn = latest.ec2Info.targetGroupArn;
        for (String id : idsToTerminate) {
            if (!instanceIdsForDeployment.contains(id)) {
                logMessageAndHalt(req, HttpStatus.UNAUTHORIZED_401, "It is not permitted to terminate an instance that is not associated with deployment " + deployment.id);
                return false;
            }
            int code = instances.get(instanceIdsForDeployment.indexOf(id)).state.getCode();
            // 48 indicates instance is terminated, 32 indicates shutting down. Prohibit terminating an already
            if (code == 48 || code == 32) {
                logMessageAndHalt(req, 400, "Instance is already terminated/shutting down: " + id);
                return false;
            }
        }
        // If checks are ok, terminate instances.
        boolean success = EC2Utils.deRegisterAndTerminateInstances(
            latest.role,
            targetGroupArn,
            latest.ec2Info.region,
            idsToTerminate
        );
        if (!success) {
            logMessageAndHalt(req, 400, "Could not complete termination request");
            return false;
        }
        return true;
    }

    /**
     * HTTP controller to fetch information about provided EC2 machines that power ELBs running a trip planner.
     */
    private static List<EC2InstanceSummary> fetchEC2InstanceSummaries(
        Request req,
        Response res
    ) throws CheckedAWSException {
        Deployment deployment = getDeploymentWithPermissions(req, res);
        return deployment.retrieveEC2Instances();
    }

    /**
     * Create a deployment bundle, and send it to the specified OTP target servers (or the specified s3 bucket).
     */
    private static String deploy (Request req, Response res) {
        // Check parameters supplied in request for validity.
        Auth0UserProfile userProfile = req.attribute("user");
        String target = req.params("target");
        Deployment deployment = getDeploymentWithPermissions(req, res);
        Project project = Persistence.projects.getById(deployment.projectId);
        if (project == null) {
            logMessageAndHalt(req, 400, "Internal reference error. Deployment's project ID is invalid");
        }
        // Get server by ID
        OtpServer otpServer = Persistence.servers.getById(target);
        if (otpServer == null) {
            logMessageAndHalt(req, 400, "Must provide valid OTP server target ID.");
            return null;
        }

        // Check that permissions of user allow them to deploy to target.
        boolean isProjectAdmin = userProfile.canAdministerProject(deployment);
        if (!isProjectAdmin && otpServer.admin) {
            logMessageAndHalt(req, 401, "User not authorized to deploy to admin-only target OTP server.");
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

        // Execute the deployment job and keep track of it in the jobs for server map.
        DeployJob job = JobUtils.queueDeployJob(deployment, userProfile, otpServer);
        if (job == null) {
            // Job for the target is still active! Send a 202 to the requester to indicate that it is not possible
            // to deploy to this target right now because someone else is deploying.
            String message = String.format(
                "Will not process request to deploy %s. Deployment currently in progress for target: %s",
                deployment.name,
                target);
            logMessageAndHalt(req, HttpStatus.ACCEPTED_202, message);
        }
        return SparkUtils.formatJobMessage(job.jobId, "Deployment initiating.");
    }

    /**
     * Create a Pelias update job based on an existing, live deployment
     */
    private static String peliasUpdate (Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        Deployment deployment = getDeploymentWithPermissions(req, res);
        Project project = Persistence.projects.getById(deployment.projectId);
        if (project == null) {
            logMessageAndHalt(req, 400, "Internal reference error. Deployment's project ID is invalid");
        }

        // Execute the pelias update job and keep track of it
        PeliasUpdateJob peliasUpdateJob = new PeliasUpdateJob(userProfile, "Updating Local Places Index", deployment);
        JobUtils.heavyExecutor.execute(peliasUpdateJob);
        return SparkUtils.formatJobMessage(peliasUpdateJob.jobId, "Pelias update initiating.");
    }

    /**
     * Uploads a file from Spark request object to the s3 bucket of the deployment the Pelias Update Job is associated with.
     * Follows https://github.com/ibi-group/datatools-server/blob/dev/src/main/java/com/conveyal/datatools/editor/controllers/api/EditorController.java#L111
     * @return      S3 URL the file has been uploaded to
     */
    private static Deployment uploadToS3 (Request req, Response res) {
        // Check parameters supplied in request for validity.
        Deployment deployment = getDeploymentWithPermissions(req, res);

        String url;
        try {

            String keyName = String.join(
                    "/",
                    bundlePrefix,
                    deployment.projectId,
                    deployment.id,
                    // Where filenames are generated. Prepend random UUID to prevent overwriting
                    UUID.randomUUID().toString()
            );
            url = SparkUtils.uploadMultipartRequestBodyToS3(req, "csvUpload", keyName);

            // Update deployment csvs
            List<String> updatedCsvList = new ArrayList<>(deployment.peliasCsvFiles);
            updatedCsvList.add(url);

            // If this is set, a file is being replaced
            String s3FileToRemove = req.raw().getHeader("urlToDelete");
            if (s3FileToRemove != null) {
                updatedCsvList.remove(s3FileToRemove);
            }

            // Persist changes after removing deleted csv files from s3
            removeDeletedCsvFiles(updatedCsvList, deployment, req);
            return Persistence.deployments.updateField(deployment.id, "peliasCsvFiles", updatedCsvList);

        } catch (Exception e) {
            logMessageAndHalt(req, 500, "Failed to upload file to S3.", e);
            return null;
        }
    }

    public static void register (String apiPrefix) {
        // Construct JSON managers which help serialize the response. Slim JSON is the generic JSON view. Full JSON
        // contains additional fields (at the moment just #ec2Instances) and should only be used when the controller
        // returns a single deployment (slimJson is better suited for a collection). If fullJson is attempted for use
        // with a collection, massive performance issues will ensure (mainly due to multiple calls to AWS EC2).
        JsonManager<Deployment> slimJson = new JsonManager<>(Deployment.class, JsonViews.UserInterface.class);
        JsonManager<Deployment> fullJson = new JsonManager<>(Deployment.class, JsonViews.UserInterface.class);
        fullJson.addMixin(Deployment.class, Deployment.DeploymentWithEc2InstancesMixin.class);

        post(apiPrefix + "secure/deployments/:id/deploy/:target", DeploymentController::deploy, slimJson::write);
        post(apiPrefix + "secure/deployments/:id/updatepelias", DeploymentController::peliasUpdate, slimJson::write);
        post(apiPrefix + "secure/deployments/:id/deploy/", ((request, response) -> {
            logMessageAndHalt(request, 400, "Must provide valid deployment target name");
            return null;
        }), slimJson::write);
        options(apiPrefix + "secure/deployments", (q, s) -> "");
        get(apiPrefix + "secure/deployments/:id/download", DeploymentController::downloadDeployment);
        get(apiPrefix + "secure/deployments/:id/artifact", DeploymentController::downloadBuildArtifact);
        get(apiPrefix + "secure/deployments/:id/ec2", DeploymentController::fetchEC2InstanceSummaries, slimJson::write);
        delete(apiPrefix + "secure/deployments/:id/ec2", DeploymentController::terminateEC2InstanceForDeployment, slimJson::write);
        get(apiPrefix + "secure/deployments/:id", DeploymentController::getDeployment, fullJson::write);
        delete(apiPrefix + "secure/deployments/:id", DeploymentController::deleteDeployment, fullJson::write);
        get(apiPrefix + "secure/deployments", DeploymentController::getAllDeployments, slimJson::write);
        get(apiPrefix + "secure/deploymentSummaries", DeploymentController::getAllDeploymentSummaries, slimJson::write);
        post(apiPrefix + "secure/deployments", DeploymentController::createDeployment, fullJson::write);
        put(apiPrefix + "secure/deployments/:id", DeploymentController::updateDeployment, fullJson::write);
        post(apiPrefix + "secure/deployments/fromfeedsource/:id", DeploymentController::createDeploymentFromFeedSource, fullJson::write);
        post(apiPrefix + "secure/deployments/:id/upload", DeploymentController::uploadToS3, fullJson::write);
        post(apiPrefix + "secure/deployments/:id/shapes", DeploymentController::downloadGIS, fullJson::write);

    }
}
