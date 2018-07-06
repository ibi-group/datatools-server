package com.conveyal.datatools.manager.jobs;

import com.amazonaws.AmazonClientException;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.not;
import static com.mongodb.client.model.Updates.pull;
import static com.mongodb.client.model.Updates.set;

/**
 * Deploy the given deployment to the OTP servers specified by targets.
 * @author mattwigway
 *
 */
public class DeployJob extends MonitorableJob {

    private static final Logger LOG = LoggerFactory.getLogger(DeployJob.class);
    private static final String bundlePrefix = "bundles/";

    /** The deployment to deploy */
    private Deployment deployment;

    /** The OTP server to deploy to (also contains S3 information). */
    private final OtpServer otpServer;

    /** Temporary file that contains the deployment data */
    private File deploymentTempFile;

    /** This hides the status field on the parent class, providing additional fields. */
    public DeployStatus status;

    @JsonProperty
    public String getDeploymentId () {
        return deployment.id;
    }

    public DeployJob(Deployment deployment, String owner, OtpServer otpServer) {
        // TODO add new job type or get rid of enum in favor of just using class names
        super(owner, "Deploying " + deployment.name, JobType.DEPLOY_TO_OTP);
        this.deployment = deployment;
        this.otpServer = otpServer;
        // Use a special subclass of status here that has additional fields
        this.status = new DeployStatus();
        status.message = "Initializing...";
        status.built = false;
        status.numServersCompleted = 0;
        status.totalServers = otpServer.internalUrl == null ? 0 : otpServer.internalUrl.size();
    }

    public void jobLogic () {
        int targetCount = otpServer.internalUrl != null ? otpServer.internalUrl.size() : 0;
        int totalTasks = 1 + targetCount;
        int tasksCompleted = 0;
        String statusMessage;

        try {
            deploymentTempFile = File.createTempFile("deployment", ".zip");
        } catch (IOException e) {
            statusMessage = "Could not create temp file for deployment";
            LOG.error(statusMessage);
            e.printStackTrace();
            status.fail(statusMessage);
            return;
        }

        LOG.info("Created deployment bundle file: " + deploymentTempFile.getAbsolutePath());

        // Dump the deployment bundle to the temp file.
        try {
            status.message = "Creating OTP Bundle";
            this.deployment.dump(deploymentTempFile, true, true, true);
            tasksCompleted++;
        } catch (Exception e) {
            statusMessage = "Error dumping deployment";
            LOG.error(statusMessage);
            e.printStackTrace();
            status.fail(statusMessage);
            return;
        }

        status.percentComplete = 100.0 * (double) tasksCompleted / totalTasks;
        LOG.info("Deployment pctComplete = {}", status.percentComplete);
        status.built = true;

        // Upload to S3, if applicable
        if(otpServer.s3Bucket != null) {
            status.message = "Uploading to S3";
            status.uploadingS3 = true;
            LOG.info("Uploading deployment {} to s3", deployment.name);
            String key = null;
            try {
                TransferManager tx = TransferManagerBuilder.standard().withS3Client(FeedStore.s3Client).build();
                key = bundlePrefix + deployment.parentProject().id + "/" + deployment.name + ".zip";
                final Upload upload = tx.upload(otpServer.s3Bucket, key, deploymentTempFile);

                upload.addProgressListener((ProgressListener) progressEvent -> {
                    status.percentUploaded = upload.getProgress().getPercentTransferred();
                });

                upload.waitForCompletion();

                // Shutdown the Transfer Manager, but don't shut down the underlying S3 client.
                // The default behavior for shutdownNow shut's down the underlying s3 client
                // which will cause any following s3 operations to fail.
                tx.shutdownNow(false);

                // copy to [name]-latest.zip
                String copyKey = bundlePrefix + deployment.parentProject().id + "/" + deployment.parentProject().name.toLowerCase() + "-latest.zip";
                CopyObjectRequest copyObjRequest = new CopyObjectRequest(
                    otpServer.s3Bucket, key, otpServer.s3Bucket, copyKey);
                FeedStore.s3Client.copyObject(copyObjRequest);
            } catch (AmazonClientException|InterruptedException e) {
                statusMessage = String.format("Error uploading (or copying) deployment bundle to s3://%s/%s", otpServer.s3Bucket, key);
                LOG.error(statusMessage);
                e.printStackTrace();
                status.fail(statusMessage);
                return;
            }

            status.uploadingS3 = false;
        }

        // If there are no OTP targets (i.e. we're only deploying to S3), we're done.
        if(otpServer.internalUrl == null) {
            status.completed = true;
            return;
        }

        // figure out what router we're using
        String router = deployment.routerId != null ? deployment.routerId : "default";

        // Send the deployment file over the wire to each OTP server.
        for (String rawUrl : otpServer.internalUrl) {
            status.message = "Deploying to " + rawUrl;
            status.uploading = true;
            LOG.info(status.message);

            URL url;
            try {
                url = new URL(rawUrl + "/routers/" + router);
            } catch (MalformedURLException e) {
                statusMessage = String.format("Malformed deployment URL %s", rawUrl);
                LOG.error(statusMessage);

                // do not set percentComplete to 100 because we continue to the next server
                // TODO: should this return instead so that the job is cancelled?
                status.error = true;
                status.message = statusMessage;
                continue;
            }

            // grab them synchronously, so that we only take down one OTP server at a time
            HttpURLConnection conn;
            try {
                conn = (HttpURLConnection) url.openConnection();
            } catch (IOException e) {
                statusMessage = String.format("Unable to open URL of OTP server %s", url);
                LOG.error(statusMessage);

                // do not set percentComplete to 100 because we continue to the next server
                // TODO: should this return instead so that the job is cancelled?
                status.error = true;
                status.message = statusMessage;
                continue;
            }

            conn.addRequestProperty("Content-Type", "application/zip");
            conn.setDoOutput(true);
            // graph build can take a long time but not more than an hour, I should think
            conn.setConnectTimeout(60 * 60 * 1000);
            conn.setFixedLengthStreamingMode(deploymentTempFile.length());

            // this makes it a post request so that we can upload our file
            WritableByteChannel post;
            try {
                post = Channels.newChannel(conn.getOutputStream());
            } catch (IOException e) {
                statusMessage = String.format("Could not open channel to OTP server %s", url);
                LOG.error(statusMessage);
                e.printStackTrace();
                status.fail(statusMessage);
                return;
            }

            // retrieveById the input file
            FileChannel input;
            try {
                input = new FileInputStream(deploymentTempFile).getChannel();
            } catch (FileNotFoundException e) {
                LOG.error("Internal error: could not read dumped deployment!");
                status.fail("Internal error: could not read dumped deployment!");
                return;
            }

            try {
                conn.connect();
            } catch (IOException e) {
                statusMessage = String.format("Unable to open connection to OTP server %s", url);
                LOG.error(statusMessage);
                status.fail(statusMessage);
                return;
            }

            // copy
            try {
                input.transferTo(0, Long.MAX_VALUE, post);
            } catch (IOException e) {
                statusMessage = String.format("Unable to transfer deployment to server %s", url);
                LOG.error(statusMessage);
                e.printStackTrace();
                status.fail(statusMessage);
                return;
            }

            try {
                post.close();
            } catch (IOException e) {
                String message = String.format("Error finishing connection to server %s", url);
                LOG.error(message);
                e.printStackTrace();
                status.fail(message);
                return;
            }

            try {
                input.close();
            } catch (IOException e) {
                // do nothing
                LOG.warn("Could not close input stream for deployment file.");
            }

            status.uploading = false;

            // wait for the server to build the graph
            // TODO: timeouts?
            try {
                int code = conn.getResponseCode();
                if (code != HttpURLConnection.HTTP_CREATED) {
                    // Get input/error stream from connection response.
                    InputStream stream = code < HttpURLConnection.HTTP_BAD_REQUEST
                            ? conn.getInputStream()
                            : conn.getErrorStream();
                    String response;
                    try (Scanner scanner = new Scanner(stream)) {
                        scanner.useDelimiter("\\Z");
                        response = scanner.next();
                    }
                    statusMessage = String.format("Got response code %d from server due to %s", code, response);
                    LOG.error(statusMessage);
                    status.fail(statusMessage);
                    // Skip deploying to any other servers.
                    // There is no reason to take out the rest of the servers, it's going to have the same result.
                    return;
                }
            } catch (IOException e) {
                statusMessage = String.format("Could not finish request to server %s", url);
                LOG.error(statusMessage);
                status.fail(statusMessage);
            }

            status.numServersCompleted++;
            tasksCompleted++;
            status.percentComplete = 100.0 * (double) tasksCompleted / totalTasks;
        }

        status.completed = true;
        status.baseUrl = otpServer.publicUrl;
    }

    @Override
    public void jobFinished () {
        // Delete temp file containing OTP deployment (OSM extract and GTFS files) so that the server's disk storage
        // does not fill up.
        boolean deleted = deploymentTempFile.delete();
        if (!deleted) {
            LOG.error("Deployment {} not deleted! Disk space in danger of filling up.", deployment.id);
        }
        String message;
        if (!status.error) {
            // Update status with successful completion state only if no error was encountered.
            status.update(false, "Deployment complete!", 100, true);
            // Store the target server in the deployedTo field.
            LOG.info("Updating deployment target to {} id={}", otpServer.target(), deployment.id);
            Persistence.deployments.updateField(deployment.id, "deployedTo", otpServer.target());
            // Update last deployed field.
            Persistence.deployments.updateField(deployment.id, "lastDeployed", new Date());
            message = String.format("Deployment %s successfully deployed to %s", deployment.name, otpServer.publicUrl);
        } else {
            message = String.format("WARNING: Deployment %s failed to deploy to %s", deployment.name, otpServer.publicUrl);
        }
        // Send notification to those subscribed to updates for the deployment.
        NotifyUsersForSubscriptionJob.createNotification("deployment-updated", deployment.id, message);
    }

    /**
     * Represents the current status of this job.
     */
    public static class DeployStatus extends Status {
        private static final long serialVersionUID = 1L;
        /** Did the manager build the bundle successfully */
        public boolean built;

        /** Is the bundle currently being uploaded to an S3 bucket? */
        public boolean uploadingS3;

        /** How much of the bundle has been uploaded? */
        public double percentUploaded;

        /** To how many servers have we successfully deployed thus far? */
        public int numServersCompleted;

        /** How many servers are we attempting to deploy to? */
        public int totalServers;

        /** Where can the user see the result? */
        public String baseUrl;

    }
}
