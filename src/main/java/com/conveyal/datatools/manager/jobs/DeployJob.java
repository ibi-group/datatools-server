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
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Map;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.persistence.FeedStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deploy the given deployment to the OTP servers specified by targets.
 * @author mattwigway
 *
 */
public class DeployJob extends MonitorableJob {

    private static final Logger LOG = LoggerFactory.getLogger(DeployJob.class);
    private static final String bundlePrefix = "bundles/";

    /** The URLs to deploy to */
    private List<String> targets;

    /** The base URL to otp.js on these targets */
    private String publicUrl;

    /** An optional AWS S3 bucket to copy the bundle to */
    private String s3Bucket;

    /** An AWS credentials file to use when uploading to S3 */
    private String s3CredentialsFilename;

    /** The deployment to deploy */
    private Deployment deployment;

    /** Temporary file that contains the deployment data */
    private File deploymentTempFile;

    /** This hides the status field on the parent class, providing additional fields. */
    public DeployStatus status;

    public DeployJob(Deployment deployment, String owner, List<String> targets, String publicUrl, String s3Bucket, String s3CredentialsFilename) {
        // TODO add new job type or get rid of enum in favor of just using class names
        super(owner, "Deploying " + deployment.name, JobType.DEPLOY_TO_OTP);
        this.deployment = deployment;
        this.targets = targets;
        this.publicUrl = publicUrl;
        this.s3Bucket = s3Bucket;
        this.s3CredentialsFilename = s3CredentialsFilename;
        // Use a special subclass of status here that has additional fields
        this.status = new DeployStatus();
        status.message = "Initializing...";
        status.built = false;
        status.numServersCompleted = 0;
        status.totalServers = targets == null ? 0 : targets.size();
    }

    public void jobLogic () {
        int targetCount = targets != null ? targets.size() : 0;
        int totalTasks = 1 + targetCount;
        int tasksCompleted = 0;
        String statusMessage;

        try {
            deploymentTempFile = File.createTempFile("deployment", ".zip");
        } catch (IOException e) {
            statusMessage = "Could not create temp file for deployment";
            LOG.error(statusMessage);
            e.printStackTrace();
            status.update(true, statusMessage, 100, true);
            return;
        }

        LOG.info("Created deployment bundle file: " + deploymentTempFile.getAbsolutePath());

        // dump the deployment bundle
        try {
            status.message = "Creating OTP Bundle";
            this.deployment.dump(deploymentTempFile, true, true, true);
            tasksCompleted++;
        } catch (Exception e) {
            statusMessage = "Error dumping deployment";
            LOG.error(statusMessage);
            e.printStackTrace();
            status.update(true, statusMessage, 100, true);
            return;
        }

        status.percentComplete = 100.0 * (double) tasksCompleted / totalTasks;
        System.out.println("pctComplete = " + status.percentComplete);
        status.built = true;

        // upload to S3, if applicable
        if(this.s3Bucket != null) {
            status.message = "Uploading to S3";
            status.uploadingS3 = true;
            LOG.info("Uploading deployment {} to s3", deployment.name);
            String key = null;
            try {
                TransferManager tx = TransferManagerBuilder.standard().withS3Client(FeedStore.s3Client).build();
                key = bundlePrefix + deployment.parentProject().id + "/" + deployment.name + ".zip";
                final Upload upload = tx.upload(this.s3Bucket, key, deploymentTempFile);

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
                    this.s3Bucket, key, this.s3Bucket, copyKey);
                FeedStore.s3Client.copyObject(copyObjRequest);
            } catch (AmazonClientException|InterruptedException e) {
                statusMessage = String.format("Error uploading (or copying) deployment bundle to s3://%s/%s", s3Bucket, key);
                LOG.error(statusMessage);
                e.printStackTrace();
                status.update(true, statusMessage, 100, true);
                return;
            }

            status.uploadingS3 = false;
        }

        // if no OTP targets (i.e. we're only deploying to S3), we're done
        if(this.targets == null) {
            status.completed = true;
            return;
        }

        // figure out what router we're using
        String router = deployment.routerId != null ? deployment.routerId : "default";

        // load it to OTP
        for (String rawUrl : this.targets) {
            status.message = "Deploying to " + rawUrl;
            status.uploading = true;

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
                status.update(true, statusMessage, 100, true);
                return;
            }

            // retrieveById the input file
            FileChannel input;
            try {
                input = new FileInputStream(deploymentTempFile).getChannel();
            } catch (FileNotFoundException e) {
                LOG.error("Internal error: could not read dumped deployment!");
                status.update(true, "Internal error: could not read dumped deployment!", 100, true);
                return;
            }

            try {
                conn.connect();
            } catch (IOException e) {
                statusMessage = String.format("Unable to open connection to OTP server %s", url);
                LOG.error(statusMessage);
                status.update(true, statusMessage, 100, true);
                return;
            }

            // copy
            try {
                input.transferTo(0, Long.MAX_VALUE, post);
            } catch (IOException e) {
                statusMessage = String.format("Unable to transfer deployment to server %s", url);
                LOG.error(statusMessage);
                e.printStackTrace();
                status.update(true, statusMessage, 100, true);
                return;
            }

            try {
                post.close();
            } catch (IOException e) {
                String message = String.format("Error finishing connection to server %s", url);
                LOG.error(message);
                e.printStackTrace();
                status.update(true, message, 100, true);
                return;
            }

            try {
                input.close();
            } catch (IOException e) {
                // do nothing
            }

            status.uploading = false;

            // wait for the server to build the graph
            // TODO: timeouts?
            try {
                if (conn.getResponseCode() != HttpURLConnection.HTTP_CREATED) {
                    statusMessage = String.format("Got response code %d from server", conn.getResponseCode());
                    LOG.error(statusMessage);
                    status.update(true, statusMessage, 100, true);
                    // Skip deploying to any other servers.
                    // There is no reason to take out the rest of the servers, it's going to have the same result.
                    return;
                }
            } catch (IOException e) {
                statusMessage = String.format("Could not finish request to server %s", url);
                LOG.error(statusMessage);
                status.update(true, statusMessage, 100, true);
            }

            status.numServersCompleted++;
            tasksCompleted++;
            status.percentComplete = 100.0 * (double) tasksCompleted / totalTasks;
        }

        status.completed = true;
        status.baseUrl = this.publicUrl;
    }

    @Override
    public void jobFinished () {
        // Delete temp file containing OTP deployment (OSM extract and GTFS files) so that the server's disk storage
        // does not fill up.
        boolean deleted = deploymentTempFile.delete();
        if (!deleted) {
            LOG.error("Deployment {} not deleted! Disk space in danger of filling up.", deployment.id);
        }

        if (!status.error) {
            // Update status with successful completion state only if no error was encountered.
            status.update(false, "Deployment complete!", 100, true);
        }
    }

    /**
     * Represents the current status of this job.
     */
    public static class DeployStatus extends Status {
//        /** What error message (defined in messages.<lang>) should be displayed to the user? */
//        public String message;
//
//        /** Is this deployment completed (successfully or unsuccessfully) */
//        public boolean completed;

//        /** Was there an error? */
//        public boolean error;

        /** Did the manager build the bundle successfully */
        public boolean built;

//        /** Is the bundle currently being uploaded to the server? */
//        public boolean uploading;

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
