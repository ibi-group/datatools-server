package com.conveyal.datatools.manager.persistence;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.conveyal.datatools.common.utils.AWSUtils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.google.common.io.ByteStreams;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.conveyal.datatools.manager.DataManager.hasConfigProperty;

/**
 * Store a feed on the file system or S3.
 * @author mattwigway
 *
 */
public class FeedStore {

    public static final Logger LOG = LoggerFactory.getLogger(FeedStore.class);

    /** Local file storage path if working offline */
    public static final File basePath = new File(DataManager.getConfigPropertyAsText("application.data.gtfs"));
    private final File path;
    /** An optional AWS S3 bucket to store the feeds */
    private static String s3Bucket;

    public static final String s3Prefix = "gtfs/";

    static {
        // s3 storage
        if (DataManager.useS3 || hasConfigProperty("modules.gtfsapi.use_extension")){
            s3Bucket = DataManager.getConfigPropertyAsText("application.data.gtfs_s3_bucket");
            try {

                s3Client = AWSUtils.getS3ClientForCredentials(getAWSCreds(), region);
            } catch (Exception e) {
                LOG.error("S3 client not initialized correctly.  Must provide config property application.data.s3_region or specify region in ~/.aws/config", e);
            }
            // TODO: check for this??
            if (s3Client == null || s3Bucket == null) {
                throw new IllegalArgumentException("Fatal error initializing s3Bucket or s3Client");
            }
        }
    }

    public FeedStore() {
        this(null);
    }

    /**
     * Allow construction of feed store in a subdirectory (e.g., "gtfsplus")
     * @param subdir
     */
    public FeedStore(String subdir) {

        // even with s3 storage, we keep a local copy, so we'll still set path.
        String pathString = basePath.getAbsolutePath();
        if (subdir != null) pathString += File.separator + subdir;
        path = getPath(pathString);
    }

    private static File getPath (String pathString) {
        File path = new File(pathString);
        if (!path.exists() || !path.isDirectory()) {
            LOG.error("Directory does not exist {}", pathString);
            throw new IllegalArgumentException("Not a directory or not found: " + pathString);
        }
        return path;
    }

    public void deleteFeed (String id) {
        // If the application is using s3 storage, delete the remote copy.
        if (DataManager.useS3){
            AWSUtils.getS3Client().deleteObject(s3Bucket, getS3Key(id));
        }
        // Always delete local copy (whether storing exclusively on local disk or using s3).
        File feed = getLocalFeed(id);
        if (feed != null) {
            boolean deleted = feed.delete();
            if (!deleted) LOG.warn("GTFS file {} not deleted. This may contribute to storage space shortages.", feed.getAbsolutePath());
        }
    }

    public static AWSCredentialsProvider getAWSCreds () {
        if (S3_CREDENTIALS_FILENAME != null) {
            return new ProfileCredentialsProvider(S3_CREDENTIALS_FILENAME, "default");
        } else {
            // default credentials providers, e.g. IAM role
            return new DefaultAWSCredentialsProviderChain();
        }
    }

    private static String getS3Key (String id) {
        return s3Prefix + id;
    }

    /**
     * Get the File for the provided feed version ID (or other entity ID, depending on the feed store's context).
     */
    public File getFeedFile(String id) {
        return new File(path, id);
    }

    /**
     * Get the feed with the given ID.
     */
    public File getFeed (String id) {
        // Whether storing locally or on s3, first try returning the local copy if it exists.
        File feed = getLocalFeed(id);
        if (feed != null) return feed;
        // s3 storage
        if (DataManager.useS3) {
            String key = getS3Key(id);
            String uri = String.format("s3://%s/%s", s3Bucket, key);
            LOG.info("Downloading feed from {}", uri);
            InputStream objectData;
            try {
                S3Object object = s3Client.getObject(
                    new GetObjectRequest(s3Bucket, key));
                objectData = object.getObjectContent();
            } catch (AmazonServiceException ase) {
                LOG.error("Error downloading " + uri, ase);
                return null;
            }

            try {
                return createTempFile(id, objectData);
            } catch (IOException e) {
                // TODO: Log to bugsnag?
                LOG.error("Error creating temp file", e);
            }
        }
        return null;
    }

    public String getS3FeedPath (String id) {
        return String.format("s3://%s/%s", s3Bucket, getS3Key(id));
    }

    /**
     * Shorthand to get the local file for the provided id.
     */
    private File getLocalFeed(String id) {
        File feed = new File(path, id);
        // Don't let folks retrieveById feeds outside of the directory
        if (feed.getParentFile().equals(path) && feed.exists()) {
            return feed;
        } else {
            return null;
        }
    }

    /**
     * Store GTFS file locally. This method is used when a new feed version or generated GTFS file
     * (e.g., the product of merging multiple GTFS files from a project) needs to be stored locally for
     * future use. Note: uploading the file to S3 is handled elsewhere as a finishing step, e.g., at the
     * conclusion of a successful feed processing/validation step.
     */
    public File newFeed (String id, InputStream inputStream, FeedSource feedSource) throws IOException {
        // write feed to specified ID.
        // NOTE: depending on the feed store, there may not be a feedSource provided (e.g., gtfsplus)
        File file = new File(path, id);
        LOG.info("Writing file to {}", file.getAbsolutePath());
        ByteStreams.copy(inputStream, new FileOutputStream(file));
        if (feedSource != null && !DataManager.useS3) {
            // Store latest as feed-source-id.zip if feedSource provided and if not using s3
            copyVersionToLatest(file, feedSource);
        }
        return file;
    }

    /**
     * Copy the GTFS file for the specified version to feed-source-id.zip, which represents the latest version for the
     * feed source.
     */
    private void copyVersionToLatest(File version, FeedSource feedSource) throws IOException {
        File latest = new File(path, feedSource.id + ".zip");
        LOG.info("Copying version to latest {}", feedSource);
        FileUtils.copyFile(version, latest, true);
    }

    private File createTempFile (String name, InputStream in) throws IOException {
        Path path = Files.createTempFile(name, null);
        // FIXME: Figure out how to manage temp files created here. Currently, we just call deleteOnExit, but
        //  this will only delete the file once the java process stops.
        File tempFile = path.toFile();
        tempFile.deleteOnExit();
        ByteStreams.copy(in, new FileOutputStream(tempFile));
        return tempFile;
    }

    /**
     * Synchronously upload the GTFS file to S3. This should only be called as part of the FeedVersion load stage.
     */
    public boolean uploadToS3 (File gtfsFile, String s3FileName, FeedSource feedSource) {
        if (s3Bucket != null) {
            try {
                LOG.info("Uploading feed {} to S3 from {}", s3FileName, gtfsFile.getAbsolutePath());
                TransferManager tm = TransferManagerBuilder.standard().withS3Client(s3Client).build();
                PutObjectRequest request = new PutObjectRequest(s3Bucket, getS3Key(s3FileName), gtfsFile);
                // Subscribe to the event and provide event handler.
                TLongList transferredBytes = new TLongArrayList();
                long totalBytes = gtfsFile.length();
                LOG.info("Total kilobytes: {}", totalBytes / 1000);
                request.setGeneralProgressListener(progressEvent -> {
                    if (transferredBytes.size() == 75) {
                        LOG.info("Each dot is {} kilobytes",transferredBytes.sum() / 1000);
                    }
                    if (transferredBytes.size() % 75 == 0) {
                        System.out.print(".");
                    }
//                    LOG.info("Uploaded {}/{}", transferredBytes.sum(), totalBytes);
                    transferredBytes.add(progressEvent.getBytesTransferred());
                });
                // TransferManager processes all transfers asynchronously,
                // so this call will return immediately.
                Upload upload = tm.upload(request);

                try {
                    // You can block and wait for the upload to finish
                    upload.waitForCompletion();
                } catch (AmazonClientException | InterruptedException e) {
                    LOG.error("Unable to upload file, upload aborted.", e);
                    return false;
                }

                // Shutdown the Transfer Manager, but don't shut down the underlying S3 client.
                // The default behavior for shutdownNow shut's down the underlying s3 client
                // which will cause any following s3 operations to fail.
                tm.shutdownNow(false);

                if (feedSource != null){
                    LOG.info("Copying feed on s3 to latest version");

                    // copy to [feedSourceId].zip
                    String copyKey = s3Prefix + feedSource.id + ".zip";
                    CopyObjectRequest copyObjRequest = new CopyObjectRequest(
                            s3Bucket, getS3Key(s3FileName), s3Bucket, copyKey);
                    s3Client.copyObject(copyObjRequest);
                }
                return true;
            } catch (AmazonServiceException e) {
                LOG.error("Error uploading feed to S3", e);
                return false;
            }
        }
        return false;
    }
}