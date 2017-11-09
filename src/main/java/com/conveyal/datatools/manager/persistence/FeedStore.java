package com.conveyal.datatools.manager.persistence;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.controllers.api.GtfsApiController;
import com.conveyal.datatools.manager.models.FeedSource;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Store a feed on the file system or s3
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

    public static AmazonS3 s3Client;
    /** An AWS credentials file to use when uploading to S3 */
    private static final String S3_CREDENTIALS_FILENAME = DataManager.getConfigPropertyAsText("application.data.s3_credentials_file");
    private static final String S3_CONFIG_FILENAME = DataManager.getConfigPropertyAsText("application.data.s3_credentials_file");

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

    static {
        // s3 storage
        if (DataManager.useS3 || GtfsApiController.extensionType.equals("mtc")){
            s3Bucket = DataManager.getConfigPropertyAsText("application.data.gtfs_s3_bucket");
            AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
                    .withCredentials(getAWSCreds());

            // if region configuration string is provided, use that
            // otherwise default to ~/.aws/config
            // NOTE: if this is missing
            String s3Region = DataManager.getConfigPropertyAsText("application.data.s3_region");
            if (s3Region != null) {
                LOG.info("Using S3 region {}", s3Region);
                builder.withRegion(s3Region);
            }
            try {
                s3Client = builder.build();
            } catch (SdkClientException e) {
                LOG.error("S3 client not initialized correctly.  Must provide config property application.data.s3_region or specify region in ~/.aws/config", e);
            }
        }
    }

    private static File getPath (String pathString) {
        File path = new File(pathString);
        if (!path.exists() || !path.isDirectory()) {
            throw new IllegalArgumentException("Not a directory or not found: " + pathString);
        }
        return path;
    }

    public List<String> getAllFeeds () {
        ArrayList<String> ret = new ArrayList<String>();
        // s3 storage
        if (DataManager.useS3) {
            // TODO: add method for retrieval of all s3 feeds
        }
        // local storage
        else {
            for (File file : path.listFiles()) {
                ret.add(file.getName());
            }
        }

        return ret;
    }

    public Long getFeedLastModified (String id) {
        // s3 storage
        if (DataManager.useS3){
            return s3Client.doesObjectExist(s3Bucket, getS3Key(id)) ? s3Client.getObjectMetadata(s3Bucket, getS3Key(id)).getLastModified().getTime() : null;
        }
        else {
            File feed = getFeed(id);
            return feed != null ? feed.lastModified() : null;
        }
    }

    public void deleteFeed (String id) {
        // s3 storage
        if (DataManager.useS3){
            s3Client.deleteObject(s3Bucket, getS3Key(id));
        }
        else {
            File feed = getFeed(id);
            if (feed != null && feed.exists())
                feed.delete();
        }
    }

    public Long getFeedSize (String id) {
        // s3 storage
        if (DataManager.useS3) {
            return s3Client.doesObjectExist(s3Bucket, getS3Key(id)) ? s3Client.getObjectMetadata(s3Bucket, getS3Key(id)).getContentLength() : null;
        }
        else {
            File feed = getFeed(id);
            return feed != null ? feed.length() : null;
        }
    }

    private static AWSCredentialsProvider getAWSCreds () {
        if (S3_CREDENTIALS_FILENAME != null) {
            return new ProfileCredentialsProvider(S3_CREDENTIALS_FILENAME, "default");
        } else {
            // default credentials providers, e.g. IAM role
            return new DefaultAWSCredentialsProviderChain();
        }
    }

    private String getS3Key (String id) {
        return s3Prefix + id;
    }

    /**
     * Get the feed with the given ID.
     */
    public File getFeed (String id) {
        // local storage
        File feed = new File(path, id);
        // don't let folks get feeds outside of the directory
        if (feed.getParentFile().equals(path) && feed.exists()) return feed;

        // s3 storage
        if (DataManager.useS3) {
            String key = getS3Key(id);
            try {
                LOG.info("Downloading feed from s3://{}/{}", s3Bucket, key);
                S3Object object = s3Client.getObject(
                        new GetObjectRequest(s3Bucket, key));
                InputStream objectData = object.getObjectContent();

                return createTempFile(id, objectData);
            } catch (AmazonServiceException ase) {
                LOG.error("Error downloading s3://{}/{}", s3Bucket, key);
                ase.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * Create a new feed with the given ID.
     */
    public File newFeed (String id, InputStream inputStream, FeedSource feedSource) {
        // For s3 storage (store locally and let gtfsCache handle loading feed to s3)
        return storeFeedLocally(id, inputStream, feedSource);
    }
    private File storeFeedLocally(String id, InputStream inputStream, FeedSource feedSource) {
        File feed = null;
        try {
            // write feed to specified ID.
            // NOTE: depending on the feed store, there may not be a feedSource provided (e.g., gtfsplus)
            feed = writeFileUsingInputStream(id, inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (feedSource != null) {
            try {
                // store latest as feed-source-id.zip if feedSource provided
                copyVersionToLatest(feed, feedSource);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return feed;
    }

    private void copyVersionToLatest(File version, FeedSource feedSource) {
        File latest = new File(String.valueOf(path), feedSource.id + ".zip");
        try {
            FileUtils.copyFile(version, latest, true);
            LOG.info("Copying version to latest {}", feedSource);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("Unable to save latest at {}", feedSource);
        }
    }

    private File writeFileUsingInputStream(String filename, InputStream inputStream) throws IOException {
        OutputStream output = null;
        File out = new File(path, filename);
        try {
            LOG.info("Writing file to {}/{}", path, filename);
            output = new FileOutputStream(out);
            byte[] buf = new byte[1024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buf)) > 0) {
                output.write(buf, 0, bytesRead);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            inputStream.close();
            output.close();
            return out;
        }
    }

    private File createTempFile (String name, InputStream in) throws IOException {
        final File tempFile = new File(new File(System.getProperty("java.io.tmpdir")), name);
        tempFile.deleteOnExit();
        try (FileOutputStream out = new FileOutputStream(tempFile)) {
            IOUtils.copy(in, out);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tempFile;
    }

    private File uploadToS3 (InputStream inputStream, String id, FeedSource feedSource) {
        if (s3Bucket != null) {
            try {
                // Use tempfile
                LOG.info("Creating temp file for {}", id);
                File tempFile = createTempFile(id, inputStream);

                LOG.info("Uploading feed {} to S3 from tempfile", id);
                TransferManager tm = TransferManagerBuilder.standard().withS3Client(s3Client).build();
                PutObjectRequest request = new PutObjectRequest(s3Bucket, getS3Key(id), tempFile);
                // Subscribe to the event and provide event handler.
                TLongList transferredBytes = new TLongArrayList();
                long totalBytes = tempFile.length();
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
                } catch (AmazonClientException amazonClientException) {
                    System.out.println("Unable to upload file, upload aborted.");
                    amazonClientException.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
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
                            s3Bucket, getS3Key(id), s3Bucket, copyKey);
                    s3Client.copyObject(copyObjRequest);
                }
                return tempFile;
            } catch (AmazonServiceException ase) {
                LOG.error("Error uploading feed to S3");
                ase.printStackTrace();
                return null;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}