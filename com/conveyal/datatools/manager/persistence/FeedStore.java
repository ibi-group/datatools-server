package com.conveyal.datatools.manager.persistence;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.controllers.api.GtfsApiController;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.gtfs.GTFSFeed;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import org.apache.commons.io.IOUtils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
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
    private String s3Bucket;

    public static final String s3Prefix = "gtfs/";

    public static AmazonS3Client s3Client;
    /** An AWS credentials file to use when uploading to S3 */
    private static final String s3CredentialsFilename = DataManager.getConfigPropertyAsText("application.data.s3_credentials_file");

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

        // s3 storage
        if (DataManager.useS3){
            this.s3Bucket = DataManager.getConfigPropertyAsText("application.data.gtfs_s3_bucket");
            s3Client = new AmazonS3Client(getAWSCreds());
        }
    }

    private static File getPath (String pathString) {
        File path = new File(pathString);
        if (!path.exists() || !path.isDirectory()) {
            path = null;
            throw new IllegalArgumentException("Not a directory or not found: " + path.getAbsolutePath());
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
            return s3Client.getObjectMetadata(s3Bucket, getS3Key(id)).getLastModified().getTime();
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
            return s3Client.getObjectMetadata(s3Bucket, getS3Key(id)).getContentLength();
        }
        else {
            File feed = getFeed(id);
            return feed != null ? feed.length() : null;
        }
    }

    private AWSCredentials getAWSCreds () {
        if (this.s3CredentialsFilename != null) {
            return new ProfileCredentialsProvider(this.s3CredentialsFilename, "default").getCredentials();
        } else {
            // default credentials providers, e.g. IAM role
            return new DefaultAWSCredentialsProviderChain().getCredentials();
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
        if (path != null) {
            File feed = new File(path, id);
            // don't let folks get feeds outside of the directory
            if (feed.getParentFile().equals(path) && feed.exists()) return feed;
        }
        // s3 storage
        else if (s3Bucket != null) {
            AWSCredentials creds = getAWSCreds();
            try {
                LOG.info("Downloading feed from s3");
                S3Object object = s3Client.getObject(
                        new GetObjectRequest(s3Bucket, getS3Key(id)));
                InputStream objectData = object.getObjectContent();

                return createTempFile(objectData);
            } catch (AmazonServiceException ase) {
                LOG.error("Error downloading from s3");
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
        // s3 storage (store locally and let gtfsCache handle loading feed to s3)
        if (DataManager.useS3) {
            return storeFeedLocally(id, inputStream, feedSource);
        }
        // local storage
        else if (path != null) {
            return storeFeedLocally(id, inputStream, feedSource);
        }
        return null;
    }

    private File storeFeedLocally(String id, InputStream inputStream, FeedSource feedSource) {
        // store latest as feed-source-id.zip
        if (feedSource != null) {
            try {
//                return copyFileUsingInputStream(id, inputStream, feedSource);
                return copyFileUsingFilesCopy(id, inputStream, feedSource);
            } catch (Exception e) {
                e.printStackTrace();
            }
//            return copyFileUsingFilesCopy(id, inputStream, feedSource);
//            File copy = new File(path, feedSource.id + ".zip");
//            FileOutputStream copyStream;
//            try {
//                copyStream = new FileOutputStream(copy);
//            } catch (FileNotFoundException e) {
//                LOG.error("Unable to save latest at {}", copy);
//                return null;
//            }
        }

//        try {
//            outStream = new FileOutputStream(out);
//        } catch (FileNotFoundException e) {
//            LOG.error("Unable to open {}", out);
//            return null;
//        }
//
//        // copy the file
//        ReadableByteChannel rbc = Channels.newChannel(inputStream);
//        try {
//            outStream.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
//            outStream.close();
//            return out;
//        } catch (IOException e) {
//            LOG.error("Unable to transfer from upload to saved file.");
//            return null;
//        }
        return null;
    }

    private File copyFileUsingFilesCopy(String id, InputStream inputStream, FeedSource feedSource) {
        final Path latest = Paths.get(String.valueOf(path), feedSource.id + ".zip");
        final Path version = Paths.get(String.valueOf(path), id);
        try {
            Files.copy(inputStream, latest, StandardCopyOption.REPLACE_EXISTING);
            LOG.info("Storing feed locally {}", id);
            Files.copy(inputStream, version, StandardCopyOption.REPLACE_EXISTING);
            return version.toFile();
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("Unable to save latest at {}", version);
        }
        return null;
    }

    private File copyFileUsingInputStream(String id, InputStream inputStream, FeedSource feedSource) throws IOException {
        OutputStream output = null;
        File out = new File(path, id);
        try {
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
            try {
                System.out.println(out.length());
            } catch (Exception e) {
                e.printStackTrace();
            }
            return out;
        }
    }

    private File createTempFile (InputStream in) throws IOException {
        final File tempFile = File.createTempFile("test", ".zip");
        tempFile.deleteOnExit();
        try (FileOutputStream out = new FileOutputStream(tempFile)) {
            IOUtils.copy(in, out);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tempFile;
    }

    private File uploadToS3 (InputStream inputStream, String id, FeedSource feedSource) {
        if(this.s3Bucket != null) {
            try {
                // Use tempfile
                LOG.info("Creating temp file for {}", id);
                File tempFile = createTempFile(inputStream);

                LOG.info("Uploading feed {} to S3 from tempfile", id);
                TransferManager tm = new TransferManager(getAWSCreds());
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
//                s3Client.putObject();

                if (feedSource != null){
                    LOG.info("Copying feed on s3 to latest version");

                    // copy to [feedSourceId].zip
                    String copyKey = s3Prefix + feedSource.id + ".zip";
                    CopyObjectRequest copyObjRequest = new CopyObjectRequest(
                            this.s3Bucket, getS3Key(id), this.s3Bucket, copyKey);
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