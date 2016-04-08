package com.conveyal.datatools.manager.persistence;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.*;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Store a feed on the file system
 * @author mattwigway
 *
 */
public class FeedStore {

    public static final Logger LOG = LoggerFactory.getLogger(FeedStore.class);


    /** Local file storage path if working offline */
    private File path;

    /** An optional AWS S3 bucket to store the feeds */
    private String s3Bucket;

    /** An AWS credentials file to use when uploading to S3 */
    private String s3CredentialsFilename;

    public FeedStore() {
        // s3 storage
        if (DataManager.config.get("application").get("data").get("use_s3_storage").asBoolean()){
            this.s3Bucket = DataManager.config.get("application").get("data").get("gtfs_s3_bucket").asText();
        }
        // local storage
        else {
            String pathString = DataManager.config.get("application").get("data").get("gtfs").asText();
            File path = new File(pathString);
            if (!path.exists() || !path.isDirectory()) {
                throw new IllegalArgumentException("Not a directory or not found: " + path.getAbsolutePath());
            }
            this.path = path;
        }

    }

    public List<String> getAllFeeds () {
        ArrayList<String> ret = new ArrayList<String>();

        // local storage
        if (path != null) {
            for (File file : path.listFiles()) {
                ret.add(file.getName());
            }
        }
        // s3 storage
        else {

        }
        return ret;
    }

    /**
     * Get the feed with the given ID.
     */
    public File getFeed (String id) {
        // local storage
        if (path != null) {
            File feed = new File(path, id);
            if (!feed.exists()) return null;
            // don't let folks get feeds outside of the directory
            if (!feed.getParentFile().equals(path)) return null;
            else return feed;
        }
        // s3 storage
        else {

            if(this.s3Bucket != null) {
                AWSCredentials creds;
                if (this.s3CredentialsFilename != null) {
                    creds = new ProfileCredentialsProvider(this.s3CredentialsFilename, "default").getCredentials();
                } else {
                    // default credentials providers, e.g. IAM role
                    creds = new DefaultAWSCredentialsProviderChain().getCredentials();
                }
                try {
                    LOG.info("Downloading feed from s3");
                    AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
                    S3Object object = s3Client.getObject(
                            new GetObjectRequest(s3Bucket, id));
                    InputStream objectData = object.getObjectContent();

                    // Process the objectData stream.
                    File tempFile = File.createTempFile("test", ".zip");
                    try (FileOutputStream out = new FileOutputStream(tempFile)) {
                        IOUtils.copy(objectData, out);
                        out.close();
                        objectData.close();
                    }
                    tempFile.deleteOnExit();
                    return tempFile;
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (AmazonServiceException ase) {
                    LOG.error("Error downloading from s3");
                    ase.printStackTrace();
                }

            }
            return null;
        }

    }

    /**
     * Create a new feed with the given ID.
     */
    public File newFeed (String id, InputStream inputStream, FeedSource feedSource) {
        // local storage
        if (path != null) {
            File out = new File(path, id);
            FileOutputStream outStream;

            try {
                outStream = new FileOutputStream(out);
            } catch (FileNotFoundException e) {
                LOG.error("Unable to open {}", out);
                return null;
            }

            // copy the file
            ReadableByteChannel rbc = Channels.newChannel(inputStream);
            try {
                outStream.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
                outStream.close();
                return out;
            } catch (IOException e) {
                LOG.error("Unable to transfer from upload to saved file.");
                return null;
            }
        }
        // s3 storage
        else {
            // upload to S3, if applicable
            if(this.s3Bucket != null) {
                AWSCredentials creds;
                if (this.s3CredentialsFilename != null) {
                    creds = new ProfileCredentialsProvider(this.s3CredentialsFilename, "default").getCredentials();
                }
                else {
                    // default credentials providers, e.g. IAM role
                    creds = new DefaultAWSCredentialsProviderChain().getCredentials();
                }


                String keyName = id;

                try {
                    // Use tempfile
                    File tempFile = File.createTempFile("test", ".zip");
                    tempFile.deleteOnExit();
                    try (FileOutputStream out = new FileOutputStream(tempFile)) {
                        IOUtils.copy(inputStream, out);
                        out.close();
                        inputStream.close();
                    }

                    LOG.info("Uploading feed to S3 from inputstream");
                    AmazonS3 s3client = new AmazonS3Client(creds);
                    s3client.putObject(new PutObjectRequest(
                            s3Bucket, keyName, tempFile));

                    if (feedSource != null){
                        LOG.info("Copying feed on s3 to latest version");
                        // copy to [name]-latest.zip
                        String copyKey = feedSource.id + ".zip";
                        CopyObjectRequest copyObjRequest = new CopyObjectRequest(
                                this.s3Bucket, keyName, this.s3Bucket, copyKey);
                        s3client.copyObject(copyObjRequest);
                    }


                    return tempFile;


                }

                catch (AmazonServiceException ase) {
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
}