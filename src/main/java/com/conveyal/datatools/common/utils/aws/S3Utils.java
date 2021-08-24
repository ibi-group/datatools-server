package com.conveyal.datatools.common.utils.aws;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.OtpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.UUID;

import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;
import static com.conveyal.datatools.manager.DataManager.hasConfigProperty;

/**
 * This class contains utilities related to using AWS S3 services.
 */
public class S3Utils {
    private static final Logger LOG = LoggerFactory.getLogger(S3Utils.class);

    private static final int REQUEST_TIMEOUT_MSEC = 30 * 1000;
    private static final AWSCredentialsProvider DEFAULT_S3_CREDENTIALS;
    private static final S3ClientManagerImpl S3ClientManager;

    public static final String DEFAULT_BUCKET;
    public static final String DEFAULT_BUCKET_GTFS_FOLDER = "gtfs/";

    static {
        // Placeholder variables need to be used before setting the final variable to make sure initialization occurs
        AmazonS3 tempS3Client = null;
        AWSCredentialsProvider tempS3CredentialsProvider = null;
        String tempGtfsS3Bucket = null;
        S3ClientManagerImpl tempS3ClientManager = null;

        // Only configure s3 if the config requires doing so
        if (DataManager.useS3 || hasConfigProperty("modules.gtfsapi.use_extension")) {
            try {
                AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
                String credentialsFile = DataManager.getConfigPropertyAsText("application.data.s3_credentials_file");
                tempS3CredentialsProvider = credentialsFile != null ?
                    new ProfileCredentialsProvider(credentialsFile, "default") :
                    new DefaultAWSCredentialsProviderChain(); // default credentials providers, e.g. IAM role
                builder.withCredentials(tempS3CredentialsProvider);

                // If region configuration string is provided, use that.
                // Otherwise defaults to value provided in ~/.aws/config
                String region = DataManager.getConfigPropertyAsText("application.data.s3_region");
                if (region != null) {
                    builder.withRegion(region);
                }
                tempS3Client = builder.build();
            } catch (Exception e) {
                LOG.error(
                    "S3 client not initialized correctly.  Must provide config property application.data.s3_region or specify region in ~/.aws/config",
                    e
                );
            }

            if (tempS3Client == null) {
                throw new IllegalArgumentException("Fatal error initializing the default s3Client");
            }
            tempS3ClientManager = new S3ClientManagerImpl(tempS3Client);

            // s3 storage
            tempGtfsS3Bucket = DataManager.getConfigPropertyAsText("application.data.gtfs_s3_bucket");
            if (tempGtfsS3Bucket == null) {
                throw new IllegalArgumentException("Required config param `application.data.gtfs_s3_bucket` missing!");
            }
        }

        // initialize final fields
        DEFAULT_S3_CREDENTIALS = tempS3CredentialsProvider;
        S3ClientManager = tempS3ClientManager;
        DEFAULT_BUCKET = tempGtfsS3Bucket;
    }

    /**
     * Makes a key for an object id that is assumed to be in the default bucket's GTFS folder
     */
    public static String makeGtfsFolderObjectKey(String id) {
        return DEFAULT_BUCKET_GTFS_FOLDER + id;
    }

    public static String getS3FeedUri(String id) {
        return getDefaultBucketUriForKey(makeGtfsFolderObjectKey(id));
    }

    public static String getDefaultBucketUriForKey(String key) {
        return String.format("s3://%s/%s", DEFAULT_BUCKET, key);
    }

    public static String getDefaultBucketUrlForKey(String key) {
        return String.format("https://s3.amazonaws.com/%s/%s", DEFAULT_BUCKET, key);
    }

    /**
     * A class that manages the creation of S3 clients.
     */
    private static class S3ClientManagerImpl extends AWSClientManager<AmazonS3> {
        public S3ClientManagerImpl(AmazonS3 defaultClient) {
            super(defaultClient);
        }

        @Override
        public AmazonS3 buildDefaultClientWithRegion(String region) {
            return AmazonS3ClientBuilder.standard().withCredentials(DEFAULT_S3_CREDENTIALS).withRegion(region).build();
        }

        @Override
        public AmazonS3 buildCredentialedClientForRoleAndRegion(
            AWSCredentialsProvider credentials, String region, String role
        ) {
            AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
            if (region != null) builder.withRegion(region);
            return builder.withCredentials(credentials).build();
        }
    }

    /**
     * Helper for downloading a file using the default S3 client.
     */
    public static String downloadObject(String bucket, String key, boolean redirect, Request req, Response res) {
        try {
            return downloadObject(getDefaultS3Client(), bucket, key, redirect, req, res);
        } catch (CheckedAWSException e) {
            logMessageAndHalt(req, 500, "Failed to download file from S3.", e);
            return null;
        }
    }

    /**
     * Given a Spark request, download an object in the selected format from S3, using presigned URLs.
     *
     * @param s3 The s3 client to use
     * @param bucket name of the bucket
     * @param key both the key and the format
     * @param redirect whether or not to redirect to the presigned url
     * @param req The underlying Spark request this came from
     * @param res The response to write the download info to
     */
    public static String downloadObject(
        AmazonS3 s3,
        String bucket,
        String key,
        boolean redirect,
        Request req,
        Response res
    ) {
        if (!s3.doesObjectExist(bucket, key)) {
            logMessageAndHalt(
                req,
                500,
                String.format("Error downloading file from S3. Object s3://%s/%s does not exist.", bucket, key)
            );
            return null;
        }

        Date expiration = new Date();
        expiration.setTime(expiration.getTime() + REQUEST_TIMEOUT_MSEC);

        GeneratePresignedUrlRequest presigned = new GeneratePresignedUrlRequest(bucket, key);
        presigned.setExpiration(expiration);
        presigned.setMethod(HttpMethod.GET);
        URL url;
        try {
            url = s3.generatePresignedUrl(presigned);
        } catch (AmazonServiceException e) {
            logMessageAndHalt(req, 500, "Failed to download file from S3.", e);
            return null;
        }

        if (redirect) {
            res.type("text/plain"); // override application/json
            res.redirect(url.toString());
            res.status(302); // temporary redirect, this URL will soon expire
            return null;
        } else {
            return SparkUtils.formatJSON("url", url.toString());
        }
    }

    /**
     * Uploads a file to S3 using a given key
     * @param keyName      The s3 key to uplaod the file to
     * @param fileToUpload The file to upload to S3
     * @return             A URL where the file is publicly accessible
     */
    public static String uploadObject(String keyName, File fileToUpload) throws AmazonServiceException, CheckedAWSException {
        String url = S3Utils.getDefaultBucketUrlForKey(keyName);
        // FIXME: This may need to change during feed store refactor
        getDefaultS3Client().putObject(new PutObjectRequest(
                S3Utils.DEFAULT_BUCKET, keyName, fileToUpload)
                // grant public read
                .withCannedAcl(CannedAccessControlList.PublicRead));
        return url;
    }

    public static AmazonS3 getDefaultS3Client() throws CheckedAWSException {
        return getS3Client (null, null);
    }

    public static AmazonS3 getS3Client(String role, String region) throws CheckedAWSException {
        return S3ClientManager.getClient(role, region);
    }

    public static AmazonS3 getS3Client(OtpServer server) throws CheckedAWSException {
        return S3Utils.getS3Client(server.role, server.getRegion());
    }

    /**
     * Verify that application can write to S3 bucket either through its own credentials or by assuming the provided IAM
     * role. We're following the recommended approach from https://stackoverflow.com/a/17284647/915811, but perhaps
     * there is a way to do this effectively without incurring AWS costs (although writing/deleting an empty file to S3
     * is probably minuscule).
     */
    public static void verifyS3WritePermissions(AmazonS3 client, String s3Bucket) throws IOException {
        String key = UUID.randomUUID().toString();
        client.putObject(s3Bucket, key, File.createTempFile("test", ".zip"));
        client.deleteObject(s3Bucket, key);
    }
}
