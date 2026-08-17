package com.conveyal.datatools.common.utils.aws;

import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.OtpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.utils.Validate;
import spark.Request;
import spark.Response;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;
import static com.conveyal.datatools.manager.DataManager.hasConfigProperty;

/**
 * This class contains utilities related to using AWS S3 services.
 */
public class S3Utils {
    private static final Logger LOG = LoggerFactory.getLogger(S3Utils.class);

    private static final int REQUEST_TIMEOUT_MSEC = 30 * 1000;
    private static final AwsCredentialsProvider DEFAULT_S3_CREDENTIALS;
    private static final S3ClientManagerImpl S3ClientManager;
    private static final S3AsyncClientManagerImpl S3AsyncClientManager;

    public static final String DEFAULT_BUCKET;
    public static final String DEFAULT_BUCKET_GTFS_FOLDER = "gtfs/";

    public static final String APP_DATA_S3_REGION = "application.data.s3_region";

    public static final String APP_DATA_CREDS_FILE = "application.data.s3_credentials_file";

    static {
        // Placeholder variables need to be used before setting the final variable to make sure initialization occurs
        AwsCredentialsProvider tempS3CredentialsProvider = null;
        String tempGtfsS3Bucket = null;
        S3ClientManagerImpl tempS3ClientManager = null;
        S3AsyncClientManagerImpl tempS3AsyncClientManager = null;

        // Only configure s3 if the config requires doing so
        if (DataManager.useS3 || hasConfigProperty("modules.gtfsapi.use_extension")) {
            S3Wrapper build = buildS3Wrapper(List.of(APP_DATA_CREDS_FILE), List.of(APP_DATA_S3_REGION));
            tempS3CredentialsProvider = build.credentials;
            tempS3ClientManager = new S3ClientManagerImpl(build.s3Client);
            tempS3AsyncClientManager = new S3AsyncClientManagerImpl(build.s3AsyncClient);

            // s3 storage
            tempGtfsS3Bucket = DataManager.getConfigPropertyAsText("application.data.gtfs_s3_bucket");
            if (tempGtfsS3Bucket == null) {
                throw new IllegalArgumentException("Required config param `application.data.gtfs_s3_bucket` missing!");
            }
        }

        // initialize final fields
        DEFAULT_S3_CREDENTIALS = tempS3CredentialsProvider;
        S3ClientManager = tempS3ClientManager;
        S3AsyncClientManager = tempS3AsyncClientManager;
        DEFAULT_BUCKET = tempGtfsS3Bucket;
    }

    public static class S3Wrapper {
        public final AwsCredentialsProvider credentials;
        public final S3Client s3Client;
        public final S3AsyncClient s3AsyncClient;
        public final String region;

        public S3Wrapper(AwsCredentialsProvider credentials, S3Client s3Client, S3AsyncClient s3AsyncClient, String region) {
            this.credentials = credentials;
            this.s3Client = s3Client;
            this.s3AsyncClient = s3AsyncClient;
            this.region = region;
        }
    }

    /**
     * Builds an S3 client from the specified credentials and region, or fallback to defaults
     * using the ambient IAM role.
     * @param configCredentials The credentials file to use for the client.
     * @param configRegions The configuration entries in the order to try to get the AWS region to apply.
     * @return An S3 client for the provided credentials and region.
     */
    public static S3Wrapper buildS3Wrapper(
        List<String> configCredentials,
        List<String> configRegions
    ) throws IllegalArgumentException {
        S3Client s3client = null;
        S3AsyncClient s3AsyncClient = null;
        AwsCredentialsProvider credentialsProvider = null;
        String finalRegion = null;
        try {
            S3ClientBuilder builder = S3Client.builder();
            S3AsyncClientBuilder asyncBuilder = S3AsyncClient.builder();
            // Iterate through the credentials and stop at the first non-null returned.
            // Default to the ambient IAM credentials, if any.
            for (String configCred : configCredentials) {
                String credentialsFile = DataManager.getConfigPropertyAsText(configCred);
                if (credentialsFile != null) {
                    try {
                        credentialsProvider = ProfileCredentialsProvider.builder()
                            .profileFile(file -> file
                                .content(Paths.get(credentialsFile))
                                .type(ProfileFile.Type.CONFIGURATION) // Expects all non-default profiles to be prefixed with "profile".
                            )
                            .profileName("default")
                            .build();
                    } catch (IllegalArgumentException e) {
                        LOG.error("Invalid credentials from {}. Trying the next one.", configCred, e);
                    }
                    if (credentialsProvider != null) break;
                }
            }
            if (credentialsProvider == null) {
                // default credentials providers, e.g. IAM role
                credentialsProvider = DefaultCredentialsProvider.builder()
                    .build();
            }
            builder.credentialsProvider(credentialsProvider);
            asyncBuilder.credentialsProvider(credentialsProvider);

            // Iterate through the regions and stop at the first non-null returned.
            // Otherwise defaults to value (typically provided in ~/.aws/config)
            for (String configRegion : configRegions) {
                String region = DataManager.getConfigPropertyAsText(configRegion);
                if (region != null) {
                    builder.region(Region.of(region));
                    asyncBuilder.region(Region.of(region));
                    finalRegion = region;
                    break;
                }
            }

            s3client = builder.build();
            s3AsyncClient = asyncBuilder.build();
        } catch (Exception e) {
            LOG.error(
                "S3 client not initialized correctly. Must provide config property {} or specify region in ~/.aws/config",
                configRegions,
                e
            );
        }

        if (s3client == null) {
            throw new IllegalArgumentException("Fatal error initializing the default s3Client");
        }

        return new S3Wrapper(credentialsProvider, s3client, s3AsyncClient, finalRegion);
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
        return String.format("https://%s.s3.amazonaws.com/%s", DEFAULT_BUCKET, key);
    }

    /**
     * A class that manages the creation of S3 clients.
     */
    private static class S3ClientManagerImpl extends AWSClientManager<S3Client> {
        public S3ClientManagerImpl(S3Client defaultClient) {
            super(defaultClient);
        }

        @Override
        public S3Client buildDefaultClientWithRegion(String region) {
            return S3Client.builder().credentialsProvider(DEFAULT_S3_CREDENTIALS).region(Region.of(region)).build();
        }

        @Override
        public S3Client buildCredentialedClientForRoleAndRegion(
            AwsCredentialsProvider credentials, String region, String role
        ) {
            S3ClientBuilder builder = S3Client.builder();
            if (region != null) builder.region(Region.of(region));
            return builder.credentialsProvider(credentials).build();
        }
    }

    /**
     * A class that manages the creation of S3 clients.
     */
    // TODO: merge these two impl classes
    private static class S3AsyncClientManagerImpl extends AWSClientManager<S3AsyncClient> {
        public S3AsyncClientManagerImpl(S3AsyncClient defaultClient) {
            super(defaultClient);
        }

        @Override
        public S3AsyncClient buildDefaultClientWithRegion(String region) {
            return S3AsyncClient.builder().credentialsProvider(DEFAULT_S3_CREDENTIALS).region(Region.of(region)).build();
        }

        @Override
        public S3AsyncClient buildCredentialedClientForRoleAndRegion(
            AwsCredentialsProvider credentials, String region, String role
        ) {
            S3AsyncClientBuilder builder = S3AsyncClient.builder();
            if (region != null) builder.region(Region.of(region));
            return builder.credentialsProvider(credentials).build();
        }
    }

    /**
     * Attemps to get the head or metadata for an S3 object.
     * @return an {@link HeadObjectResponse} if the requested object exists, null otherwise.
     */
    public static HeadObjectResponse getHeadObject(S3Client s3Client, String bucketName, String key) {
        try {
            Validate.notEmpty(bucketName, "The bucket name must not be null or an empty string.", "");
            Validate.notEmpty(key, "The object key must not be null or an empty string.", "");
            return s3Client.headObject(HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build());
        } catch (NoSuchKeyException e) {
            return null;
        }
    }

    public static boolean objectExists(S3Client s3Client, String bucketName, String key) {
        return getHeadObject(s3Client, bucketName, key) != null;
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
        S3Client s3,
        String bucket,
        String key,
        boolean redirect,
        Request req,
        Response res
    ) {
        if (!objectExists(s3, bucket, key)) {
            logMessageAndHalt(
                req,
                500,
                String.format("Error downloading file from S3. Object s3://%s/%s does not exist.", bucket, key)
            );
            return null;
        }

        Date expiration = new Date();
        expiration.setTime(expiration.getTime() + REQUEST_TIMEOUT_MSEC);

        URL url;
        try (S3Presigner presigner = S3Presigner.create()) {
            GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                .signatureDuration(Duration.ofMillis(REQUEST_TIMEOUT_MSEC))
                .getObjectRequest(objReq -> objReq.bucket(bucket).key(key))
                .build();

            PresignedGetObjectRequest presignedRequest = presigner.presignGetObject(presignRequest);
            url = presignedRequest.url();
        } catch (AwsServiceException e) {
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
    public static String uploadObject(String keyName, File fileToUpload) throws AwsServiceException, CheckedAWSException {
        String url = S3Utils.getDefaultBucketUrlForKey(keyName);
        // FIXME: This may need to change during feed store refactor
        getDefaultS3Client().putObject(PutObjectRequest.builder().bucket(S3Utils.DEFAULT_BUCKET).key(keyName)
            // grant public read
            .acl(ObjectCannedACL.PUBLIC_READ)
            .build(), RequestBody.fromFile(fileToUpload));
        return url;
    }

    public static S3Client getDefaultS3Client() throws CheckedAWSException {
        return getS3Client (null, null);
    }

    public static S3Client getS3Client(String role, String region) throws CheckedAWSException {
        return S3ClientManager.getClient(role, region);
    }

    public static S3Client getS3Client(OtpServer server) throws CheckedAWSException {
        return S3Utils.getS3Client(server.role, server.getRegion());
    }

    public static S3AsyncClient getDefaultS3AsyncClient() throws CheckedAWSException {
        return getS3AsyncClient (null, null);
    }

    public static S3AsyncClient getS3AsyncClient(String role, String region) throws CheckedAWSException {
        return S3AsyncClientManager.getClient(role, region);
    }

    /**
     * Verify that application can write to S3 bucket either through its own credentials or by assuming the provided IAM
     * role. We're following the recommended approach from https://stackoverflow.com/a/17284647/915811, but perhaps
     * there is a way to do this effectively without incurring AWS costs (although writing/deleting an empty file to S3
     * is probably minuscule).
     */
    public static void verifyS3WritePermissions(S3Client client, String s3Bucket) throws IOException {
        String key = UUID.randomUUID().toString();
        client.putObject(PutObjectRequest.builder().bucket(s3Bucket).key(key)
            .build(), RequestBody.fromFile(File.createTempFile("test", ".zip")));
        client.deleteObject(DeleteObjectRequest.builder().bucket(s3Bucket).key(key)
            .build());
    }
}
