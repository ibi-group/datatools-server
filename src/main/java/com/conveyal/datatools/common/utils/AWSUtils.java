package com.conveyal.datatools.common.utils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancing;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingClientBuilder;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.OtpServer;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletException;
import javax.servlet.http.Part;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;

import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;

/**
 * This class mainly has utility functions for obtaining a new AWS client for a various AWS service. This class will
 * create new clients only when they are needed and will refresh the clients in case they have expired. It is expected
 * that all AWS clients are obtained from this class and not created elsewhere in order to properly manage AWS clients
 * and to avoid repetition of code that creates clients.
 */
public class AWSUtils {

    private static final Logger LOG = LoggerFactory.getLogger(AWSUtils.class);
    private static final int REQUEST_TIMEOUT_MSEC = 30 * 1000;
    public static final long DEFAULT_EXPIRING_AWS_ASSET_VALID_DURATION_MILLIS = 800 * 1000;

    private static final AmazonEC2 DEFAULT_EC2_CLIENT = AmazonEC2Client.builder().build();
    private static final AmazonElasticLoadBalancing DEFAULT_ELB_CLIENT = AmazonElasticLoadBalancingClient
        .builder()
        .build();
    private static final AmazonIdentityManagement DEFAULT_IAM_CLIENT = AmazonIdentityManagementClientBuilder
        .defaultClient();
    private static final AWSCredentialsProvider DEFAULT_S3_CREDENTIALS;
    private static final AmazonS3 DEFAULT_S3_CLIENT;

    static {
        // Configure the default S3 client
        // A temporary variable needs to be used before setting the final variable
        AmazonS3 tempS3Client = null;
        AWSCredentialsProvider tempS3CredentialsProvider = null;
        try {
            AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
            String credentialsFile = DataManager.getConfigPropertyAsText(
                "application.data.s3_credentials_file"
            );
            tempS3CredentialsProvider = credentialsFile != null
                ? new ProfileCredentialsProvider(credentialsFile, "default")
                : new DefaultAWSCredentialsProviderChain(); // default credentials providers, e.g. IAM role
            builder.withCredentials(tempS3CredentialsProvider);

            // If region configuration string is provided, use that.
            // Otherwise defaults to value provided in ~/.aws/config
            String region = DataManager.getConfigPropertyAsText("application.data.s3_region");
            if (region != null) {
                builder.withRegion(region);
            }
            tempS3Client = builder.build();
        } catch (Exception e) {
            LOG.error("S3 client not initialized correctly.  Must provide config property application.data.s3_region or specify region in ~/.aws/config", e);
        }
        DEFAULT_S3_CREDENTIALS = tempS3CredentialsProvider;
        DEFAULT_S3_CLIENT = tempS3Client;
        if (DEFAULT_S3_CLIENT == null || DEFAULT_S3_CREDENTIALS == null) {
            throw new IllegalArgumentException("Fatal error initializing the default s3Client");
        }
    }

    private static HashMap<String, ExpiringAsset<AWSStaticCredentialsProvider>> crendentialsProvidersByRole =
        new HashMap<>();
    private static EC2ClientManagerImpl EC2ClientManager = new EC2ClientManagerImpl(DEFAULT_EC2_CLIENT);
    private static ELBClientManagerImpl ELBClientManager = new ELBClientManagerImpl(DEFAULT_ELB_CLIENT);
    private static IAMClientManagerImpl IAMClientManager = new IAMClientManagerImpl(DEFAULT_IAM_CLIENT);
    private static S3ClientManagerImpl S3ClientManager = new S3ClientManagerImpl(DEFAULT_S3_CLIENT);

    public static String uploadBranding(Request req, String key) {
        String url;

        String s3Bucket = DataManager.getConfigPropertyAsText("application.data.gtfs_s3_bucket");
        if (s3Bucket == null) {
            logMessageAndHalt(
                req,
                500,
                "s3bucket is incorrectly configured on server",
                new Exception("s3bucket is incorrectly configured on server")
            );
        }

        // Get file from request
        if (req.raw().getAttribute("org.eclipse.jetty.multipartConfig") == null) {
            MultipartConfigElement multipartConfigElement = new MultipartConfigElement(System.getProperty("java.io.tmpdir"));
            req.raw().setAttribute("org.eclipse.jetty.multipartConfig", multipartConfigElement);
        }
        String extension = null;
        File tempFile = null;
        try {
            Part part = req.raw().getPart("file");
            extension = "." + part.getContentType().split("/", 0)[1];
            tempFile = File.createTempFile(key + "_branding", extension);
            InputStream inputStream;
            inputStream = part.getInputStream();
            FileOutputStream out = new FileOutputStream(tempFile);
            IOUtils.copy(inputStream, out);
        } catch (IOException | ServletException e) {
            e.printStackTrace();
            logMessageAndHalt(req, 400, "Unable to read uploaded file");
        }

        try {
            String keyName = "branding/" + key + extension;
            url = "https://s3.amazonaws.com/" + s3Bucket + "/" + keyName;
            // FIXME: This may need to change during feed store refactor
            getDefaultS3Client().putObject(new PutObjectRequest(
                    s3Bucket, keyName, tempFile)
                    // grant public read
                    .withCannedAcl(CannedAccessControlList.PublicRead));
            return url;
        } catch (AmazonServiceException | CheckedAWSException e) {
            logMessageAndHalt(req, 500, "Error uploading file to S3", e);
            return null;
        } finally {
            boolean deleted = tempFile.delete();
            if (!deleted) {
                LOG.error("Could not delete s3 upload file.");
            }
        }
    }

    /**
     * Helper for downloading a file using the default S3 client.
     */
    public static String downloadFromS3(String bucket, String key, boolean redirect, Request req, Response res) {
        try {
            return downloadFromS3(getDefaultS3Client(), bucket, key, redirect, req, res);
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
    public static String downloadFromS3(
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
     * Create credentials for a new session for the provided IAM role and session name. The primary AWS account for the
     * Data Tools application must be able to assume this role (e.g., through delegating access via an account IAM role
     * https://docs.aws.amazon.com/IAM/latest/UserGuide/tutorial_cross-account-with-roles.html). The credentials can be
     * then used for creating a temporary S3 or EC2 client.
     */
    private static AWSStaticCredentialsProvider getCredentialsForRole(String role) throws CheckedAWSException {
        String roleSessionName = "data-tools-session";
        if (role == null) return null;
        // check if an active credentials provider exists for this role
        ExpiringAsset<AWSStaticCredentialsProvider> session = crendentialsProvidersByRole.get(role);
        if (session != null && session.isActive()) return session.asset;
        // either a session hasn't been created or an existing one has expired. Create a new session.
        STSAssumeRoleSessionCredentialsProvider sessionProvider = new STSAssumeRoleSessionCredentialsProvider
            .Builder(
                role,
                roleSessionName
            )
            .build();
        AWSSessionCredentials credentials;
        try {
            credentials = sessionProvider.getCredentials();
        } catch (AmazonServiceException e) {
            throw new CheckedAWSException("Failed to obtain AWS credentials");
        }
        AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(
            new BasicSessionCredentials(
                credentials.getAWSAccessKeyId(),
                credentials.getAWSSecretKey(),
                credentials.getSessionToken()
            )
        );
        // store the credentials provider in a lookup by role for future use
        crendentialsProvidersByRole.put(
            role,
            new ExpiringAsset<>(credentialsProvider, DEFAULT_EXPIRING_AWS_ASSET_VALID_DURATION_MILLIS)
        );
        return credentialsProvider;
    }

    public static AmazonEC2 getEC2Client(String role, String region) throws CheckedAWSException {
        return EC2ClientManager.getClient(role, region);
    }

    public static AmazonEC2 getEC2Client(OtpServer server) throws CheckedAWSException {
        return getEC2Client(
            server.role,
            server.ec2Info == null ? null : server.ec2Info.region
        );
    }

    public static AmazonElasticLoadBalancing getELBClient(String role, String region) throws CheckedAWSException {
        return ELBClientManager.getClient(role, region);
    }

    public static AmazonIdentityManagement getIAMClient(String role, String region) throws CheckedAWSException {
        return IAMClientManager.getClient(role, region);
    }

    public static AmazonS3 getDefaultS3Client() throws CheckedAWSException {
        return getS3Client (null, null);
    }

    public static AmazonS3 getS3Client(String role, String region) throws CheckedAWSException {
        return S3ClientManager.getClient(role, region);
    }

    /**
     * A class that manages the creation of EC2 clients.
     */
    private static class EC2ClientManagerImpl extends AWSClientManager<AmazonEC2> {
        public EC2ClientManagerImpl(AmazonEC2 defaultClient) {
            super(defaultClient);
        }

        @Override
        public AmazonEC2 buildDefaultClientWithRegion(String region) {
            return AmazonEC2Client.builder().withRegion(region).build();
        }

        @Override
        public AmazonEC2 buildCredentialedClientForRoleAndRegion(
            String role,
            String region
        ) throws CheckedAWSException {
            AWSStaticCredentialsProvider credentials = getCredentialsForRole(role);
            AmazonEC2ClientBuilder builder = AmazonEC2Client.builder().withCredentials(credentials);
            if (region != null) {
                builder = builder.withRegion(region);
            }
            return builder.build();
        }
    }

    /**
     * A class that manages the creation of ELB clients.
     */
    private static class ELBClientManagerImpl extends AWSClientManager<AmazonElasticLoadBalancing> {
        public ELBClientManagerImpl(AmazonElasticLoadBalancing defaultClient) {
            super(defaultClient);
        }

        @Override
        public AmazonElasticLoadBalancing buildDefaultClientWithRegion(String region) {
            return AmazonElasticLoadBalancingClient.builder().withRegion(region).build();
        }

        @Override
        public AmazonElasticLoadBalancing buildCredentialedClientForRoleAndRegion(
            String role,
            String region
        ) throws CheckedAWSException {
            AWSStaticCredentialsProvider credentials = getCredentialsForRole(role);
            AmazonElasticLoadBalancingClientBuilder builder = AmazonElasticLoadBalancingClient
                .builder()
                .withCredentials(credentials);
            if (region != null) {
                builder = builder.withRegion(region);
            }
            return builder.build();
        }
    }

    /**
     * A class that manages the creation of IAM clients.
     */
    private static class IAMClientManagerImpl extends AWSClientManager<AmazonIdentityManagement> {
        public IAMClientManagerImpl(AmazonIdentityManagement defaultClient) {
            super(defaultClient);
        }

        @Override
        public AmazonIdentityManagement buildDefaultClientWithRegion(String region) {
            return defaultClient;
        }

        @Override
        public AmazonIdentityManagement buildCredentialedClientForRoleAndRegion(String role, String region)
            throws CheckedAWSException {
            AWSStaticCredentialsProvider credentials = getCredentialsForRole(role);
            return AmazonIdentityManagementClientBuilder.standard().withCredentials(credentials).build();
        }
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
            String role,
            String region
        ) throws CheckedAWSException {
            AWSStaticCredentialsProvider credentials = getCredentialsForRole(role);
            AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
            if (region != null) builder.withRegion(region);
            return builder.withCredentials(credentials).build();
        }
    }
}
