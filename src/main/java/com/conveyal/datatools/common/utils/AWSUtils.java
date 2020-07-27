package com.conveyal.datatools.common.utils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
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
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.persistence.FeedStore;
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
 * Created by landon on 8/2/16.
 */
public class AWSUtils {

    private static final Logger LOG = LoggerFactory.getLogger(AWSUtils.class);
    private static final int REQUEST_TIMEOUT_MSEC = 30 * 1000;

    private static final AmazonEC2 DEFAULT_EC2_CLEINT = AmazonEC2Client.builder().build();
    private static final AmazonElasticLoadBalancing DEFAULT_ELB_CLIENT = AmazonElasticLoadBalancingClient
        .builder()
        .build();
    private static final AmazonIdentityManagement DEFAULT_IAM_CLIENT = AmazonIdentityManagementClientBuilder
        .defaultClient();
    private static final AmazonS3 DEFAULT_S3_CLEINT;

    static {
        // Configure the default S3 client
        // A temporary variable needs to be used before setting the final variable
        AmazonS3 tempS3Client = null;
        try {
            // If region configuration string is provided, use that.
            // Otherwise defaults to value provided in ~/.aws/config
            String region = DataManager.getConfigPropertyAsText("application.data.s3_region");
            AmazonS3ClientBuilder S3ClientBuilder = AmazonS3ClientBuilder.standard();
            String S3CredentialsFile = DataManager.getConfigPropertyAsText(
                "application.data.s3_credentials_file"
            );
            S3ClientBuilder.withCredentials(
                S3CredentialsFile != null
                    ? new ProfileCredentialsProvider(S3CredentialsFile, "default")
                    : new DefaultAWSCredentialsProviderChain() // default credentials providers, e.g. IAM role
            );
            tempS3Client = S3ClientBuilder.build();
        } catch (Exception e) {
            LOG.error("S3 client not initialized correctly.  Must provide config property application.data.s3_region or specify region in ~/.aws/config", e);
        }
        DEFAULT_S3_CLEINT = tempS3Client;
        if (DEFAULT_S3_CLEINT == null) {
            throw new IllegalArgumentException("Fatal error initializing the default s3Client");
        }
    }

    private static HashMap<String, CredentialsProviderSession> crendentialsProvidersByRole = new HashMap<>();
    private static HashMap<String, AmazonEC2> nonRoleEC2ClientsByRegion = new HashMap<>();
    private static HashMap<String, AmazonEC2ClientWithRole> EC2ClientsByRoleAndRegion = new HashMap<>();
    private static HashMap<String, AmazonS3> nonRoleS3ClientsByRegion = new HashMap<>();
    private static HashMap<String, AmazonS3ClientWithRole> S3ClientsByRoleAndRegion = new HashMap<>();

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
            AmazonS3 s3client = FeedStore.s3Client;
            s3client.putObject(new PutObjectRequest(
                    s3Bucket, keyName, tempFile)
                    // grant public read
                    .withCannedAcl(CannedAccessControlList.PublicRead));
            return url;
        } catch (AmazonServiceException ase) {
            logMessageAndHalt(req, 500, "Error uploading file to S3", ase);
            return null;
        } finally {
            boolean deleted = tempFile.delete();
            if (!deleted) {
                LOG.error("Could not delete s3 upload file.");
            }
        }
    }

    /**
     * Download an object in the selected format from S3, using presigned URLs.
     * @param s3
     * @param bucket name of the bucket
     * @param filename both the key and the format
     * @param redirect
     * @param res
     * @return
     */
    public static String downloadFromS3(AmazonS3 s3, String bucket, String filename, boolean redirect, Response res){
        Date expiration = new Date();
        expiration.setTime(expiration.getTime() + REQUEST_TIMEOUT_MSEC);

        GeneratePresignedUrlRequest presigned = new GeneratePresignedUrlRequest(bucket, filename);
        presigned.setExpiration(expiration);
        presigned.setMethod(HttpMethod.GET);
        URL url = s3.generatePresignedUrl(presigned);

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
    private static AWSStaticCredentialsProvider getCredentialsForRole(String role) throws NonRuntimeAWSException {
        String roleSessionName = "data-tools-session";
        if (role == null) return null;
        // check if an active credentials provider exists for this role
        CredentialsProviderSession session = crendentialsProvidersByRole.get(role);
        if (session != null && session.isActive()) return session.credentialsProvider;
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
            throw new NonRuntimeAWSException("Failed to obtain AWS credentials");
        }
        AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(
            new BasicSessionCredentials(
                credentials.getAWSAccessKeyId(),
                credentials.getAWSSecretKey(),
                credentials.getSessionToken()
            )
        );
        // store the credentials provider in a lookup by role for future use
        crendentialsProvidersByRole.put(role, new CredentialsProviderSession(credentialsProvider));
        return credentialsProvider;
    }

    /**
     * Obtain a potentially cached EC2 client for the provided role ARN and region. If the role and region are null, the
     * default EC2 credentials will be used. If just the role is null a cached client configured for the specified
     * region will be returned. For clients that require using a role, a client will be obtained (either via a cache or
     * by creation and then insertion into the cache) that has obtained the proper credentials.
     */
    public static AmazonEC2 getEC2Client (String role, String region) throws NonRuntimeAWSException {
        // return default client for null region and role
        if (role == null && region == null) {
            return DEFAULT_EC2_CLEINT;
        }

        // if the role is null, return a potentially cached EC2 client with the region configured
        AmazonEC2 EC2Client;
        if (role == null) {
            EC2Client = nonRoleEC2ClientsByRegion.get(region);
            if (EC2Client == null) {
                EC2Client = AmazonEC2Client.builder().withRegion(region).build();
                nonRoleEC2ClientsByRegion.put(region, EC2Client);
            }
            return EC2Client;
        }

        // check for the availability of a EC2 client already associated with the given role and region
        String roleRegionKey = makeRoleRegionKey(role, region);
        AmazonEC2ClientWithRole EC2ClientWithRole = EC2ClientsByRoleAndRegion.get(roleRegionKey);
        if (EC2ClientWithRole != null && EC2ClientWithRole.isActive()) return EC2ClientWithRole.EC2Client;

        // Either a new client hasn't been created or it has expired. Create a new client and cache it.
        AWSStaticCredentialsProvider credentials = getCredentialsForRole(role);
        AmazonEC2ClientBuilder builder = AmazonEC2Client.builder().withCredentials(credentials);
        if (region != null) {
            builder = builder.withRegion(region);
        }
        EC2Client = builder.build();
        EC2ClientsByRoleAndRegion.put(roleRegionKey, new AmazonEC2ClientWithRole(EC2Client));
        return EC2Client;
    }

    public static AmazonS3 getS3Client () throws NonRuntimeAWSException {
        return getS3Client (null, null);
    }

    /**
     * Obtain an S3 client for the provided role ARN. If role is null, the default EC2 credentials
     * will be used. Similarly, if the region is null, it will be omitted while building the S3 client.
     */
    public static AmazonS3 getS3Client(String role, String region) throws NonRuntimeAWSException {
        // return default client for null region and role
        if (role == null && region == null) {
            return DEFAULT_S3_CLEINT;
        }

        // if the role is null, return a potentially cached S3 client with the region configured
        AmazonS3 S3Client;
        if (role == null) {
            S3Client = nonRoleS3ClientsByRegion.get(region);
            if (S3Client == null) {
                S3Client = AmazonS3ClientBuilder.standard().withRegion(region).build();
                nonRoleS3ClientsByRegion.put(region, S3Client);
            }
            return S3Client;
        }

        // check for the availability of a S3 client already associated with the given role and region
        String roleRegionKey = makeRoleRegionKey(role, region);
        AmazonS3ClientWithRole S3ClientWithRole = S3ClientsByRoleAndRegion.get(roleRegionKey);
        if (S3ClientWithRole != null && S3ClientWithRole.isActive()) return S3ClientWithRole.S3Client;

        // Either a new client hasn't been created or it has expired. Create a new client and cache it.
        AWSStaticCredentialsProvider credentials = getCredentialsForRole(role);
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        if (region != null) builder.withRegion(region);
        S3Client = builder.withCredentials(credentials).build();
        S3ClientsByRoleAndRegion.put(roleRegionKey, new AmazonS3ClientWithRole(S3Client));
        return S3Client;
    }

    /**
     * A helper exception class that does not extend the RunTimeException class in order to make the compiler properly
     * detect possible places where an exception could occur.
     */
    public static class NonRuntimeAWSException extends Throwable {
        public NonRuntimeAWSException(String message) {
            super(message);
        }
    }

    /**
     * A util class for storing items that will eventually expire
     */
    private static class ExpiringAsset {
        private final long expirationTime;

        private ExpiringAsset() {
            // set the expiration time to be in 800 seconds. The default credentials provider session length is 900
            // seconds, but we want to be sure to refresh well before the expiration in order to make sure no subsequent
            // requests use an expired session.
            this.expirationTime = System.currentTimeMillis() + 800 * 1000;
        }

        /**
         * @return true if the asset hasn't yet expired
         */
        public boolean isActive() {
            return expirationTime > System.currentTimeMillis();
        }
    }

    /**
     * A class that contains credentials provider sessions
     */
    private static class CredentialsProviderSession extends ExpiringAsset {
        private final AWSStaticCredentialsProvider credentialsProvider;

        private CredentialsProviderSession(AWSStaticCredentialsProvider credentialsProvider) {
            super();
            this.credentialsProvider = credentialsProvider;
        }
    }

    /**
     * A class that contains an AmazonEC2 client that will eventually expire
     */
    private static class AmazonEC2ClientWithRole extends ExpiringAsset {
        private final AmazonEC2 EC2Client;

        private AmazonEC2ClientWithRole(AmazonEC2 EC2Client) {
            super();
            this.EC2Client = EC2Client;
        }
    }

    /**
     * A class that contains an AmazonS3 client that will eventually expire
     */
    private static class AmazonS3ClientWithRole extends ExpiringAsset {
        private final AmazonS3 S3Client;

        private AmazonS3ClientWithRole(AmazonS3 S3Client) {
            super();
            this.S3Client = S3Client;
        }
    }
}
