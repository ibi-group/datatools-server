package com.conveyal.datatools.common.utils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
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

import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;

/**
 * Created by landon on 8/2/16.
 */
public class AWSUtils {

    private static final Logger LOG = LoggerFactory.getLogger(AWSUtils.class);
    private static final int REQUEST_TIMEOUT_MSEC = 30 * 1000;

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
    public static AWSStaticCredentialsProvider getCredentialsForRole(String role, String sessionName) {
        String roleSessionName = "data-tools-session";
        if (role == null) return null;
        if (sessionName != null) roleSessionName = String.join("-", roleSessionName, sessionName);
        STSAssumeRoleSessionCredentialsProvider sessionProvider = new STSAssumeRoleSessionCredentialsProvider.Builder(
            role,
            roleSessionName
        ).build();
        AWSSessionCredentials credentials = sessionProvider.getCredentials();
        return new AWSStaticCredentialsProvider(new BasicSessionCredentials(
            credentials.getAWSAccessKeyId(), credentials.getAWSSecretKey(),
            credentials.getSessionToken()));
    }

    /**
     * Shorthand method to obtain an EC2 client for the provided role ARN. If role is null, the default EC2 credentials
     * will be used.
     */
    public static AmazonEC2 getEC2ClientForRole (String role, String region) {
        AWSStaticCredentialsProvider credentials = getCredentialsForRole(role, "ec2-client");
        return region == null
            ? getEC2ClientForCredentials(credentials)
            : getEC2ClientForCredentials(credentials, region);
    }

    /**
     * Shorthand method to obtain an EC2 client for the provided credentials. If credentials are null, the default EC2
     * credentials will be used.
     */
    public static AmazonEC2 getEC2ClientForCredentials (AWSCredentialsProvider credentials) {
        return AmazonEC2Client.builder().withCredentials(credentials).build();
    }

    /**
     * Shorthand method to obtain an EC2 client for the provided credentials and region. If credentials are null, the
     * default EC2 credentials will be used.
     */
    public static AmazonEC2 getEC2ClientForCredentials (AWSCredentialsProvider credentials, String region) {
        return AmazonEC2Client.builder().withCredentials(credentials).withRegion(region).build();
    }

    /**
     * Shorthand method to obtain an S3 client for the provided credentials. If credentials are null, the default EC2
     * credentials will be used.
     */
    public static AmazonS3 getS3ClientForCredentials (AWSCredentialsProvider credentials, String region) {
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        if (region != null) builder.withRegion(region);
        return builder.withCredentials(credentials).build();
    }

    /**
     * Shorthand method to obtain an S3 client for the provided role ARN. If role is null, the default EC2 credentials
     * will be used. Similarly, if the region is null, it will be omitted while building the S3 client.
     */
    public static AmazonS3 getS3ClientForRole(String role, String region) {
        AWSStaticCredentialsProvider credentials = getCredentialsForRole(role, "s3-client");
        return getS3ClientForCredentials(credentials, region);
    }

    /** Shorthand method to obtain an S3 client for the provided role ARN. */
    public static AmazonS3 getS3ClientForRole(String role) {
        return getS3ClientForRole(role, null);
    }
}
