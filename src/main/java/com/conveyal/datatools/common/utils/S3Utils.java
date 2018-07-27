package com.conveyal.datatools.common.utils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.auth.policy.Action;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
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
import java.util.HashSet;
import java.util.Set;

import static com.conveyal.datatools.common.utils.SparkUtils.haltWithMessage;
import static spark.Spark.halt;

/**
 * Created by landon on 8/2/16.
 */
public class S3Utils {

    private static final Logger LOG = LoggerFactory.getLogger(S3Utils.class);
    private static final int REQUEST_TIMEOUT_MSEC = 30 * 1000;

    public static String uploadBranding(Request req, String key) throws IOException, ServletException {
        String url;

        String s3Bucket = DataManager.getConfigPropertyAsText("application.data.gtfs_s3_bucket");
        if (s3Bucket == null) {
            halt(400);
        }

        // Get file from request
        if (req.raw().getAttribute("org.eclipse.jetty.multipartConfig") == null) {
            MultipartConfigElement multipartConfigElement = new MultipartConfigElement(System.getProperty("java.io.tmpdir"));
            req.raw().setAttribute("org.eclipse.jetty.multipartConfig", multipartConfigElement);
        }
        Part part = req.raw().getPart("file");
        String extension = "." + part.getContentType().split("/", 0)[1];
        File tempFile = File.createTempFile(key + "_branding", extension);
        InputStream inputStream;
        try {
            inputStream = part.getInputStream();
            FileOutputStream out = new FileOutputStream(tempFile);
            IOUtils.copy(inputStream, out);
        } catch (Exception e) {
            e.printStackTrace();
            haltWithMessage(400, "Unable to read uploaded file");
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
            ase.printStackTrace();
            haltWithMessage(400, "Error uploading file to S3");
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
}
