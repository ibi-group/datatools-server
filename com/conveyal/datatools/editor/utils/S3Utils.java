package com.conveyal.datatools.editor.utils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.conveyal.datatools.manager.DataManager;
import org.apache.commons.io.IOUtils;
import spark.Request;

import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletException;
import javax.servlet.http.Part;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static spark.Spark.halt;

/**
 * Created by landon on 8/2/16.
 */
public class S3Utils {

    public static String uploadBranding(Request req, String id) throws IOException, ServletException {
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

//        req.attribute("org.eclipse.jetty.multipartConfig", new MultipartConfigElement("/temp"));

        Part part = req.raw().getPart("file");
        String extension = "." + part.getContentType().split("/", 0)[1];
        File tempFile = File.createTempFile(id, extension);
        tempFile.deleteOnExit();

        InputStream inputStream;
        try {
            inputStream = part.getInputStream();
            FileOutputStream out = new FileOutputStream(tempFile);
            IOUtils.copy(inputStream, out);
        } catch (Exception e) {
//            LOG.error("Unable to open input stream from upload");
            halt("Unable to read uploaded file");
        }

        try {
//            LOG.info("Uploading route branding to S3");
            // Upload file to s3
            AWSCredentials creds;

            // default credentials providers, e.g. IAM role
            creds = new DefaultAWSCredentialsProviderChain().getCredentials();

            String keyName = "branding/" + id + extension;
            url = "https://s3.amazonaws.com/" + s3Bucket + "/" + keyName;
            AmazonS3 s3client = new AmazonS3Client(creds);
            s3client.putObject(new PutObjectRequest(
                    s3Bucket, keyName, tempFile)
                    // grant public read
                    .withCannedAcl(CannedAccessControlList.PublicRead));
            return url;
        }
        catch (AmazonServiceException ase) {
            halt("Error uploading feed to S3");
            return null;
        }
    }
}
