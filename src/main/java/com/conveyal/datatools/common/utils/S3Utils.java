package com.conveyal.datatools.common.utils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.policy.Action;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
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

import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletException;
import javax.servlet.http.Part;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import static com.conveyal.datatools.common.utils.SparkUtils.haltWithMessage;
import static spark.Spark.halt;

/**
 * Created by landon on 8/2/16.
 */
public class S3Utils {

    private static final Logger LOG = LoggerFactory.getLogger(S3Utils.class);

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
     * Create temporary S3 credentials in order to grant access to some set of objects (s3://bucket/key)
     * using a predefined role.  More info here: http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/prog-services-sts.html#retrieving-an-sts-token
     * @param role predefined AWS role that must be used as the baseline for allowable actions, effects, and resources
     * @param bucket S3 bucket name
     * @param key S3 key to grant access to
     * @param effect policy statement effect (for example, GetObject)
     * @param action action allowed by temporary credentials (must intersect with the policies already defined by role)
     * @param durationSeconds duration in seconds that the credentials are valid for (900 is minumum)
     * @return
     */
    public static Credentials getS3Credentials(String role, String bucket, String key, Statement.Effect effect, S3Actions action, int durationSeconds) {
        AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClientBuilder.defaultClient();
        Policy policy = new Policy();
        policy.setId("datatools-feed-access");
        Statement statement = new Statement(effect);
        Set<Action> actions = new HashSet<>();
        actions.add(action);
        statement.setActions(actions);
        Set<Resource> resources = new HashSet<>();
        resources.add(new Resource("arn:aws:s3:::" + bucket + "/" + key));
        statement.setResources(resources);
        Set<Statement> statements = new HashSet<>();
        statements.add(statement);
        policy.setStatements(statements);
        AssumeRoleRequest assumeRequest = new AssumeRoleRequest()
                .withRoleArn(role)
                .withPolicy(policy.toJson()) // some policy that limits access to certain objects (intersects with ROLE_ARN policies
                .withDurationSeconds(durationSeconds) // 900 is minimum duration (seconds)
                .withRoleSessionName("feed-access");
        AssumeRoleResult assumeResult =
                stsClient.assumeRole(assumeRequest);
        return assumeResult.getCredentials();
    }
}
