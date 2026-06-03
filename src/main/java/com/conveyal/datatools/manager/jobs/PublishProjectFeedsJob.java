package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.common.utils.aws.S3Utils;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.Project;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;

import static com.conveyal.datatools.manager.DataManager.getConfigPropertyAsText;
import static com.conveyal.datatools.manager.DataManager.hasConfigProperty;

/**
 * Publish the latest GTFS files for all public feeds in a project.
 */
public class PublishProjectFeedsJob extends MonitorableJob {
    public static final Logger LOG = LoggerFactory.getLogger(PublishProjectFeedsJob.class);

    private Project project;
    private static final String dataSupportEmail = getConfigPropertyAsText("application.public_gtfs_contact_email");

    public PublishProjectFeedsJob(Project project, Auth0UserProfile owner) {
        super(owner, "Generating public html for " + project.name, JobType.MAKE_PROJECT_PUBLIC);
        this.project = project;
        status.update("Waiting to publish feeds...", 0);
    }

    @JsonProperty
    public String getProjectId () {
        return project.id;
    }

    @Override
    public void jobLogic () {
        status.update("Preparing HTML for public feeds page", 10);
        String output;
        String title = "Public Feeds";
        StringBuilder r = new StringBuilder();
        r.append("<!DOCTYPE html>\n");
        r.append("<html>\n");
        r.append("<head>\n");
        r.append("<title>" + title + "</title>\n");
        r.append("<style type=\"text/css\">\n" +
                "        body { font-family: arial,helvetica,clean,sans-serif; font-size: 12px }\n" +
                "        h1 { font-size: 18px }\n" +
                "    </style>");

        r.append("</head>\n");
        r.append("<body>\n");
        r.append("<h1>" + title + "</h1>\n");
        r.append("The following feeds, in GTFS format, are available for download and use.\n");
        if (dataSupportEmail != null) {
            r.append(String.format("If you have inquiries, please contact us at: <a href=\"mailto:%1$s\">%1$s</a>", dataSupportEmail));
        }
        r.append("<ul>\n");
        status.update("Ensuring public GTFS files are up-to-date.", 50);
        project.retrieveProjectFeedSources().stream()
            .filter(fs -> fs.isPublic)
            // Store latest version id, so we don't have to fetch it again in the forEach stage.
            .map(fs -> new Pair<FeedSource, String>(fs, fs.latestVersionId()) {})
            .filter(p -> p.getValue() != null)
            .forEach(p -> {
                // generate list item for feed source
                FeedSource fs = p.getKey();
                String latestVersionId = p.getValue();
                String url;
                if (fs.url != null && !hasConfigProperty("modules.enterprise.prefer_s3_links")) {
                    url = fs.url.toString();
                }
                else {
                    // ensure latest feed is written to the s3 public folder
                    try {
                        fs.makePublic(latestVersionId);
                    } catch (Exception e) {
                        status.fail("Failed to make GTFS files public on S3", e);
                        return;
                    }
                    url = S3Utils.getDefaultBucketUrlForKey(fs.toPublicKey());
                }
                r.append("<li>");
                r.append("<a href=\"" + url + "\">");
                r.append(fs.name);
                r.append("</a>");
                r.append(" (");
                if (fs.url != null && fs.lastFetched != null) {
                    r.append("last checked: " + new SimpleDateFormat("dd MMM yyyy").format(fs.lastFetched) + ", ");
                }
                if (fs.lastUpdated() != null) {
                    r.append("last updated: " + new SimpleDateFormat("dd MMM yyyy").format(fs.lastUpdated()) + ")");
                }
                r.append("</li>");
            });
        r.append("</ul>");
        r.append("</body>");
        r.append("</html>");
        output = r.toString();
        String fileName = "index.html";
        String folder = "public/";
        status.update("Updating GTFS directory...", 90);
        File file = new File(String.join("/", FileUtils.getTempDirectory().getAbsolutePath(), fileName));
        file.deleteOnExit();
        try {
            FileUtils.writeStringToFile(file, output);
        } catch (IOException e) {
            LOG.error("Failed to write string to file", e);
            e.printStackTrace();
        }
        try {
            S3Utils.uploadObject(folder + fileName, file);
        } catch (Exception e) {
            status.fail("Failed to perform S3 actions", e);
        }
    }

    @Override
    public void jobFinished() {
        if (!status.error) {
            status.completeSuccessfully("Public page updated successfully!");
        }
    }
}
