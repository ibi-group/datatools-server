package com.conveyal.datatools.manager.jobs;

import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.FeedStore;
import org.apache.commons.io.FileUtils;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * Created by landon on 1/31/17.
 */
public class MakePublicJob extends MonitorableJob {
    public Project project;
    public Status status;
    private static final Logger LOG = LoggerFactory.getLogger(MakePublicJob.class);

    public MakePublicJob(Project project, String owner) {
        super(owner, "Generating public html for " + project.name, JobType.MAKE_PROJECT_PUBLIC);
        this.project = project;
        status = new Status();
        status.message = "Waiting to begin validation...";
        status.percentComplete = 0;
    }
    @Override
    public Status getStatus() {
        return null;
    }

    @Override
    public void handleStatusEvent(Map statusMap) {

    }

    @Override
    public void run() {
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
        r.append("<ul>\n");
        project.getProjectFeedSources().stream()
                .filter(fs -> fs.isPublic && fs.getLatest() != null)
                .forEach(fs -> {
                    String url = fs.url != null ? fs.url.toString() : "https://s3.amazonaws.com/" + DataManager.feedBucket + "/public/" + fs.name +".zip";
                    FeedVersion latest = fs.getLatest();
                    r.append("<li>");
                    r.append("<a href=\"" + url + "\">");
                    r.append(fs.name);
                    r.append("</a>");
                    r.append(" (");
                    if (fs.url != null && fs.lastFetched != null) {
                        r.append("last checked: " + new SimpleDateFormat("dd MMM yyyy").format(fs.lastFetched) + ", ");
                    }
                    if (fs.getLastUpdated() != null) {
                        r.append("last updated: " + new SimpleDateFormat("dd MMM yyyy").format(fs.getLastUpdated()) + ")");
                    }
                    r.append("</li>");
        });
        r.append("</ul>");
        r.append("</body>");
        r.append("</html>");
        output = r.toString();
        String fileName = "index.html";
        String folder = "public/";
        File file = new File(FileUtils.getTempDirectory() + fileName);
        file.deleteOnExit();
        try {
            FileUtils.writeStringToFile(file, output);
        } catch (IOException e) {
            e.printStackTrace();
        }

        FeedStore.s3Client.putObject(DataManager.feedBucket, folder + fileName, file);
        FeedStore.s3Client.setObjectAcl(DataManager.feedBucket, folder + fileName, CannedAccessControlList.PublicRead);

        jobFinished();
    }
}
