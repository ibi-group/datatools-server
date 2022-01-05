package com.conveyal.datatools.manager.jobs;

import com.amazonaws.services.s3.AmazonS3URI;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.common.utils.aws.S3Utils;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.HttpUtils;
import com.conveyal.datatools.manager.utils.SimpleHttpResponse;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.in;

public class PeliasUpdateJob extends MonitorableJob {
    /**
     * The deployment to send to Pelias
     */
    private final Deployment deployment;

    /**
     * The workerId our request has on the webhook server. Used to get status updates
     */
    private String workerId;


    /**
     * Timer used to poll the status endpoint
     */
    private final Timer timer;

    /**
     * The number of webhook status requests allowed to fail before considering the server down
     */
    private int webhookStatusFailuresAllowed = 3;

    /**
     * S3 URI to upload logs to
     */
    private final AmazonS3URI logUploadS3URI;

    public PeliasUpdateJob(Auth0UserProfile owner, String name, Deployment deployment, AmazonS3URI logUploadS3URI) {
        super(owner, name, JobType.UPDATE_PELIAS);
        this.deployment = deployment;
        this.timer = new Timer();
        this.logUploadS3URI = logUploadS3URI;
    }
    public PeliasUpdateJob(Auth0UserProfile owner, String name, Deployment deployment) {
        super(owner, name, JobType.UPDATE_PELIAS);
        this.deployment = deployment;
        this.timer = new Timer();

        if (deployment.deployJobSummaries.size() <= 0) {
            throw new RuntimeException("Deployment must be deployed to at least one server to update Pelias!");
        }

        // Get log upload URI from deployment (the latest build artifacts folder is where the logs get uploaded to)
        this.logUploadS3URI = new AmazonS3URI(deployment.deployJobSummaries.get(deployment.deployJobSummaries.size() - 1).buildArtifactsFolder);
    }

    /**
     * This method must be overridden by subclasses to perform the core steps of the job.
     */
    @Override
    public void jobLogic() throws Exception {
        status.message = "Launching Local Places Index update request";
        workerId = this.makeWebhookRequest();
        status.percentComplete = 1.0;

        // Give server 1 second to create worker
        Thread.sleep(1000);
        // Check status every 2 seconds
        timer.schedule(new StatusChecker(), 0, 2000);

    }

    private void getWebhookStatus() {
        URI url = getWebhookURI(deployment.parentProject().peliasWebhookUrl + "/status/" + workerId);

        // Convert raw body to JSON
        PeliasWebhookStatusMessage statusResponse;

        try {
            SimpleHttpResponse response = HttpUtils.httpRequestRawResponse(url, 1000, HttpUtils.REQUEST_METHOD.GET, null);
            // Convert raw body to PeliasWebhookStatusMessage
            String jsonResponse = response.body;
            statusResponse = JsonUtil.objectMapper.readValue(jsonResponse, PeliasWebhookStatusMessage.class);
        } catch (Exception ex) {
            // Allow a set number of failed requests before showing the user failure message
            if (--webhookStatusFailuresAllowed == 0) {
                status.fail("Webhook status did not provide a valid response!", ex);
                timer.cancel();
            }
            return;
        }

        if (!statusResponse.error.equals("false")) {
            status.fail(statusResponse.error);
            timer.cancel();
            return;
        }

        if (statusResponse.completed) {
            status.completeSuccessfully(statusResponse.message);
            timer.cancel();
            return;
        }

        status.message = statusResponse.message;
        status.percentComplete = statusResponse.percentComplete;
        status.completed = false;
    }

    /**
     * Make a request to Pelias update webhook
     *
     * @return The workerID of the run created on the Pelias server
     */
    private String makeWebhookRequest() {
        URI url = getWebhookURI(deployment.parentProject().peliasWebhookUrl);

        // Convert from feedVersionIds to Pelias Config objects
        List<PeliasWebhookGTFSFeedFormat> gtfsFeeds = Persistence.feedVersions.getFiltered(in("_id", deployment.feedVersionIds))
                .stream()
                .map(PeliasWebhookGTFSFeedFormat::new)
                .collect(Collectors.toList());

        PeliasWebhookRequestBody peliasWebhookRequestBody = new PeliasWebhookRequestBody();
        peliasWebhookRequestBody.gtfsFeeds = gtfsFeeds;
        peliasWebhookRequestBody.csvFiles = deployment.peliasCsvFiles;
        peliasWebhookRequestBody.logUploadUrl = logUploadS3URI.toString();
        peliasWebhookRequestBody.deploymentId = deployment.id;
        peliasWebhookRequestBody.resetDb = deployment.peliasResetDb;

        String query = JsonUtil.toJson(peliasWebhookRequestBody);

        // Create headers needed for Pelias webhook
        List<Header> headers = new ArrayList<>();
        headers.add(new BasicHeader("Accept", "application/json"));
        headers.add(new BasicHeader("Content-type", "application/json"));

        // Get webhook response
        SimpleHttpResponse response = HttpUtils.httpRequestRawResponse(url, 5000, HttpUtils.REQUEST_METHOD.POST, query, headers);

        // Convert raw body to JSON
        String jsonResponse;
        try {
            jsonResponse = response.body;
        }
        catch (NullPointerException ex) {
            status.fail("Webhook server specified did not provide a response!", ex);
            return null;
        }

        // Parse JSON
        JsonNode webhookResponse;
        try {
            webhookResponse = JsonUtil.objectMapper.readTree(jsonResponse);
        } catch (IOException ex) {
            status.fail("The Webhook server's response was invalid! Is the server URL correct?", ex);
            return null;
        }

        if (webhookResponse.get("error") != null) {
            status.fail("Server returned an error: " + webhookResponse.get("error").asText());
            return null;
        }

        return webhookResponse.get("workerId").asText();
    }

    /**
     * Helper function to convert Deployment webhook URL to URI object
     * @param webhookUrlString  String containing URL to parse
     * @return                  URI object with webhook URL
     */
    private URI getWebhookURI(String webhookUrlString) {
        URI url;
        try {
            url = new URI(webhookUrlString);
        } catch (URISyntaxException ex) {
            status.fail("Webhook URL was not a valid URL", ex);
            return null;
        }

        return url;
    }

    /**
     * Class used to execute the status update
     */
    class StatusChecker extends TimerTask {
        public void run() {
            getWebhookStatus();
        }
    }

    /**
     * The request body required by the Pelias webhook
     */
    private static class PeliasWebhookRequestBody {
        public List<PeliasWebhookGTFSFeedFormat> gtfsFeeds;
        public List<String> csvFiles;
        public String logUploadUrl;
        public String deploymentId;
        public boolean resetDb;
    }

    /**
     * The GTFS feed info format the Pelias webhook requires
     */
    private static class PeliasWebhookGTFSFeedFormat {
        public String uri;
        public String name;
        public String filename;

        public PeliasWebhookGTFSFeedFormat(FeedVersion feedVersion) {
            uri = S3Utils.getS3FeedUri(feedVersion.id);
            name = Persistence.feedSources.getById(feedVersion.feedSourceId).name;
            filename = feedVersion.id;
        }
    }

    /**
     * The status object returned by the webhook/status endpoint
     */
    public static class PeliasWebhookStatusMessage {
        public Boolean completed;
        public String error;
        public String message;
        public Double percentComplete;
    }
}