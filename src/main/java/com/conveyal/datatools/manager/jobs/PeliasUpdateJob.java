package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.common.utils.aws.S3Utils;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.in;

public class PeliasUpdateJob extends MonitorableJob {
    /**
     * The deployment to send to Pelias
     */
    private Deployment deployment;

    public PeliasUpdateJob(Auth0UserProfile owner, String name, Deployment deployment) {
        super(owner, name, JobType.UPDATE_PELIAS);
        this.deployment = deployment;
    }

    /**
     * This method must be overridden by subclasses to perform the core steps of the job.
     */
    @Override
    public void jobLogic() throws Exception {
        status.update("Here we go!", 5.0);
        String workerId = this.makeWebhookRequest(this.deployment);
        // TODO: Check status endpoint every few seconds and update status
        Thread.sleep(1000);
        status.update(workerId, 55.0);
        Thread.sleep(8000);
        status.completeSuccessfully("it's all done :)");
    }

    /**
     * Make a request to Pelias update webhook
     *
     * @return The workerID of the run created on the Pelias server
     */
    private String makeWebhookRequest(Deployment deployment) throws IOException {
        URL url;
        try {
            url = new URL(deployment.peliasWebhookUrl);
        } catch (MalformedURLException ex) {
            status.fail("Webhook URL was not a valid URL", ex);
            return null;
        }

        // Convert from feedVersionIds to Pelias Config objects
        List<PeliasWebhookGTFSFeedFormat> gtfsFeeds = Persistence.feedVersions.getFiltered(in("_id", deployment.feedVersionIds))
                .stream()
                .map(PeliasWebhookGTFSFeedFormat::new)
                .collect(Collectors.toList());

        PeliasWebhookRequestBody peliasWebhookRequestBody = new PeliasWebhookRequestBody();
        peliasWebhookRequestBody.gtfsFeeds = gtfsFeeds;

        String query = JsonUtil.toJson(peliasWebhookRequestBody);

        HttpResponse response;

        try {
            CloseableHttpClient client = HttpClientBuilder.create().build();
            HttpPost request = new HttpPost(deployment.peliasWebhookUrl);
            StringEntity queryEntity = new StringEntity(query);
            request.setEntity(queryEntity);
            request.setHeader("Accept", "application/json");
            request.setHeader("Content-type", "application/json");

            response = client.execute(request);
        } catch (IOException ex) {
            status.fail("Couldn't connect to webhook URL given.", ex);
            return null;
        }

        String json = EntityUtils.toString(response.getEntity());
        JsonNode webhookResponse = null;
        try {
            webhookResponse = JsonUtil.objectMapper.readTree(json);
        } catch (IOException ex) {
            status.fail("Webhook server returned error:", ex);
            return null;
        }

        return webhookResponse.get("workerId").asText();
    }


    /**
     * The request body required by the Pelias webhook
     */
    private class PeliasWebhookRequestBody {
        public List<PeliasWebhookGTFSFeedFormat> gtfsFeeds;
        public List<String> csvFiles;
    }

    /**
     * The GTFS feed info format the Pelias webhook requires
     */
    private class PeliasWebhookGTFSFeedFormat {
        public String uri;
        public String name;
        public String filename;

        public PeliasWebhookGTFSFeedFormat(FeedVersion feedVersion) {
            uri = S3Utils.getS3FeedUri(feedVersion.id);
            name = Persistence.feedSources.getById(feedVersion.feedSourceId).name;
            filename = feedVersion.id;
        }
    }

    private class PeliasWebhookErrorMessage {
        public Boolean completed;
        public String error;
        public String message;
    }
}