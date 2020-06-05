package com.conveyal.datatools.manager.extensions.transitfeeds;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.extensions.ExternalFeedResource;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedRetrievalMethod;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import static com.conveyal.datatools.manager.models.ExternalFeedSourceProperty.constructId;

/**
 * Created by demory on 3/31/16.
 */
public class TransitFeedsFeedResource implements ExternalFeedResource {

    public static final Logger LOG = LoggerFactory.getLogger(TransitFeedsFeedResource.class);

    private static final String api = "http://api.transitfeeds.com/v1/getFeeds";
    private String apiKey;

    public TransitFeedsFeedResource () {
        apiKey = DataManager.getConfigPropertyAsText("TRANSITFEEDS_KEY");
    }

    @Override
    public String getResourceType() {
        return "TRANSITFEEDS";
    }

    @Override
    public void importFeedsForProject(Project project, String authHeader) throws IOException {
        LOG.info("Importing feeds from TransitFeeds");

        URL url;
        // multiple pages for transitfeeds because of 100 feed limit per page
        boolean nextPage = true;
        int count = 1;

        do {
            try {
                url = new URL(api + "?key=" + apiKey + "&limit=100" + "&page=" + String.valueOf(count));
            } catch (MalformedURLException ex) {
                LOG.error("Could not construct URL for TransitFeeds API");
                throw ex;
            }


            StringBuffer response = new StringBuffer();

            try {
                HttpURLConnection con = (HttpURLConnection) url.openConnection();

                // optional default is GET
                con.setRequestMethod("GET");

                //add request header
                con.setRequestProperty("User-Agent", "User-Agent");

                int responseCode = con.getResponseCode();
                System.out.println("\nSending 'GET' request to URL : " + url);
                System.out.println("Response Code : " + responseCode);

                BufferedReader in = new BufferedReader(
                        new InputStreamReader(con.getInputStream()));
                String inputLine;

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();
            } catch (IOException ex) {
                LOG.error("Could not read from Transit Feeds API");
                throw ex;
            }

            String json = response.toString();
            JsonNode transitFeedNode = null;
            try {
                transitFeedNode = JsonUtil.objectMapper.readTree(json);
            } catch (IOException ex) {
                LOG.error("Error parsing TransitFeeds JSON response");
                throw ex;
            }

            for (JsonNode feed : transitFeedNode.get("results").get("feeds")) {

                // test that feed is in fact GTFS
                if (!feed.get("t").asText().contains("GTFS")){
                    continue;
                }

                // test that feed falls in bounding box (if box exists)
                if (project.bounds != null) {
                    Double lat = feed.get("l").get("lat").asDouble();
                    Double lng = feed.get("l").get("lng").asDouble();
                    if (lat < project.bounds.south || lat > project.bounds.north || lng < project.bounds.west || lng > project.bounds.east) {
                        continue;
                    }
                }

                FeedSource source = null;
                String tfId = feed.get("id").asText();

                // check if a feed already exists with this id
                for (FeedSource existingSource : project.retrieveProjectFeedSources()) {
                    ExternalFeedSourceProperty idProp =
                            Persistence.externalFeedSourceProperties.getById(constructId(existingSource, this.getResourceType(), "id"));
                    if (idProp != null && idProp.value.equals(tfId)) {
                        source = existingSource;
                    }
                }

                String feedName;
                feedName = feed.get("t").asText();

                if (source == null) source = new FeedSource(feedName);
                else source.name = feedName;

                source.retrievalMethod = FeedRetrievalMethod.FETCHED_AUTOMATICALLY;
                source.name = feedName;
                System.out.println(source.name);

                try {
                    if (feed.get("u") != null) {
                        if (feed.get("u").get("d") != null) {
                            source.url = new URL(feed.get("u").get("d").asText());
                        } else if (feed.get("u").get("i") != null) {
                            source.url = new URL(feed.get("u").get("i").asText());
                        }
                    }
                } catch (MalformedURLException ex) {
                    LOG.error("Error constructing URLs from TransitFeeds API response");
                    throw ex;
                }

                source.projectId = project.id;
                // FIXME: Store feed source
//                source.save();

                // create/update the external props
                // FIXME: Add this back in
//                ExternalFeedSourceProperty.updateOrCreate(source, this.getResourceType(), "id", tfId);

            }
            if (transitFeedNode.get("results").get("page") == transitFeedNode.get("results").get("numPages")){
                LOG.info("finished last page of transitfeeds");
                nextPage = false;
            }
            count++;
        } while(nextPage);
    }

    @Override
    public void feedSourceCreated(FeedSource source, String authHeader) {

    }

    @Override
    public void propertyUpdated(ExternalFeedSourceProperty property, String previousValue, String authHeader) {

    }

    @Override
    public void feedVersionCreated(FeedVersion feedVersion, String authHeader) {

    }
}
