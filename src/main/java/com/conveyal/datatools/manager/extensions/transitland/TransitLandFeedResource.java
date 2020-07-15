package com.conveyal.datatools.manager.extensions.transitland;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.extensions.ExternalFeedResource;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedRetrievalMethod;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import static com.conveyal.datatools.manager.models.ExternalFeedSourceProperty.constructId;

/**
 * Created by demory on 3/31/16.
 */

public class TransitLandFeedResource implements ExternalFeedResource {

    public static final Logger LOG = LoggerFactory.getLogger(TransitLandFeedResource.class);

    private String api;

    public TransitLandFeedResource() {
        api = DataManager.getConfigPropertyAsText("extensions.transitland.api");
    }

    @Override
    public String getResourceType() {
        return "TRANSITLAND";
    }

    @Override
    public void importFeedsForProject(Project project, String authHeader) throws IOException, IllegalAccessException {
        LOG.info("Importing TransitLand feeds");
        URL url = null;
        ObjectMapper mapper = new ObjectMapper();
        int perPage = 10000;
        int count = 0;
        int offset;
        int total = 0;
        String locationFilter = "";
        boolean nextPage = true;

        if (project.bounds != null) {
            locationFilter = "&bbox=" + project.bounds.toTransitLandString();
        }

        do {
            offset = perPage * count;
            try {
                url = new URL(api + "?total=true&per_page=" + perPage + "&offset=" + offset + locationFilter);
            } catch (MalformedURLException ex) {
                LOG.error("Error constructing TransitLand API URL");
                throw ex;
            }

            try {
                HttpURLConnection con = (HttpURLConnection) url.openConnection();

                // optional default is GET
                con.setRequestMethod("GET");

                //add request header
                con.setRequestProperty("User-Agent", "User-Agent");

                int responseCode = con.getResponseCode();
                LOG.info("Sending 'GET' request to URL : " + url);
                LOG.info("Response Code : " + responseCode);

                BufferedReader in = new BufferedReader(
                        new InputStreamReader(con.getInputStream()));
                String inputLine;
                StringBuffer response = new StringBuffer();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();

                String json = response.toString();
                JsonNode node = mapper.readTree(json);
                total = node.get("meta").get("total").asInt();
                for (JsonNode feed : node.get("feeds")) {
                    TransitLandFeed tlFeed = new TransitLandFeed(feed);

                    FeedSource source = null;

                    // Check if a feed source already exists in the project with this id, i.e., a sync
                    // has already occurred in the past and most feed sources may already exist
                    for (FeedSource existingSource : project.retrieveProjectFeedSources()) {
                        ExternalFeedSourceProperty onestopIdProp =
                                Persistence.externalFeedSourceProperties.getById(constructId(existingSource, this.getResourceType(), "onestop_id"));
                        if (onestopIdProp != null && onestopIdProp.value.equals(tlFeed.onestop_id)) {
                            source = existingSource;
                        }
                    }

                    String feedName;
                    feedName = tlFeed.onestop_id;

                    // FIXME: lots of duplicated code here, but I'm not sure if Mongo has an updateOrCreate function.
                    // Feed source is new, let's store a new one.
                    if (source == null) {
                        source = new FeedSource(feedName);
                        source.projectId = project.id;
                        source.retrievalMethod = FeedRetrievalMethod.FETCHED_AUTOMATICALLY;
                        try {
                            source.url = new URL(tlFeed.url);
                        } catch (MalformedURLException e) {
                            throw e;
                        }
                        Persistence.feedSources.create(source);
                        LOG.info("Creating new feed source: {}", source.name);
                    } else {
                        // Feed source already existed. Let's just sync it.
                        URL feedUrl;
                        source.retrievalMethod = FeedRetrievalMethod.FETCHED_AUTOMATICALLY;
                        try {
                            feedUrl = new URL(tlFeed.url);
                            Persistence.feedSources.updateField(source.id, "url", feedUrl);
                        } catch (MalformedURLException e) {
                            throw e;
                        }
                        // FIXME: These shouldn't be separate updates.
                        Persistence.feedSources.updateField(source.id, "name", feedName);
                        Persistence.feedSources.updateField(source.id, "retrievalMethod", FeedRetrievalMethod.FETCHED_AUTOMATICALLY);
                        LOG.info("Syncing properties: {}", source.name);
                    }

                    // create / update the properties

                    for(Field tlField : tlFeed.getClass().getDeclaredFields()) {
                        String fieldName = tlField.getName();
                        String fieldValue = tlField.get(tlFeed) != null ? tlField.get(tlFeed).toString() : null;

                        // FIXME
//                        ExternalFeedSourceProperty.updateOrCreate(source, this.getResourceType(), fieldName, fieldValue);
                    }
                }
            } catch (Exception ex) {
                LOG.error("Error reading from TransitLand API");
                throw ex;
            }
            count++;
        }
        // iterate over results until most recent total exceeds total feeds in TransitLand
        while(offset + perPage < total);

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
