package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.jobs.FetchSingleFeedJob;
import com.conveyal.datatools.manager.models.*;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.net.URL;
import java.util.*;

import static spark.Spark.*;

/**
 * Created by demory on 3/21/16.
 */

public class FeedSourceController {

    public static JsonManager<FeedSource> json =
            new JsonManager<>(FeedSource.class, JsonViews.UserInterface.class);

    public static FeedSource getFeedSource(Request req, Response res) {
        String id = req.params("id");
        return FeedSource.get(id);
    }

    public static Collection<FeedSource> getAllFeedSources(Request req, Response res) throws JsonProcessingException {
        Collection<FeedSource> sources = new ArrayList<>();

        String projectId = req.queryParams("projectId");
        Boolean publicFilter = Boolean.valueOf(req.queryParams("public"));
        if(projectId != null) {
            for (FeedSource source: FeedSource.getAll()) {
                if(source.projectId.equals(projectId)) {
                    // if requesting public sources and source is not public; skip source
                    if (publicFilter && !source.isPublic)
                        continue;
                    sources.add(source);
                }
            }
        }
        else {
            for (FeedSource source: FeedSource.getAll()) {
                // if requesting public sources and source is not public; skip source
                if (publicFilter && !source.isPublic)
                    continue;
                sources.add(source);
            }
        }

        return sources;
    }

    public static FeedSource createFeedSource(Request req, Response res) throws IOException {
        FeedSource source;
        /*if (req.queryParams("type") != null){
            //FeedSource.FeedSourceType type = FeedSource.FeedSourceType.TRANSITLAND;
            source = new FeedSource("onestop-id");
            applyJsonToFeedSource(source, req.body());
            source.save();

            return source;
        }
        else {
            source = new FeedSource();

        }*/

        source = new FeedSource();

        applyJsonToFeedSource(source, req.body());
        source.save();

        for(String resourceType : DataManager.feedResources.keySet()) {
            DataManager.feedResources.get(resourceType).feedSourceCreated(source, req.headers("Authorization"));
        }

        return source;
    }

    public static FeedSource updateFeedSource(Request req, Response res) throws IOException {
        String id = req.params("id");
        FeedSource source = FeedSource.get(id);

        applyJsonToFeedSource(source, req.body());
        source.save();

        return source;
    }

    public static void applyJsonToFeedSource(FeedSource source, String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(json);
        Iterator<Map.Entry<String, JsonNode>> fieldsIter = node.fields();
        while (fieldsIter.hasNext()) {
            Map.Entry<String, JsonNode> entry = fieldsIter.next();

            if(entry.getKey().equals("projectId")) {
                System.out.println("setting fs project");
                source.setProject(Project.get(entry.getValue().asText()));
            }

            if(entry.getKey().equals("name")) {
                source.name = entry.getValue().asText();
            }

            if(entry.getKey().equals("url")) {
                source.url = new URL(entry.getValue().asText());
            }

            if(entry.getKey().equals("retrievalMethod")) {
                source.retrievalMethod = FeedSource.FeedRetrievalMethod.FETCHED_AUTOMATICALLY.valueOf(entry.getValue().asText());
            }

            if(entry.getKey().equals("snapshotVersion")) {
                source.snapshotVersion = entry.getValue().asText();
            }

            if(entry.getKey().equals("isPublic")) {
                source.isPublic = entry.getValue().asBoolean();
            }

        }
    }

    public static FeedSource updateExternalFeedResource(Request req, Response res) throws IOException {
        String id = req.params("id");
        FeedSource source = FeedSource.get(id);
        String resourceType = req.queryParams("resourceType");
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(req.body());
        Iterator<Map.Entry<String, JsonNode>> fieldsIter = node.fields();

        while (fieldsIter.hasNext()) {
            Map.Entry<String, JsonNode> entry = fieldsIter.next();
            ExternalFeedSourceProperty prop =
                    ExternalFeedSourceProperty.find(source, resourceType, entry.getKey());

            if (prop != null) {
                // update the property in our DB
                String previousValue = prop.value;
                prop.value = entry.getValue().asText();
                prop.save();

                // trigger an event on the external resource
                if(DataManager.feedResources.containsKey(resourceType)) {
                    DataManager.feedResources.get(resourceType).propertyUpdated(prop, previousValue, req.headers("Authorization"));
                }

            }

        }

        return source;
    }

    public static FeedSource deleteFeedSource(Request req, Response res) {
        String id = req.params("id");
        FeedSource source = FeedSource.get(id);
        source.delete();
        return source;
    }

    /**
     * Refetch this feed
     * @throws JsonProcessingException
     */
    public static Boolean fetch (Request req, Response res) throws JsonProcessingException {

        FeedSource s = FeedSource.get(req.params("id"));

        System.out.println("fetching feed for source "+ s.name);
        // ways to have permission to do this:
        // 1) be an admin
        // 2) have access to this feed through project permissions
        // if all fail, the user cannot do this.

        //if (!userProfile.canAdministerProject(s.feedCollectionId) && !userProfile.canManageFeed(s.feedCollectionId, s.id))
        //    return unauthorized();

        FetchSingleFeedJob job = new FetchSingleFeedJob(s);
        job.run();
        return true;
    }


    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/feedsource/:id", FeedSourceController::getFeedSource, json::write);
        options(apiPrefix + "secure/feedsource", (q, s) -> "");
        get(apiPrefix + "secure/feedsource", FeedSourceController::getAllFeedSources, json::write);
        post(apiPrefix + "secure/feedsource", FeedSourceController::createFeedSource, json::write);
        put(apiPrefix + "secure/feedsource/:id", FeedSourceController::updateFeedSource, json::write);
        put(apiPrefix + "secure/feedsource/:id/updateExternal", FeedSourceController::updateExternalFeedResource, json::write);
        delete(apiPrefix + "secure/feedsource/:id", FeedSourceController::deleteFeedSource, json::write);
        post(apiPrefix + "secure/feedsource/:id/fetch", FeedSourceController::fetch, JsonUtil.objectMapper::writeValueAsString);

        // Public routes
        get(apiPrefix + "public/feedsource/:id", FeedSourceController::getFeedSource, json::write);
        get(apiPrefix + "public/feedsource", FeedSourceController::getAllFeedSources, json::write);
    }
}