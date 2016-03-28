package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.jobs.FetchSingleFeedJob;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static spark.Spark.*;

/**
 * Created by demory on 3/21/16.
 */

public class FeedSourceController {

    public static FeedSource getFeedSource(Request req, Response res) {
        String id = req.params("id");
        return FeedSource.get(id);
    }

    public static Collection<FeedSource> getAllFeedSources(Request req, Response res) throws JsonProcessingException {
        Collection<FeedSource> sources = new ArrayList<>();

        String projectId = req.queryParams("projectId");
        if(projectId != null) {
            for (FeedSource source: FeedSource.getAll()) {
                if(source.projectId.equals(projectId)) sources.add(source);
            }
        }
        else sources = FeedSource.getAll();

        return sources;
    }

    public static FeedSource createFeedSource(Request req, Response res) throws IOException {
        FeedSource source;
        if (req.queryParams("type") != null){
            FeedSource.FeedSourceType type = FeedSource.FeedSourceType.TRANSITLAND;
            source = new FeedSource(type, "onestop-id");
            applyJsonToFeedSource(source, req.body());
            source.save();

            return source;
        }
        else {
            source = new FeedSource();

        }

        applyJsonToFeedSource(source, req.body());
        source.save();

        return source;


    }

    public static FeedSource updateFeedSource(Request req, Response res) throws IOException {
        String id = req.params("id");
        System.out.println(">>> updating FS " + id);
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


        }
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
        get(apiPrefix + "secure/feedsource/:id", FeedSourceController::getFeedSource, JsonUtil.objectMapper::writeValueAsString);
        get(apiPrefix + "secure/feedsource", FeedSourceController::getAllFeedSources, JsonUtil.objectMapper::writeValueAsString);
        post(apiPrefix + "secure/feedsource", FeedSourceController::createFeedSource, JsonUtil.objectMapper::writeValueAsString);
        put(apiPrefix + "secure/feedsource/:id", FeedSourceController::updateFeedSource, JsonUtil.objectMapper::writeValueAsString);
        delete(apiPrefix + "secure/feedsource/:id", FeedSourceController::deleteFeedSource, JsonUtil.objectMapper::writeValueAsString);
        post(apiPrefix + "secure/feedsource/:id/fetch", FeedSourceController::fetch, JsonUtil.objectMapper::writeValueAsString);
    }
}