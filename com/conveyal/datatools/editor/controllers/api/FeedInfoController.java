package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.datastore.FeedTx;
import com.conveyal.datatools.editor.datastore.GlobalTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.transit.EditorFeed;
import com.conveyal.datatools.editor.models.transit.Stop;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.geotools.referencing.GeodeticCalculator;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static spark.Spark.*;
import static spark.Spark.delete;
import static spark.Spark.put;

/**
 * Created by landon on 6/14/16.
 */
public class FeedInfoController {
    public static JsonManager<Stop> json =
            new JsonManager<>(Stop.class, JsonViews.UserInterface.class);

    public static Object getFeedInfo(Request req, Response res) {
        String id = req.params("id");

        if (id == null) {
            return null;
            // TODO: return all feedInfos for project?
        }
        GlobalTx gtx = VersionedDataStore.getGlobalTx();

        EditorFeed fs = gtx.feeds.get(id);
        return fs;
    }

    public static Object createFeedInfo(Request req, Response res) {
        FeedTx tx = null;
        Object json = null;
        try {
            Stop stop = Base.mapper.readValue(req.body(), Stop.class);

            if (req.session().attribute("feedId") != null && !req.session().attribute("feedId").equals(stop.feedId))
                halt(400);

            if (!VersionedDataStore.agencyExists(stop.feedId)) {
                halt(400);
            }

            tx = VersionedDataStore.getFeedTx(stop.feedId);

            if (tx.stops.containsKey(stop.id)) {
                halt(400);
            }

            tx.stops.put(stop.id, stop);
            tx.commit();
            json = Base.toJson(stop, false);
        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
        } finally {
            if (tx != null) tx.rollbackIfOpen();
        }
        return json;
    }


    public static Object updateFeedInfo(Request req, Response res) throws IOException {
        String id = req.params("id");

        EditorFeed feed;

        try {
            feed = Base.mapper.readValue(req.body(), EditorFeed.class);

            GlobalTx gtx = VersionedDataStore.getGlobalTx();

            if(!gtx.feeds.containsKey(feed.id)) {
                gtx.rollback();
                halt(400);
                return null;
            }

            gtx.feeds.put(feed.id, feed);
            gtx.commit();

            return Base.toJson(feed, false);
        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
        }
        return null;
    }

    public static void applyJsonToFeedInfo(EditorFeed source, String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(json);
        Iterator<Map.Entry<String, JsonNode>> fieldsIter = node.fields();
        while (fieldsIter.hasNext()) {
            Map.Entry<String, JsonNode> entry = fieldsIter.next();

            if (entry.getValue().isNull()) {
                continue;
            }

            if(entry.getKey().equals("color")) {
                source.color = entry.getValue().asText();
            }

            if(entry.getKey().equals("defaultLat")) {
                source.defaultLat = entry.getValue().asDouble();
            }

            if(entry.getKey().equals("defaultLon")) {
                source.defaultLon = entry.getValue().asDouble();
            }

            if(entry.getKey().equals("routeTypeId")) {
                source.routeTypeId = entry.getValue().asText();
            }

            if(entry.getKey().equals("feedPublisherName")) {
                source.feedPublisherName = entry.getValue().asText();
            }

            if(entry.getKey().equals("feedVersion")) {
                source.feedVersion = entry.getValue().asText();
            }

            if(entry.getKey().equals("feedPublisherUrl")) {
                String url = entry.getValue().asText();
                try {
                    source.feedPublisherUrl = new URL(url);

                } catch (MalformedURLException e) {
                    halt(400, "URL '" + url + "' not valid.");
                }
                source.feedPublisherUrl = new URL(entry.getValue().asText());
            }

            if(entry.getKey().equals("feedLang")) {
                source.feedLang = entry.getValue().asText();
            }

            if(entry.getKey().equals("feedStartDate")) {
                System.out.println(entry.getValue());
                Long seconds = entry.getValue().asLong();
                Long days = seconds / 60 / 60 / 24;
//                System.out.println(days);
                try {
                    LocalDate date = LocalDate.ofEpochDay(days);
//                    System.out.println(date.format(DateTimeFormatter.BASIC_ISO_DATE));
                    source.feedStartDate = date;
                } catch (Exception e) {
                    e.printStackTrace();
                    halt(400, seconds + " is not a valid date");
                }
            }

            if(entry.getKey().equals("feedEndDate")) {
                Long seconds = entry.getValue().asLong();
                Long days = seconds / 60 / 60 / 24;
//                System.out.println(days);
                try {
                    LocalDate date = LocalDate.ofEpochDay(days);
                    source.feedEndDate = date;
                } catch (Exception e) {
                    e.printStackTrace();
                    halt(400, seconds + " is not a valid date");
                }
            }

        }
    }
    // TODO: deleting editor feed is handled in delete feed source?
    public static Object deleteFeedInfo(Request req, Response res) {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");
        Object json = null;

        if (feedId == null)
            feedId = req.session().attribute("feedId");

        if (feedId == null) {
            halt(400);
        }

        FeedTx tx = VersionedDataStore.getFeedTx(feedId);
        try {
            if (!tx.stops.containsKey(id)) {
                halt(404);
            }

            if (!tx.getTripPatternsByStop(id).isEmpty()) {
                halt(400);
            }

            Stop s = tx.stops.remove(id);
            tx.commit();
            json = Base.toJson(s, false);
        } catch (Exception e) {
            halt(400);
            e.printStackTrace();
        } finally {
            tx.rollbackIfOpen();
        }
        return json;
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/feedinfo/:id", FeedInfoController::getFeedInfo, json::write);
        options(apiPrefix + "secure/feedinfo", (q, s) -> "");
        get(apiPrefix + "secure/feedinfo", FeedInfoController::getFeedInfo, json::write);
        post(apiPrefix + "secure/feedinfo", FeedInfoController::createFeedInfo, json::write);
        put(apiPrefix + "secure/feedinfo/:id", FeedInfoController::updateFeedInfo, json::write);
        delete(apiPrefix + "secure/feedinfo/:id", FeedInfoController::deleteFeedInfo, json::write);
    }
}
