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

        if (id == null) {
            halt(400);
        }
        GlobalTx gtx = null;
        EditorFeed fs = null;
        try {
            gtx = VersionedDataStore.getGlobalTx();
            fs = gtx.feeds.get(id);

            applyJsonToFeedInfo(fs, req.body());
            gtx.commit();
        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
        } finally {
            if (gtx != null) gtx.rollbackIfOpen();
        }

        return fs;
    }

    public static void applyJsonToFeedInfo(EditorFeed source, String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(json);
        Iterator<Map.Entry<String, JsonNode>> fieldsIter = node.fields();
        while (fieldsIter.hasNext()) {
            Map.Entry<String, JsonNode> entry = fieldsIter.next();

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
                String dateString = entry.getValue().asText();
                DateFormat formatter = new SimpleDateFormat("d-MMM-yyyy,HH:mm:ss aaa");
                try {
                    Date date = formatter.parse(dateString);
                    source.feedStartDate = date;
                } catch (ParseException e) {
                    e.printStackTrace();
                    halt(400, dateString + " is not a valid date");
                }
            }

            if(entry.getKey().equals("feedEndDate")) {
                String dateString = entry.getValue().asText();
                DateFormat formatter = new SimpleDateFormat("d-MMM-yyyy,HH:mm:ss aaa");
                try {
                    Date date = formatter.parse(dateString);
                    source.feedEndDate = date;
                } catch (ParseException e) {
                    e.printStackTrace();
                    halt(400, dateString + " is not a valid date");
                }
            }

        }
    }
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
