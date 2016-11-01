package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.datastore.FeedTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.transit.StopTime;
import com.conveyal.datatools.editor.models.transit.Trip;
import com.conveyal.datatools.editor.models.transit.TripPattern;
import com.conveyal.datatools.editor.models.transit.TripPatternStop;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import spark.Request;
import spark.Response;

import static spark.Spark.*;


public class TripController {
    public static final Logger LOG = LoggerFactory.getLogger(TripController.class);

    public static JsonManager<Trip> json =
            new JsonManager<>(Trip.class, JsonViews.UserInterface.class);

    public static Object getTrip(Request req, Response res
//            String id, String patternId, String calendarId, String feedId
    ) {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");
        String patternId = req.queryParams("patternId");
        String calendarId = req.queryParams("calendarId");

        if (feedId == null)
            feedId = req.session().attribute("feedId");

        if (feedId == null) {
            halt(400);
        }

        FeedTx tx = VersionedDataStore.getFeedTx(feedId);

        try {
            if (id != null) {
                if (tx.trips.containsKey(id))
                    return Base.toJson(tx.trips.get(id), false);
                else
                    halt(404);
            }
            else if (patternId != null && calendarId != null) {
                if (!tx.tripPatterns.containsKey(patternId) || !tx.calendars.containsKey(calendarId)) {
                    halt(404);
                }
                else {
                    return Base.toJson(tx.getTripsByPatternAndCalendar(patternId, calendarId), false);
                }
            }

            else if(patternId != null) {
                return Base.toJson(tx.getTripsByPattern(patternId), false);
            }
            else {
                return Base.toJson(tx.trips.values(), false);
            }
                
        } catch (Exception e) {
            tx.rollback();
            e.printStackTrace();
            halt(400);
        }
        return null;
    }
    
    public static Object createTrip(Request req, Response res) {
        FeedTx tx = null;

        try {
            Trip trip = Base.mapper.readValue(req.body(), Trip.class);

            if (req.session().attribute("feedId") != null && !req.session().attribute("feedId").equals(trip.feedId))
                halt(400);

            if (!VersionedDataStore.feedExists(trip.feedId)) {
                halt(400);
            }

            tx = VersionedDataStore.getFeedTx(trip.feedId);

            if (tx.trips.containsKey(trip.id)) {
                tx.rollback();
                halt(400);
            }

            if (!tx.tripPatterns.containsKey(trip.patternId) || trip.stopTimes.size() != tx.tripPatterns.get(trip.patternId).patternStops.size()) {
                tx.rollback();
                halt(400);
            }

            tx.trips.put(trip.id, trip);
            tx.commit();

            return Base.toJson(trip, false);
        } catch (Exception e) {
            e.printStackTrace();
            if (tx != null) tx.rollbackIfOpen();
            halt(400);
        }
        return null;
    }
    
    public static Object updateTrip(Request req, Response res) {
        FeedTx tx = null;

        try {
            Trip trip = Base.mapper.readValue(req.body(), Trip.class);

            if (req.session().attribute("feedId") != null && !req.session().attribute("feedId").equals(trip.feedId))
                halt(400);

            if (!VersionedDataStore.feedExists(trip.feedId)) {
                halt(400);
            }

            tx = VersionedDataStore.getFeedTx(trip.feedId);

            if (!tx.trips.containsKey(trip.id)) {
                tx.rollback();
                halt(400);
            }

            if (!tx.tripPatterns.containsKey(trip.patternId) || trip.stopTimes.size() != tx.tripPatterns.get(trip.patternId).patternStops.size()) {
                tx.rollback();
                halt(400);
            }

            TripPattern patt = tx.tripPatterns.get(trip.patternId);

            // confirm that each stop in the trip matches the stop in the pattern

            for (int i = 0; i < trip.stopTimes.size(); i++) {
                TripPatternStop ps = patt.patternStops.get(i);
                StopTime st =  trip.stopTimes.get(i);

                if (st == null)
                    // skipped stop
                    continue;

                if (!st.stopId.equals(ps.stopId)) {
                    LOG.error("Mismatch between stop sequence in trip and pattern at position {}, pattern: {}, stop: {}", i, ps.stopId, st.stopId);
                    tx.rollback();
                    halt(400);
                }
            }

            tx.trips.put(trip.id, trip);
            tx.commit();

            return Base.toJson(trip, false);
        } catch (Exception e) {
            if (tx != null) tx.rollbackIfOpen();
            e.printStackTrace();
            halt(400);
        }
        return null;
    }

    public static Object deleteTrip(Request req, Response res) {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");
        Object json = null;

        if (feedId == null)
            feedId = req.session().attribute("feedId");

        if (id == null || feedId == null) {
            halt(400);
        }

        FeedTx tx = VersionedDataStore.getFeedTx(feedId);
        Trip trip = tx.trips.remove(id);
        try {
            json = Base.toJson(trip, false);
        } catch (IOException e) {
            halt(400);
        }
        tx.commit();
        
        return json;
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/trip/:id", TripController::getTrip, json::write);
        options(apiPrefix + "secure/trip", (q, s) -> "");
        get(apiPrefix + "secure/trip", TripController::getTrip, json::write);
        post(apiPrefix + "secure/trip", TripController::createTrip, json::write);
        put(apiPrefix + "secure/trip/:id", TripController::updateTrip, json::write);
        delete(apiPrefix + "secure/trip/:id", TripController::deleteTrip, json::write);
    }
}
