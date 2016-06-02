package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.datastore.AgencyTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.transit.Trip;
import com.conveyal.datatools.editor.models.transit.TripPattern;
import org.mapdb.Fun;
import org.mapdb.Fun.Tuple2;

import java.util.Collection;
import java.util.Set;

import spark.Request;
import spark.Response;

import static spark.Spark.*;


public class TripPatternController {

    public static JsonManager<TripPattern> json =
            new JsonManager<>(TripPattern.class, JsonViews.UserInterface.class);

    public static Object getTripPattern(Request req, Response res) {
        String id = req.params("id");
        String routeId = req.queryParams("routeId");
        String feedId = req.queryParams("feedId");
        Object json = null;

        if (feedId == null)
            feedId = req.session().attribute("feedId");

        if (feedId == null) {
            halt(400);
        }

        final AgencyTx tx = VersionedDataStore.getAgencyTx(feedId);

        try {

            if(id != null) {
               if (!tx.tripPatterns.containsKey(id))
                   halt(404);
               else
                   json = Base.toJson(tx.tripPatterns.get(id), false);
            }
            else if (routeId != null) {

                if (!tx.routes.containsKey(routeId))
                    halt(404);
                else {
                    Set<Tuple2<String, String>> tpKeys = tx.tripPatternsByRoute.subSet(new Tuple2(routeId, null), new Tuple2(routeId, Fun.HI));

                    Collection<TripPattern> patts = Collections2.transform(tpKeys, new Function<Tuple2<String, String>, TripPattern>() {

                        @Override
                        public TripPattern apply(Tuple2<String, String> input) {
                            return tx.tripPatterns.get(input.b);
                        }
                    });

                    json = Base.toJson(patts, false);
                }
            }
            else {
                halt(400);
            }
            
            tx.rollback();
            
        } catch (Exception e) {
            tx.rollback();
            e.printStackTrace();
            halt(400);
        }
        return json;
    }

    public static Object createTripPattern(Request req, Response res) {
        TripPattern tripPattern;
        
        try {
            tripPattern = Base.mapper.readValue(req.body(), TripPattern.class);
            
            if (req.session().attribute("feedId") != null && !req.session().attribute("feedId").equals(tripPattern.feedId))
                halt(400);
            
            if (!VersionedDataStore.agencyExists(tripPattern.feedId)) {
                halt(400);
            }
            
            AgencyTx tx = VersionedDataStore.getAgencyTx(tripPattern.feedId);
            
            if (tx.tripPatterns.containsKey(tripPattern.id)) {
                tx.rollback();
                halt(400);
            }
            
            tripPattern.calcShapeDistTraveled();
            
            tx.tripPatterns.put(tripPattern.id, tripPattern);
            tx.commit();

            return Base.toJson(tripPattern, false);
        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
        }
        return null;
    }

    public static Object updateTripPattern(Request req, Response res) {
        TripPattern tripPattern;
        AgencyTx tx = null;
        try {
            tripPattern = Base.mapper.readValue(req.body(), TripPattern.class);
            
            if (req.session().attribute("feedId") != null && !req.session().attribute("feedId").equals(tripPattern.feedId))
                halt(400);
            
            if (!VersionedDataStore.agencyExists(tripPattern.feedId)) {
                halt(400);
            }
            
            if (tripPattern.id == null) {
                halt(400);
            }
            
            tx = VersionedDataStore.getAgencyTx(tripPattern.feedId);
            
            TripPattern originalTripPattern = tx.tripPatterns.get(tripPattern.id);
            
            if(originalTripPattern == null) {
                tx.rollback();
                halt(400);
            }
                        
            // update stop times
            try {
                TripPattern.reconcilePatternStops(originalTripPattern, tripPattern, tx);
            } catch (IllegalStateException e) {
                tx.rollback();
                halt(400);
            }
            
            tripPattern.calcShapeDistTraveled();
            
            tx.tripPatterns.put(tripPattern.id, tripPattern);
            tx.commit();

            return Base.toJson(tripPattern, false);
        } catch (Exception e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
            halt(400);
        }
        return null;
    }

    public static Object deleteTripPattern(Request req, Response res) {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");

        if (feedId == null)
            feedId = req.session().attribute("feedId");

        if(id == null || feedId == null) {
            halt(400);
        }

        AgencyTx tx = VersionedDataStore.getAgencyTx(feedId);

        try {
            // first zap all trips on this trip pattern
            for (Trip trip : tx.getTripsByPattern(id)) {
                tx.trips.remove(trip.id);
            }

            tx.tripPatterns.remove(id);
            tx.commit();
        } finally {
            tx.rollbackIfOpen();
        }
        return null;
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/trippattern/:id", TripPatternController::getTripPattern, json::write);
        options(apiPrefix + "secure/trippattern", (q, s) -> "");
        get(apiPrefix + "secure/trippattern", TripPatternController::getTripPattern, json::write);
        post(apiPrefix + "secure/trippattern", TripPatternController::createTripPattern, json::write);
        put(apiPrefix + "secure/trippattern/:id", TripPatternController::updateTripPattern, json::write);
        delete(apiPrefix + "secure/trippattern/:id", TripPatternController::deleteTripPattern, json::write);
    }
}
