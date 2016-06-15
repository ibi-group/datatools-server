package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.editor.datastore.FeedTx;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.transit.*;
import org.geotools.referencing.GeodeticCalculator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.mapdb.BTreeMap;
import spark.Request;
import spark.Response;

import static spark.Spark.*;


public class StopController {

    public static JsonManager<Stop> json =
            new JsonManager<>(Stop.class, JsonViews.UserInterface.class);

    public static Object getStop(Request req, Response res) {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");
        String patternId = req.queryParams("patternId");
        Boolean majorStops = Boolean.valueOf(req.queryParams("majorStops"));
        Double west = null;
        if (req.queryParams("west") != null)
            west = Double.valueOf(req.queryParams("west"));
        Double east = null;
        if (req.queryParams("east") != null)
            east = Double.valueOf(req.queryParams("east"));
        Double north = null;
        if (req.queryParams("north") != null)
            north = Double.valueOf(req.queryParams("north"));
        Double south = null;
        if (req.queryParams("south") != null)
            south = Double.valueOf(req.queryParams("south"));

        if (feedId == null)
            feedId = req.session().attribute("feedId");

        if (feedId == null) {
            halt(400);
        }

        final FeedTx tx = VersionedDataStore.getFeedTx(feedId);
 
        try {
              if (id != null) {
                if (!tx.stops.containsKey(id)) {
                    tx.rollback();
                    halt(404);
                }

                Object json = Base.toJson(tx.stops.get(id), false);
                tx.rollback();
                return json;
            }
              else if (Boolean.TRUE.equals(majorStops)) {
                  // get the major stops for the agency
                  Collection<Stop> stops = Collections2.transform(tx.majorStops, new Function<String, Stop>() {
                    @Override
                    public Stop apply(String input) {
                        // TODO Auto-generated method stub
                        return tx.stops.get(input);
                    }
                  });

                  Object stopsJson = Base.toJson(stops, false);
                  tx.rollback();
                  return stopsJson;
              }
            else if (west != null && east != null && south != null && north != null) {
                Collection<Stop> matchedStops = tx.getStopsWithinBoundingBox(north, east, south, west);
                Object json = Base.toJson(matchedStops, false);
                tx.rollback();
                return json;
            }
            else if (patternId != null) {
                if (!tx.tripPatterns.containsKey(patternId)) {
                    halt(404);
                }

                TripPattern p = tx.tripPatterns.get(patternId);

                Collection<Stop> ret = Collections2.transform(p.patternStops, new Function<TripPatternStop, Stop>() {
                    @Override
                    public Stop apply(TripPatternStop input) {
                        return tx.stops.get(input.stopId);
                    }
                });

                Object json = Base.toJson(ret, false);
                tx.rollback();
                return json;
            }
              // return all
            else {
                  BTreeMap<String, Stop> stops;
                  try {
                      stops = tx.stops;
                      Collection<Stop> matchedStops = stops.values();
                      Object json = Base.toJson(matchedStops, false);
                      tx.rollback();
                      return json;
                  } catch (IllegalAccessError e) {
                      return new ArrayList<>();
                  }

            }

        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
            tx.rollback();
        }
        return null;
    }

    public static Object createStop(Request req, Response res) {
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


    public static Object updateStop(Request req, Response res) {
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
            
            if (!tx.stops.containsKey(stop.id)) {
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

    public static Object deleteStop(Request req, Response res) {
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
    
    
    public static Object findDuplicateStops(Request req, Response res) {
        String feedId = req.queryParams("feedId");
        Object json = null;

        if (feedId == null)
            feedId = req.session().attribute("feedId");

        if (feedId == null) {
            halt(400);
        }

        FeedTx atx = VersionedDataStore.getFeedTx(feedId);

        try {
            List<List<Stop>> ret = new ArrayList<List<Stop>>();

            for (Stop stop : atx.stops.values()) {
                // find nearby stops, within 5m
                // at the equator, 1 degree is 111 km
                // everywhere else this will overestimate, which is why we have a distance check as well (below)
                double thresholdDegrees = 5 / 111000d;

                Collection<Stop> candidateStops = atx.getStopsWithinBoundingBox(
                        stop.getLat() + thresholdDegrees,
                        stop.getLon() + thresholdDegrees,
                        stop.getLat() - thresholdDegrees,
                        stop.getLon() - thresholdDegrees);

                // we will always find a single stop, this one.
                if (candidateStops.size() <= 1)
                    continue;

                List<Stop> duplicatesOfThis = new ArrayList<Stop>();

                // note: this stop will be added implicitly because it is distance zero from itself
                GeodeticCalculator gc = new GeodeticCalculator();
                gc.setStartingGeographicPoint(stop.getLon(), stop.getLat());
                for (Stop other : candidateStops) {
                    gc.setDestinationGeographicPoint(other.getLon(), other.getLat());
                    if (gc.getOrthodromicDistance() < 10) {
                        duplicatesOfThis.add(other);
                    }
                }

                if (duplicatesOfThis.size() > 1) {
                    ret.add(duplicatesOfThis);
                }
            }

            json = Base.toJson(ret, false);
         } catch (Exception e) {
             e.printStackTrace();
             halt(400);
         }
        finally {
            atx.rollback();
        }
        return json;
    }

    public static Object mergeStops(Request req, Response res) {
        List<String> mergedStopIds = Arrays.asList(req.queryParams("mergedStopIds").split(","));
        String feedId = req.queryParams("feedId");

        if (mergedStopIds.size() <= 1) {
            halt(400);
        }

        if (feedId == null)
            feedId = req.session().attribute("feedId");

        if (feedId == null) {
            halt(400);
        }

        FeedTx tx = VersionedDataStore.getFeedTx(feedId);

        try {
            Stop.merge(mergedStopIds, tx);
            tx.commit();
        } finally {
            tx.rollbackIfOpen();
        }
        return true;
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/stop/:id", StopController::getStop, json::write);
        options(apiPrefix + "secure/stop", (q, s) -> "");
        get(apiPrefix + "secure/stop", StopController::getStop, json::write);
        get(apiPrefix + "secure/stop/mergeStops", StopController::mergeStops, json::write);
        post(apiPrefix + "secure/stop", StopController::createStop, json::write);
        put(apiPrefix + "secure/stop/:id", StopController::updateStop, json::write);
        delete(apiPrefix + "secure/stop/:id", StopController::deleteStop, json::write);
    }
}
