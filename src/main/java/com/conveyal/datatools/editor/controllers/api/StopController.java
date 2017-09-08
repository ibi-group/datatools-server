package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.editor.datastore.FeedTx;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.transit.*;
import org.geotools.referencing.GeodeticCalculator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.HaltException;
import spark.Request;
import spark.Response;

import static spark.Spark.*;


public class StopController {

    public static final JsonManager<Stop> json =
            new JsonManager<>(Stop.class, JsonViews.UserInterface.class);
    private static final Logger LOG = LoggerFactory.getLogger(StopController.class);

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

        if (feedId == null) {
            halt(400);
        }

        final FeedTx tx = VersionedDataStore.getFeedTx(feedId);
 
        try {
              if (id != null) {
                if (!tx.stops.containsKey(id)) {
                    halt(404);
                }

                return tx.stops.get(id);
            }
              else if (Boolean.TRUE.equals(majorStops)) {
                  // retrieveById the major stops for the agency
                  Collection<Stop> stops = tx.majorStops.stream()
                          .map(tx.stops::get)
                          .collect(Collectors.toList());

                  return stops;
              }
            else if (west != null && east != null && south != null && north != null) {
                Collection<Stop> matchedStops = tx.getStopsWithinBoundingBox(north, east, south, west);
                return matchedStops;
            }
            else if (patternId != null) {
                if (!tx.tripPatterns.containsKey(patternId)) {
                    halt(404);
                }

                TripPattern p = tx.tripPatterns.get(patternId);

                Collection<Stop> ret = p.patternStops.stream()
                        .map(tx.stops::get)
                        .collect(Collectors.toList());

                return ret;
            }
            // return all
            else {
                  Collection<Stop> matchedStops = new ArrayList<>(tx.stops.values());
                  return matchedStops;
            }

        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
        } finally {
            if (tx != null) tx.rollbackIfOpen();
        }
        return null;
    }

    public static Object createStop(Request req, Response res) {
        FeedTx tx = null;
        try {
            Stop stop = Base.mapper.readValue(req.body(), Stop.class);
            
            if (req.session().attribute("feedId") != null && !req.session().attribute("feedId").equals(stop.feedId))
                halt(400);
            
            if (!VersionedDataStore.feedExists(stop.feedId)) {
                halt(400, "Stop must reference feed source ID");
            }
            
            tx = VersionedDataStore.getFeedTx(stop.feedId);
            
            if (tx.stops.containsKey(stop.id)) {
                halt(400);
            }
            
            tx.stops.put(stop.id, stop);
            tx.commit();
            return stop;
        } catch (IOException e) {
            e.printStackTrace();
            halt(400);
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (tx != null) tx.rollbackIfOpen();
        }
        return null;
    }

    public static Object getPatternsForStop(Request req, Response res) throws IOException {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");

        if (feedId == null)
            feedId = req.session().attribute("feedId");

        if (feedId == null) {
            halt(400);
        }

        FeedTx tx = null;
        try {
            tx = VersionedDataStore.getFeedTx(feedId);
            if (!tx.stops.containsKey(id)) {
                halt(404);
            }
            return tx.getTripPatternsByStop(id);
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (tx != null) tx.rollbackIfOpen();
        }
        return null;
    }


    public static Object updateStop(Request req, Response res) throws IOException {
        FeedTx tx = null;
        Stop stop = Base.mapper.readValue(req.body(), Stop.class);
        String feedId = req.queryParams("feedId");
        if (feedId == null) {
            halt(400, "Must provide feed ID");
        }

        if (!VersionedDataStore.feedExists(feedId)) {
            halt(400, "Feed ID ("+feedId+") does not exist");
        }
        try {
            tx = VersionedDataStore.getFeedTx(feedId);

            if (!tx.stops.containsKey(stop.id)) {
                halt(400);
            }

            tx.stops.put(stop.id, stop);
            tx.commit();
            return stop;
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (tx != null) tx.rollbackIfOpen();
        }
        return null;
    }

    public static Object deleteStop(Request req, Response res) throws IOException {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");

        if (feedId == null) {
            halt(400);
        }

        FeedTx tx = null;
        try {
            tx = VersionedDataStore.getFeedTx(feedId);
            if (!tx.stops.containsKey(id)) {
                halt(404);
            }

            if (!tx.getTripPatternsByStop(id).isEmpty()) {
                Set<String> patterns = tx.getTripPatternsByStop(id).stream()
                        .map(tripPattern -> tripPattern.name)
                        .collect(Collectors.toSet());
                Set<String> routes = tx.getTripPatternsByStop(id).stream()
                        .map(tripPattern -> tripPattern.routeId)
                        .collect(Collectors.toSet());
                halt(400, errorMessage("Trip patterns ("+patterns.toString()+") for routes "+routes.toString()+" reference stop ID" + id));
            }

            Stop s = tx.stops.remove(id);
            tx.commit();
            return s;
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (tx != null) tx.rollbackIfOpen();
        }
        return null;
    }

    private static String errorMessage(String s) {
        JSONObject json = new JSONObject();
        json.put("error", true);
        json.put("message", s);
        return json.toString();
    }


    public static Object findDuplicateStops(Request req, Response res) {
        String feedId = req.queryParams("feedId");

        if (feedId == null) {
            halt(400);
        }

        FeedTx tx = null;

        try {
            tx = VersionedDataStore.getFeedTx(feedId);

            List<List<Stop>> ret = new ArrayList<List<Stop>>();

            for (Stop stop : tx.stops.values()) {
                // find nearby stops, within 5m
                // at the equator, 1 degree is 111 km
                // everywhere else this will overestimate, which is why we have a distance check as well (below)
                double thresholdDegrees = 5 / 111000d;

                Collection<Stop> candidateStops = tx.getStopsWithinBoundingBox(
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
            return ret;
         } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
             e.printStackTrace();
             halt(400);
         }
        finally {
            if (tx != null) tx.rollbackIfOpen();
        }
        return null;
    }

    public static Object mergeStops(Request req, Response res) {
        List<String> mergedStopIds = Arrays.asList(req.queryParams("mergedStopIds").split(","));
        String feedId = req.queryParams("feedId");

        if (mergedStopIds.size() <= 1) {
            halt(400);
        }

        if (feedId == null) {
            halt(400);
        }

        FeedTx tx = null;

        try {
            tx = VersionedDataStore.getFeedTx(feedId);
            Stop.merge(mergedStopIds, tx);
            tx.commit();
            return true;
        } finally {
            if (tx != null) tx.rollbackIfOpen();
        }
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/stop/:id", StopController::getStop, json::write);
        get(apiPrefix + "secure/stop/:id/patterns", StopController::getPatternsForStop, json::write);
        options(apiPrefix + "secure/stop", (q, s) -> "");
        get(apiPrefix + "secure/stop", StopController::getStop, json::write);
        get(apiPrefix + "secure/stop/mergeStops", StopController::mergeStops, json::write);
        post(apiPrefix + "secure/stop", StopController::createStop, json::write);
        put(apiPrefix + "secure/stop/:id", StopController::updateStop, json::write);
        delete(apiPrefix + "secure/stop/:id", StopController::deleteStop, json::write);
    }
}
