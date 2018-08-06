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
import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.HaltException;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.conveyal.datatools.common.utils.SparkUtils.haltWithMessage;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.options;
import static spark.Spark.post;
import static spark.Spark.put;


public class TripController {
    public static final Logger LOG = LoggerFactory.getLogger(TripController.class);

    public static final JsonManager<Trip> json =
            new JsonManager<>(Trip.class, JsonViews.UserInterface.class);

    private static Object getTrip(Request req, Response res) {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");
        String patternId = req.queryParams("patternId");
        String calendarId = req.queryParams("calendarId");

        if (feedId == null) {
            haltWithMessage(req, 400, "feedId is required");
        }

        FeedTx tx = null;

        try {
            tx = VersionedDataStore.getFeedTx(feedId);
            if (id != null) {
                if (tx.trips.containsKey(id))
                    return Base.toJson(tx.trips.get(id), false);
                else
                    haltWithMessage(req,404, "trip not found in database");
            }
            else if (patternId != null && calendarId != null) {
                if (!tx.tripPatterns.containsKey(patternId) || !tx.calendars.containsKey(calendarId)) {
                    haltWithMessage(req, 404, "pattern or calendar not found in database");
                }
                else {
                    LOG.info("requesting trips for pattern/cal");
                    return tx.getTripsByPatternAndCalendar(patternId, calendarId);
                }
            }

            else if(patternId != null) {
                return tx.getTripsByPattern(patternId);
            }
            else {
                return new ArrayList<>(tx.trips.values());
            }
                
        } catch (IOException e) {
            e.printStackTrace();
            haltWithMessage(req, 400, "an io exception happened", e);
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            haltWithMessage(req, 500, "an unexpected error occurred", e);
        } finally {
            if (tx != null) tx.rollbackIfOpen();
        }
        return null;
    }

    private static Object createTrip(Request req, Response res) {
        FeedTx tx = null;
        String createMultiple = req.queryParams("multiple");
        try {
            List<Trip> trips = new ArrayList<>();
            if (createMultiple != null && createMultiple.equals("true")) {
                trips = Base.mapper.readValue(req.body(), new TypeReference<List<Trip>>(){});
            } else {
                Trip trip = Base.mapper.readValue(req.body(), Trip.class);
                trips.add(trip);
            }
            for (Trip trip : trips) {
                if (req.session().attribute("feedId") != null && !req.session().attribute("feedId").equals(trip.feedId))
                    haltWithMessage(req, 400, "feedId does not match");

                if (!VersionedDataStore.feedExists(trip.feedId)) {
                    haltWithMessage(req, 400, "feed does not exist");
                }

                tx = VersionedDataStore.getFeedTx(trip.feedId);

                String errorMessage;
                if (tx.trips.containsKey(trip.id)) {
                    errorMessage = "Trip ID " + trip.id + " already exists.";
                    LOG.error(errorMessage);
                    haltWithMessage(req, 400, errorMessage);

                }
                validateTrip(req, tx, trip);
                tx.trips.put(trip.id, trip);
            }
            tx.commit();

            return trips;
        } catch (IOException e) {
            e.printStackTrace();
            haltWithMessage(req, 400, "an io exception happened", e);
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            haltWithMessage(req, 500, "an unexpected error occurred", e);
        } finally {
            if (tx != null) tx.rollbackIfOpen();
        }
        return null;
    }
    
    private static Object updateTrip(Request req, Response res) {
        FeedTx tx = null;

        try {
            Trip trip = Base.mapper.readValue(req.body(), Trip.class);

            if (!VersionedDataStore.feedExists(trip.feedId)) {
                haltWithMessage(req, 400, "feed does not exist");
            }

            tx = VersionedDataStore.getFeedTx(trip.feedId);
            validateTrip(req, tx, trip);
            tx.trips.put(trip.id, trip);
            tx.commit();

            return trip;
        } catch (IOException e) {
            e.printStackTrace();
            haltWithMessage(req, 400, "Unknown IO error occurred saving trip");
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            haltWithMessage(req, 500, "an unexpected error occurred", e);
        } finally {
            if (tx != null) tx.rollbackIfOpen();
        }
        return null;
    }

    /**
     * Validates that a saved trip will not cause issues with referenced pattern primarily due to
     * mismatched stops.
     */
    private static void validateTrip(Request req, FeedTx tx, Trip trip) {
        TripPattern patt;
        String errorMessage;
        // Confirm that referenced pattern ID exists
        if (!tx.tripPatterns.containsKey(trip.patternId)) {
            errorMessage = "Pattern ID " + trip.patternId + " does not exist.";
            LOG.error(errorMessage);
            haltWithMessage(req, 400, errorMessage);
            throw new IllegalStateException("Cannot create/update trip for pattern that does not exist");
        } else {
            patt = tx.tripPatterns.get(trip.patternId);
        }
        // Confirm that # of stops in trip and pattern match.
        if (trip.stopTimes.size() != patt.patternStops.size()) {
            errorMessage = String.format(
                    "Number of stops in trip %d does not equal number of stops in pattern (%d)",
                    trip.stopTimes.size(),
                    patt.patternStops.size()
            );
            LOG.error(errorMessage);
            haltWithMessage(req, 400, errorMessage);
        }
        // Confirm that each stop ID in the trip matches the stop ID in the pattern.
        for (int i = 0; i < trip.stopTimes.size(); i++) {
            TripPatternStop ps = patt.patternStops.get(i);
            StopTime st =  trip.stopTimes.get(i);

            if (st == null)
                // null StopTime for a trip indicates a skipped stop
                continue;

            if (!st.stopId.equals(ps.stopId)) {
                errorMessage = String.format(
                        "Mismatch between stop sequence in trip and pattern at position %d, pattern: %s, stop: %s",
                        i,
                        ps.stopId,
                        st.stopId
                );
                LOG.error(errorMessage);
                haltWithMessage(req, 400, errorMessage);
            }
        }
    }

    private static Object deleteTrip(Request req, Response res) {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");
        String[] idList = req.queryParams("tripIds").split(",");

        if (feedId == null) {
            haltWithMessage(req, 400, "Must provide feedId");
        }

        FeedTx tx = null;
        try {
            tx = VersionedDataStore.getFeedTx(feedId);

            // for a single trip
            if (id != null) {
                Trip trip = tx.trips.remove(id);
                return trip;
            }
            if (idList.length > 0) {
                Set<Trip> trips = new HashSet<>();
                for (String tripId : idList) {
                    Trip trip = tx.trips.remove(tripId);
                    trips.add(trip);
                }
                tx.commit();
                return trips;
            }

        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            haltWithMessage(req, 500, "an unexpected error occurred", e);
        } finally {
            if (tx != null) tx.rollbackIfOpen();
        }
        return null;
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/trip/:id", TripController::getTrip, json::write);
        options(apiPrefix + "secure/trip", (q, s) -> "");
        get(apiPrefix + "secure/trip", TripController::getTrip, json::write);
        post(apiPrefix + "secure/trip", TripController::createTrip, json::write);
        put(apiPrefix + "secure/trip/:id", TripController::updateTrip, json::write);
        delete(apiPrefix + "secure/trip", TripController::deleteTrip, json::write);
        delete(apiPrefix + "secure/trip/:id", TripController::deleteTrip, json::write);
    }
}
