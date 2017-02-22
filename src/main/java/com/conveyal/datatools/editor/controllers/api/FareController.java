package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.datastore.FeedTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.transit.Fare;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.HaltException;
import spark.Request;
import spark.Response;

import java.util.Calendar;
import java.util.Collection;

import static spark.Spark.*;
import static spark.Spark.delete;

/**
 * Created by landon on 6/22/16.
 */
public class FareController {
    public static JsonManager<Calendar> json =
            new JsonManager<>(Calendar.class, JsonViews.UserInterface.class);
    private static final Logger LOG = LoggerFactory.getLogger(FareController.class);
    public static Object getFare(Request req, Response res) {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");

        if (feedId == null) {
            feedId = req.session().attribute("feedId");
        }

        if (feedId == null) {
            halt(400);
        }

        final FeedTx tx = VersionedDataStore.getFeedTx(feedId);

        try {
            if (id != null) {
                if (!tx.fares.containsKey(id)) {
                    halt(404);
                    tx.rollback();
                }

                else {
                    Fare fare = tx.fares.get(id);
//                    fare.addDerivedInfo(tx);
                    return fare;
                }
            }
            else {
                Collection<Fare> fares = tx.fares.values();
                for (Fare fare : fares) {
//                    fare.addDerivedInfo(tx);
                }
                return fares;
            }

            tx.rollback();
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            tx.rollback();
            e.printStackTrace();
            halt(400);
        }
        return null;
    }

    public static Object createFare(Request req, Response res) {
        Fare fare;
        FeedTx tx = null;

        try {
            fare = Base.mapper.readValue(req.body(), Fare.class);

            if (!VersionedDataStore.feedExists(fare.feedId)) {
                halt(400);
            }

            if (req.session().attribute("feedId") != null && !req.session().attribute("feedId").equals(fare.feedId))
                halt(400);

            tx = VersionedDataStore.getFeedTx(fare.feedId);

            if (tx.fares.containsKey(fare.id)) {
                tx.rollback();
                halt(400);
            }

            // check if gtfsFareId is specified, if not create from DB id
            if(fare.gtfsFareId == null) {
                fare.gtfsFareId = "CAL_" + fare.id.toString();
            }

//            fare.addDerivedInfo(tx);

            tx.fares.put(fare.id, fare);
            tx.commit();

            return fare;
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
        }
        return null;
    }

    public static Object updateFare(Request req, Response res) {
        Fare fare;
        FeedTx tx = null;

        try {
            fare = Base.mapper.readValue(req.body(), Fare.class);

            if (!VersionedDataStore.feedExists(fare.feedId)) {
                halt(400);
            }

            if (req.session().attribute("feedId") != null && !req.session().attribute("feedId").equals(fare.feedId))
                halt(400);

            tx = VersionedDataStore.getFeedTx(fare.feedId);

            if (!tx.fares.containsKey(fare.id)) {
                tx.rollback();
                halt(400);
            }

            // check if gtfsFareId is specified, if not create from DB id
            if(fare.gtfsFareId == null) {
                fare.gtfsFareId = "CAL_" + fare.id.toString();
            }

//            fare.addDerivedInfo(tx);

            tx.fares.put(fare.id, fare);

            Object json = Base.toJson(fare, false);

            tx.commit();

            return json;
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
            halt(400);
        }
        return null;
    }

    public static Object deleteFare(Request req, Response res) {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");

        FeedTx tx = VersionedDataStore.getFeedTx(feedId);

        if (id == null || !tx.fares.containsKey(id)) {
            tx.rollback();
            halt(404);
        }

        tx.fares.remove(id);

        tx.commit();

        return true; // ok();
    }
    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/fare/:id", FareController::getFare, json::write);
        options(apiPrefix + "secure/fare", (q, s) -> "");
        get(apiPrefix + "secure/fare", FareController::getFare, json::write);
        post(apiPrefix + "secure/fare", FareController::createFare, json::write);
        put(apiPrefix + "secure/fare/:id", FareController::updateFare, json::write);
        delete(apiPrefix + "secure/fare/:id", FareController::deleteFare, json::write);
    }
}
