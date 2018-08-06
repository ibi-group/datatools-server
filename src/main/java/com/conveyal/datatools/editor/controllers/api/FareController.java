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

import java.util.ArrayList;
import java.util.Collection;

import static com.conveyal.datatools.common.utils.SparkUtils.haltWithMessage;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.options;
import static spark.Spark.post;
import static spark.Spark.put;

/**
 * Created by landon on 6/22/16.
 */
public class FareController {
    public static final JsonManager<Fare> json =
            new JsonManager<>(Fare.class, JsonViews.UserInterface.class);
    private static final Logger LOG = LoggerFactory.getLogger(FareController.class);

    public static Object getFare(Request req, Response res) {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");

        if (feedId == null) {
            feedId = req.session().attribute("feedId");
        }

        if (feedId == null) {
            haltWithMessage(req, 400, "feedId is required");
        }

        FeedTx tx = null;

        try {
            tx = VersionedDataStore.getFeedTx(feedId);
            if (id != null) {
                if (!tx.fares.containsKey(id)) {
                    haltWithMessage(req, 404, "fare not found in database");
                }

                else {
                    Fare fare = tx.fares.get(id);
                    return fare;
                }
            }
            else {
                // put values into a new ArrayList to avoid returning MapDB BTreeMap
                // (and possible access error once transaction is closed)
                Collection<Fare> fares = new ArrayList<>(tx.fares.values());
                return fares;
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

    public static Object createFare(Request req, Response res) {
        Fare fare;
        FeedTx tx = null;

        try {
            fare = Base.mapper.readValue(req.body(), Fare.class);

            if (!VersionedDataStore.feedExists(fare.feedId)) {
                haltWithMessage(req, 400, "feed does not exist");
            }

            tx = VersionedDataStore.getFeedTx(fare.feedId);

            if (tx.fares.containsKey(fare.id)) {
                haltWithMessage(req, 400, "a fare with this id already exists in the database");
            }

            // check if gtfsFareId is specified, if not create from DB id
            if(fare.gtfsFareId == null) {
                fare.gtfsFareId = "CAL_" + fare.id.toString();
            }

            tx.fares.put(fare.id, fare);
            tx.commit();

            return fare;
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

    public static Fare updateFare(Request req, Response res) {
        Fare fare;
        FeedTx tx = null;

        try {
            fare = Base.mapper.readValue(req.body(), Fare.class);

            if (!VersionedDataStore.feedExists(fare.feedId)) {
                haltWithMessage(req, 400, "feed does not exist");
            }

            tx = VersionedDataStore.getFeedTx(fare.feedId);
            if (!tx.fares.containsKey(fare.id)) {
                haltWithMessage(req, 404, "fare not found in database");
            }

            // check if gtfsFareId is specified, if not create from DB id
            if(fare.gtfsFareId == null) {
                fare.gtfsFareId = "CAL_" + fare.id.toString();
            }

            tx.fares.put(fare.id, fare);
            tx.commit();
            return fare;
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

    public static Object deleteFare(Request req, Response res) {
        Fare fare;
        String id = req.params("id");
        String feedId = req.queryParams("feedId");

        FeedTx tx = null;

        try {
            tx = VersionedDataStore.getFeedTx(feedId);

            if (id == null || !tx.fares.containsKey(id)) {
                tx.rollback();
                haltWithMessage(req, 404, "fare not found in database");
            }
            fare = tx.fares.get(id);
            tx.fares.remove(id);
            tx.commit();

            return fare;
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            if (tx != null) tx.rollbackIfOpen();
            e.printStackTrace();
            haltWithMessage(req, 500, "an unexpected error occurred", e);
        } finally {
            if (tx != null) tx.rollbackIfOpen();
        }
        return null;
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
