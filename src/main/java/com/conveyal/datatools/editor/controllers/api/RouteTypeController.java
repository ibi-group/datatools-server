package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.datastore.GlobalTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.transit.RouteType;

import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import spark.Request;
import spark.Response;

import static com.conveyal.datatools.common.utils.SparkUtils.haltWithMessage;
import static spark.Spark.*;


public class RouteTypeController {
    public static JsonManager<RouteType> json =
            new JsonManager<>(RouteType.class, JsonViews.UserInterface.class);
    public static Object getRouteType(Request req, Response res) {
        String id = req.params("id");
        Object json = null;
        try {
            GlobalTx tx = VersionedDataStore.getGlobalTx();

            if(id != null) {
                if(tx.routeTypes.containsKey(id))
                    json = Base.toJson(tx.routeTypes.get(id), false);
                else
                    haltWithMessage(req, 404, "route type not found in database");

                tx.rollback();
            }
            else {
                json = Base.toJson(tx.routeTypes.values(), false);
                tx.rollback();
            }
        } catch (Exception e) {
            e.printStackTrace();
            haltWithMessage(req, 500, "an unexpected error occurred", e);
        }
        return json;
    }

    public static Object createRouteType(Request req, Response res) {
        RouteType routeType;

        try {
            routeType = Base.mapper.readValue(req.body(), RouteType.class);

            GlobalTx tx = VersionedDataStore.getGlobalTx();

            if (tx.routeTypes.containsKey(routeType.id)) {
                tx.rollback();
                haltWithMessage(req, 400, "a route type with this id already exists in the database");
            }

            tx.routeTypes.put(routeType.id, routeType);
            tx.commit();

            return routeType;
        } catch (Exception e) {
            e.printStackTrace();
            haltWithMessage(req, 500, "an unexpected error occurred", e);
        }
        return null;
    }


    public static Object updateRouteType(Request req, Response res) {
        RouteType routeType;

        try {
            routeType = Base.mapper.readValue(req.body(), RouteType.class);

            if(routeType.id == null) {
                haltWithMessage(req, 400, "id is required in route type");
            }

            GlobalTx tx = VersionedDataStore.getGlobalTx();
            if (!tx.routeTypes.containsKey(routeType.id)) {
                tx.rollback();
                haltWithMessage(req, 404, "route type not found in database");
            }

            tx.routeTypes.put(routeType.id, routeType);
            tx.commit();

            return routeType;
        } catch (Exception e) {
            e.printStackTrace();
            haltWithMessage(req, 500, "an unexpected error occurred", e);
        }
        return null;
    }

    // TODO: cascaded delete, etc.
    public static Object deleteRouteType(Request req, Response res) {
        String id = req.params("id");
        if (id == null)
            haltWithMessage(req, 400, "id is required");

        GlobalTx tx = VersionedDataStore.getGlobalTx();

        if (!tx.routeTypes.containsKey(id)) {
            tx.rollback();
            haltWithMessage(req, 400, "route type not found in database");
        }

        tx.routeTypes.remove(id);
        tx.commit();

        return true; // ok();
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/routetype/:id", RouteTypeController::getRouteType, json::write);
        options(apiPrefix + "secure/routetype", (q, s) -> "");
        get(apiPrefix + "secure/routetype", RouteTypeController::getRouteType, json::write);
        post(apiPrefix + "secure/routetype", RouteTypeController::createRouteType, json::write);
        put(apiPrefix + "secure/routetype/:id", RouteTypeController::updateRouteType, json::write);
        delete(apiPrefix + "secure/routetype/:id", RouteTypeController::deleteRouteType, json::write);
    }
}
