package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.editor.controllers.Application;
import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.controllers.Secure;
import com.conveyal.datatools.editor.controllers.Security;
import com.conveyal.datatools.editor.datastore.GlobalTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.transit.RouteType;

import spark.Request;
import spark.Response;

import static spark.Spark.*;


public class RouteTypeController {



    public static void getRouteType(String id) {
        try {
            GlobalTx tx = VersionedDataStore.getGlobalTx();

            if(id != null) {
                if(tx.routeTypes.containsKey(id))
                    renderJSON(Base.toJson(tx.routeTypes.get(id), false));
                else
                    halt(404);

                tx.rollback();
            }
            else {
                renderJSON(Base.toJson(tx.routeTypes.values(), false));
                tx.rollback();
            }
        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
        }

    }

    public static void createRouteType() {
        RouteType routeType;

        try {
            routeType = Base.mapper.readValue(params.get("body"), RouteType.class);

            GlobalTx tx = VersionedDataStore.getGlobalTx();

            if (tx.routeTypes.containsKey(routeType.id)) {
                tx.rollback();
                halt(400);
                return;
            }

            tx.routeTypes.put(routeType.id, routeType);
            tx.commit();

            renderJSON(Base.toJson(routeType, false));
        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
        }
    }


    public static void updateRouteType() {
        RouteType routeType;

        try {
            routeType = Base.mapper.readValue(params.get("body"), RouteType.class);

            if(routeType.id == null) {
                halt(400);
                return;
            }

            GlobalTx tx = VersionedDataStore.getGlobalTx();
            if (!tx.routeTypes.containsKey(routeType.id)) {
                tx.rollback();
                halt(404);
                return;
            }

            tx.routeTypes.put(routeType.id, routeType);
            tx.commit();

            renderJSON(Base.toJson(routeType, false));
        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
        }
    }

    // TODO: cascaded delete, etc.
    public static void deleteRouteType(String id) {
        if (id == null)
            halt(400);

        GlobalTx tx = VersionedDataStore.getGlobalTx();

        if (!tx.routeTypes.containsKey(id)) {
            tx.rollback();
            halt(400);
            return;
        }

        tx.routeTypes.remove(id);
        tx.commit();

        return true; // ok();
    }

}
