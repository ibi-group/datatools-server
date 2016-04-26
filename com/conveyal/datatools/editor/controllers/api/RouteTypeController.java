package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.editor.controllers.Application;
import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.controllers.Secure;
import com.conveyal.datatools.editor.controllers.Security;
import com.conveyal.datatools.editor.datastore.GlobalTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.transit.RouteType;

import static spark.Spark.*;

@With(Secure.class)
public class RouteTypeController extends Controller {
    @Before
    static void initSession() throws Throwable {

        if(!Security.isConnected() && !Application.checkOAuth(request, session))
            Secure.login("");
    }

    public static void getRouteType(String id) {
        try {
            GlobalTx tx = VersionedDataStore.getGlobalTx();

            if(id != null) {
                if(tx.routeTypes.containsKey(id))
                    renderJSON(Base.toJson(tx.routeTypes.get(id), false));
                else
                    notFound();

                tx.rollback();
            }
            else {
                renderJSON(Base.toJson(tx.routeTypes.values(), false));
                tx.rollback();
            }
        } catch (Exception e) {
            e.printStackTrace();
            badRequest();
        }

    }

    public static void createRouteType() {
        RouteType routeType;

        try {
            routeType = Base.mapper.readValue(params.get("body"), RouteType.class);

            GlobalTx tx = VersionedDataStore.getGlobalTx();

            if (tx.routeTypes.containsKey(routeType.id)) {
                tx.rollback();
                badRequest();
                return;
            }

            tx.routeTypes.put(routeType.id, routeType);
            tx.commit();

            renderJSON(Base.toJson(routeType, false));
        } catch (Exception e) {
            e.printStackTrace();
            badRequest();
        }
    }


    public static void updateRouteType() {
        RouteType routeType;

        try {
            routeType = Base.mapper.readValue(params.get("body"), RouteType.class);

            if(routeType.id == null) {
                badRequest();
                return;
            }

            GlobalTx tx = VersionedDataStore.getGlobalTx();
            if (!tx.routeTypes.containsKey(routeType.id)) {
                tx.rollback();
                notFound();
                return;
            }

            tx.routeTypes.put(routeType.id, routeType);
            tx.commit();

            renderJSON(Base.toJson(routeType, false));
        } catch (Exception e) {
            e.printStackTrace();
            badRequest();
        }
    }

    // TODO: cascaded delete, etc.
    public static void deleteRouteType(String id) {
        if (id == null)
            badRequest();

        GlobalTx tx = VersionedDataStore.getGlobalTx();

        if (!tx.routeTypes.containsKey(id)) {
            tx.rollback();
            badRequest();
            return;
        }

        tx.routeTypes.remove(id);
        tx.commit();

        ok();
    }

}
