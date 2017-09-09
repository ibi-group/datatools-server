package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.editor.models.transit.StatusType;
import com.conveyal.datatools.common.utils.S3Utils;
import com.conveyal.datatools.editor.datastore.FeedTx;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.transit.Route;
import com.conveyal.datatools.editor.models.transit.Trip;
import com.conveyal.datatools.editor.models.transit.TripPattern;
import org.mapdb.Fun;
import org.mapdb.Fun.Tuple2;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.HaltException;
import spark.Request;
import spark.Response;

import static spark.Spark.*;


public class RouteController {
    public static final JsonManager<Route> json =
            new JsonManager<>(Route.class, JsonViews.UserInterface.class);
    private static final Logger LOG = LoggerFactory.getLogger(RouteController.class);

    public static Object getRoute(Request req, Response res) {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");

        if (feedId == null) {
            halt(400);
        }

        FeedTx tx = null;

        try {
            tx = VersionedDataStore.getFeedTx(feedId);
            if (id != null) {
                if (!tx.routes.containsKey(id)) {
                    halt(400);
                }

                Route route = tx.routes.get(id);
                route.addDerivedInfo(tx);

                return route;
            }
            else {
                // put values into a new ArrayList to avoid returning MapDB BTreeMap
                // (and possible access error once transaction is closed)
                Set<Route> ret = new HashSet<>(tx.routes.values());

                for (Route r : ret) {
                    r.addDerivedInfo(tx);
                }
                return ret;
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

    public static Route createRoute(Request req, Response res) {
        Route route;
        FeedTx tx = null;
        String feedId = req.queryParams("feedId");

        if (feedId == null) {
            halt(400);
        }

        try {
            route = Base.mapper.readValue(req.body(), Route.class);
            
            if (req.session().attribute("feedId") != null && !req.session().attribute("feedId").equals(route.feedId))
                halt(400);
   
            tx = VersionedDataStore.getFeedTx(feedId);
            
            if (tx.routes.containsKey(route.id)) {
                halt(400, "Failed to create route with duplicate id");
            }

            // check if gtfsRouteId is specified, if not create from DB id
            if(route.gtfsRouteId == null) {
                route.gtfsRouteId = "ROUTE_" + route.id;
            }
            
            tx.routes.put(route.id, route);
            tx.commit();

            return route;
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

    public static Route updateRoute(Request req, Response res) {
        Route route;
        String id = req.params("id");
        String feedId = req.queryParams("feedId");

        FeedTx tx = null;
        try {
            route = Base.mapper.readValue(req.body(), Route.class);
            if (feedId == null) {
                halt(400);
            }
            tx = VersionedDataStore.getFeedTx(feedId);
            
            if (!tx.routes.containsKey(id)) {
                halt(404);
            }

            Route oldRoute = tx.routes.get(id);

            // if admin-only fields have changed, double check (client should limit this too)
            // that the user has permission to do so
            // TODO: notify subscribers if status has changed to PENDING_APPROVAL?
            if (route.publiclyVisible != oldRoute.publiclyVisible ||
                    (route.status != oldRoute.status &&
                            (route.status.equals(StatusType.APPROVED) ||
                                    oldRoute.status.equals(StatusType.APPROVED)))) {
                FeedSource feedSource = Persistence.feedSources.getById(feedId);
                Auth0UserProfile userProfile = req.attribute("user");

                if (!userProfile.canApproveGTFS(feedSource.organizationId(), feedSource.projectId, feedId)) {
                    halt(403, SparkUtils.formatJSON("User does not have permission to change status of route", 403));
                }
            }

            // check if gtfsRouteId is specified, if not create from DB id
            if(route.gtfsRouteId == null) {
                route.gtfsRouteId = "ROUTE_" + id;
            }
            
            tx.routes.put(id, route);
            tx.commit();

            return route;
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

    public static Route uploadRouteBranding(Request req, Response res) {
        Route route;
        String id = req.params("id");
        String feedId = req.queryParams("feedId");

        FeedTx tx = null;

        try {
            if (feedId == null) {
                halt(400);
            }

            tx = VersionedDataStore.getFeedTx(feedId);

            if (!tx.routes.containsKey(id)) {
                halt(404);
            }

            route = tx.routes.get(id);

            String url = S3Utils.uploadBranding(req, id);

            // set routeBrandingUrl to s3 location
            route.routeBrandingUrl = url;

            tx.routes.put(id, route);
            tx.commit();

            return route;
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

    public static Route deleteRoute(Request req, Response res) {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");

        if (feedId == null)
            feedId = req.session().attribute("feedId");

        if(id == null || feedId == null)
            halt(400);

        FeedTx tx = null;
        
        try {
            tx = VersionedDataStore.getFeedTx(feedId);

            if (!tx.routes.containsKey(id)) {
                halt(404);
            }
            
            Route r = tx.routes.get(id);

            // delete affected trips
            Set<Tuple2<String, String>> affectedTrips = tx.tripsByRoute.subSet(new Tuple2(r.id, null), new Tuple2(r.id, Fun.HI));
            for (Tuple2<String, String> trip : affectedTrips) {
                tx.trips.remove(trip.b);
            }
            
            // delete affected patterns
            // note that all the trips on the patterns will have already been deleted above
            Set<Tuple2<String, String>> affectedPatts = tx.tripPatternsByRoute.subSet(new Tuple2(r.id, null), new Tuple2(r.id, Fun.HI));
            for (Tuple2<String, String> tp : affectedPatts) {
                tx.tripPatterns.remove(tp.b);
            }
            
            tx.routes.remove(id);
            tx.commit();
            return r; // ok();
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            halt(404, e.getMessage());
        } finally {
            if (tx != null) tx.rollbackIfOpen();
        }
        return null;
    }
    
    /** merge route from into route into, for the given agency ID */
    public static Object mergeRoutes (Request req, Response res) {
        String from = req.queryParams("from");
        String into = req.queryParams("into");

        String feedId = req.queryParams("feedId");

        if (feedId == null)
            feedId = req.session().attribute("feedId");

        if (feedId == null || from == null || into == null)
            halt(400);

        final FeedTx tx = VersionedDataStore.getFeedTx(feedId);

        try {
            // ensure the routes exist
            if (!tx.routes.containsKey(from) || !tx.routes.containsKey(into)) {
                halt(400);
            }

            // retrieveById all the trip patterns for route from
            // note that we clone them here so we can later modify them
            Collection<TripPattern> tps = Collections2.transform(
                    tx.tripPatternsByRoute.subSet(new Tuple2(from, null), new Tuple2(from, Fun.HI)),
                    // NOTE: this function cannot be replace with lambda due to type issues
                    new Function<Tuple2<String, String>, TripPattern>() {
                        @Override
                        public TripPattern apply(Tuple2<String, String> input) {
                            try {
                                return tx.tripPatterns.get(input.b).clone();
                            } catch (CloneNotSupportedException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                                throw new RuntimeException(e);
                            }
                        }
                    });

             for (TripPattern tp : tps) {
                 tp.routeId = into;
                 tx.tripPatterns.put(tp.id, tp);
             }

             // now move all the trips
             Collection<Trip> ts = Collections2.transform(
                     tx.tripsByRoute.subSet(new Tuple2(from, null), new Tuple2(from, Fun.HI)),
                     // NOTE: this function cannot be replace with lambda due to type issues
                     new Function<Tuple2<String, String>, Trip>() {
                         @Override
                         public Trip apply(Tuple2<String, String> input) {
                             try {
                                return tx.trips.get(input.b).clone();
                            } catch (CloneNotSupportedException e) {
                                e.printStackTrace();
                                throw new RuntimeException(e);
                            }
                         }
                     });

             for (Trip t : ts) {
                 t.routeId = into;
                 tx.trips.put(t.id, t);
             }

             tx.routes.remove(from);

             tx.commit();
             return true; // ok();
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (tx != null) tx.rollbackIfOpen();
        }
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/route/:id", RouteController::getRoute, json::write);
        options(apiPrefix + "secure/route", (q, s) -> "");
        get(apiPrefix + "secure/route", RouteController::getRoute, json::write);
        post(apiPrefix + "secure/route/merge", RouteController::mergeRoutes, json::write);
        post(apiPrefix + "secure/route", RouteController::createRoute, json::write);
        put(apiPrefix + "secure/route/:id", RouteController::updateRoute, json::write);
        post(apiPrefix + "secure/route/:id/uploadbranding", RouteController::uploadRouteBranding, json::write);
        delete(apiPrefix + "secure/route/:id", RouteController::deleteRoute, json::write);
    }
}
