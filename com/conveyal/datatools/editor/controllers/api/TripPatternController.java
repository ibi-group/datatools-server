package com.conveyal.datatools.editor.controllers.api;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.conveyal.datatools.editor.controllers.Application;
import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.controllers.Secure;
import com.conveyal.datatools.editor.controllers.Security;
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



    public static void getTripPattern(String id, String routeId, String agencyId) {
        if (agencyId == null)
            agencyId = session.get("agencyId");

        if (agencyId == null) {
            halt(400);
            return;
        }

        final AgencyTx tx = VersionedDataStore.getAgencyTx(agencyId);

        try {

            if(id != null) {
               if (!tx.tripPatterns.containsKey(id))
                   halt(404);
               else
                   renderJSON(Base.toJson(tx.tripPatterns.get(id), false));
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

                    renderJSON(Base.toJson(patts, false));
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
    }

    public static void createTripPattern() {
        TripPattern tripPattern;
        
        try {
            tripPattern = Base.mapper.readValue(params.get("body"), TripPattern.class);
            
            if (session.contains("agencyId") && !session.get("agencyId").equals(tripPattern.agencyId))
                halt(400);
            
            if (!VersionedDataStore.agencyExists(tripPattern.agencyId)) {
                halt(400);
                return;
            }
            
            AgencyTx tx = VersionedDataStore.getAgencyTx(tripPattern.agencyId);
            
            if (tx.tripPatterns.containsKey(tripPattern.id)) {
                tx.rollback();
                halt(400);
            }
            
            tripPattern.calcShapeDistTraveled();
            
            tx.tripPatterns.put(tripPattern.id, tripPattern);
            tx.commit();

            renderJSON(Base.toJson(tripPattern, false));
        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
        }
    }

    public static void updateTripPattern() {
        TripPattern tripPattern;
        AgencyTx tx = null;
        try {
            tripPattern = Base.mapper.readValue(params.get("body"), TripPattern.class);
            
            if (session.contains("agencyId") && !session.get("agencyId").equals(tripPattern.agencyId))
                halt(400);
            
            if (!VersionedDataStore.agencyExists(tripPattern.agencyId)) {
                halt(400);
                return;
            }
            
            if (tripPattern.id == null) {
                halt(400);
                return;
            }
            
            tx = VersionedDataStore.getAgencyTx(tripPattern.agencyId);
            
            TripPattern originalTripPattern = tx.tripPatterns.get(tripPattern.id);
            
            if(originalTripPattern == null) {
                tx.rollback();
                halt(400);
                return;
            }
                        
            // update stop times
            try {
                TripPattern.reconcilePatternStops(originalTripPattern, tripPattern, tx);
            } catch (IllegalStateException e) {
                tx.rollback();
                halt(400);
                return;
            }
            
            tripPattern.calcShapeDistTraveled();
            
            tx.tripPatterns.put(tripPattern.id, tripPattern);
            tx.commit();

            renderJSON(Base.toJson(tripPattern, false));
        } catch (Exception e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
            halt(400);
        }
    }

    public static void deleteTripPattern(String id, String agencyId) {
        if (agencyId == null)
            agencyId = session.get("agencyId");

        if(id == null || agencyId == null) {
            halt(400);
            return;
        }

        AgencyTx tx = VersionedDataStore.getAgencyTx(agencyId);

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
    }
}
