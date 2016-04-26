package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.editor.controllers.Application;
import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.controllers.Secure;
import com.conveyal.datatools.editor.controllers.Security;
import com.conveyal.datatools.editor.datastore.AgencyTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.transit.StopTime;
import com.conveyal.datatools.editor.models.transit.Trip;
import com.conveyal.datatools.editor.models.transit.TripPattern;
import com.conveyal.datatools.editor.models.transit.TripPatternStop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static spark.Spark.*;

@With(Secure.class)
public class TripController extends Controller {
    public static final Logger LOG = LoggerFactory.getLogger(TripController.class);
    @Before
    static void initSession() throws Throwable {

        if(!Security.isConnected() && !Application.checkOAuth(request, session))
            Secure.login("");
    }

    public static void getTrip(String id, String patternId, String calendarId, String agencyId) {
        if (agencyId == null)
            agencyId = session.get("agencyId");

        if (agencyId == null) {
            badRequest();
            return;
        }

        AgencyTx tx = VersionedDataStore.getAgencyTx(agencyId);

        try {
            if (id != null) {
                if (tx.trips.containsKey(id))
                    renderJSON(Base.toJson(tx.trips.get(id), false));
                else
                    notFound();
            }
            else if (patternId != null && calendarId != null) {
                if (!tx.tripPatterns.containsKey(patternId) || !tx.calendars.containsKey(calendarId)) {
                    notFound();
                }
                else {
                    renderJSON(Base.toJson(tx.getTripsByPatternAndCalendar(patternId, calendarId), false));
                }
            }

            else if(patternId != null) {
                renderJSON(Base.toJson(tx.getTripsByPattern(patternId), false));
            }
            else {
                renderJSON(Base.toJson(tx.trips.values(), false));
            }
                
        } catch (Exception e) {
            tx.rollback();
            e.printStackTrace();
            badRequest();
        }

    }
    
    public static void createTrip() {
        AgencyTx tx = null;

        try {
            Trip trip = Base.mapper.readValue(params.get("body"), Trip.class);

            if (session.contains("agencyId") && !session.get("agencyId").equals(trip.agencyId))
                badRequest();

            if (!VersionedDataStore.agencyExists(trip.agencyId)) {
                badRequest();
                return;
            }

            tx = VersionedDataStore.getAgencyTx(trip.agencyId);

            if (tx.trips.containsKey(trip.id)) {
                tx.rollback();
                badRequest();
                return;
            }

            if (!tx.tripPatterns.containsKey(trip.patternId) || trip.stopTimes.size() != tx.tripPatterns.get(trip.patternId).patternStops.size()) {
                tx.rollback();
                badRequest();
                return;
            }

            tx.trips.put(trip.id, trip);
            tx.commit();

            renderJSON(Base.toJson(trip, false));
        } catch (Exception e) {
            e.printStackTrace();
            if (tx != null) tx.rollbackIfOpen();
            badRequest();
        }
    }
    
    public static void updateTrip() {
        AgencyTx tx = null;

        try {
            Trip trip = Base.mapper.readValue(params.get("body"), Trip.class);

            if (session.contains("agencyId") && !session.get("agencyId").equals(trip.agencyId))
                badRequest();

            if (!VersionedDataStore.agencyExists(trip.agencyId)) {
                badRequest();
                return;
            }

            tx = VersionedDataStore.getAgencyTx(trip.agencyId);

            if (!tx.trips.containsKey(trip.id)) {
                tx.rollback();
                badRequest();
                return;
            }

            if (!tx.tripPatterns.containsKey(trip.patternId) || trip.stopTimes.size() != tx.tripPatterns.get(trip.patternId).patternStops.size()) {
                tx.rollback();
                badRequest();
                return;
            }

            TripPattern patt = tx.tripPatterns.get(trip.patternId);

            // confirm that each stop in the trip matches the stop in the pattern

            for (int i = 0; i < trip.stopTimes.size(); i++) {
                TripPatternStop ps = patt.patternStops.get(i);
                StopTime st =  trip.stopTimes.get(i);

                if (st == null)
                    // skipped stop
                    continue;

                if (!st.stopId.equals(ps.stopId)) {
                    LOG.error("Mismatch between stop sequence in trip and pattern at position %s, pattern: %s, stop: %s", i, ps.stopId, st.stopId);
                    tx.rollback();
                    badRequest();
                    return;
                }
            }

            tx.trips.put(trip.id, trip);
            tx.commit();

            renderJSON(Base.toJson(trip, false));
        } catch (Exception e) {
            if (tx != null) tx.rollbackIfOpen();
            e.printStackTrace();
            badRequest();
        }
    }

    public static void deleteTrip(String id, String agencyId) {
        if (agencyId == null)
            agencyId = session.get("agencyId");

        if (id == null || agencyId == null) {
            badRequest();
            return;
        }

        AgencyTx tx = VersionedDataStore.getAgencyTx(agencyId);
        Trip trip = tx.trips.remove(id);
        String json;
        try {
            json = Base.toJson(trip, false);
        } catch (IOException e) {
            badRequest();
            return;
        }
        tx.commit();
        
        renderJSON(json);
    }
}
