package com.conveyal.datatools.editor.controllers.api;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import com.conveyal.datatools.editor.controllers.Application;
import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.controllers.Secure;
import com.conveyal.datatools.editor.controllers.Security;
import com.conveyal.datatools.editor.datastore.AgencyTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.transit.ScheduleException;
import com.conveyal.datatools.editor.models.transit.ServiceCalendar;
import com.conveyal.datatools.editor.models.transit.ServiceCalendar.ServiceCalendarForPattern;
import com.conveyal.datatools.editor.models.transit.Trip;
import org.mapdb.Fun;

import static spark.Spark.*;

import java.util.Collection;
import java.util.Set;

@With(Secure.class)
public class CalendarController extends Controller {
    @Before
    static void initSession() throws Throwable {

        if(!Security.isConnected() && !Application.checkOAuth(request, session))
            Secure.login("");
    }

    public static void getCalendar(String id, String agencyId, final String patternId) {
        if (agencyId == null) {
            agencyId = session.get("agencyId");
        }

        if (agencyId == null) {
            badRequest();
            return;
        }

        final AgencyTx tx = VersionedDataStore.getAgencyTx(agencyId);

        try {
            if (id != null) {
                if (!tx.calendars.containsKey(id)) {
                    notFound();
                    tx.rollback();
                    return;
                }

                else {
                    ServiceCalendar c = tx.calendars.get(id);
                    c.addDerivedInfo(tx);
                    renderJSON(Base.toJson(c, false));
                }
            }
            else if (patternId != null) {
                if (!tx.tripPatterns.containsKey(patternId)) {
                    tx.rollback();
                    notFound();
                    return;
                }

                Set<String> serviceCalendarIds = Sets.newHashSet();
                for (Trip trip : tx.getTripsByPattern(patternId)) {
                    serviceCalendarIds.add(trip.calendarId);
                }

                Collection<ServiceCalendarForPattern> ret =
                        Collections2.transform(serviceCalendarIds, new Function<String, ServiceCalendarForPattern>() {

                            @Override
                            public ServiceCalendarForPattern apply(String input) {
                                ServiceCalendar cal = tx.calendars.get(input);

                                Long count = tx.tripCountByPatternAndCalendar.get(new Fun.Tuple2(patternId, cal.id));

                                if (count == null) count = 0L;

                                return new ServiceCalendarForPattern(cal, tx.tripPatterns.get(patternId), count);
                            }

                        });

                renderJSON(Base.toJson(ret, false));
            }
            else {
                Collection<ServiceCalendar> cals = tx.calendars.values();
                for (ServiceCalendar c : cals) {
                    c.addDerivedInfo(tx);
                }
                renderJSON(Base.toJson(cals, false));
            }

            tx.rollback();
        } catch (Exception e) {
            tx.rollback();
            e.printStackTrace();
            badRequest();
        }

    }

    public static void createCalendar() {
        ServiceCalendar cal;
        AgencyTx tx = null;

        try {
            cal = Base.mapper.readValue(params.get("body"), ServiceCalendar.class);

            if (!VersionedDataStore.agencyExists(cal.agencyId)) {
                badRequest();
                return;
            }

            if (session.contains("agencyId") && !session.get("agencyId").equals(cal.agencyId))
                badRequest();
            
            tx = VersionedDataStore.getAgencyTx(cal.agencyId);
            
            if (tx.calendars.containsKey(cal.id)) {
                tx.rollback();
                badRequest();
                return;
            }
            
            // check if gtfsServiceId is specified, if not create from DB id
            if(cal.gtfsServiceId == null) {
                cal.gtfsServiceId = "CAL_" + cal.id.toString();
            }
            
            cal.addDerivedInfo(tx);
            
            tx.calendars.put(cal.id, cal);
            tx.commit();

            renderJSON(Base.toJson(cal, false));
        } catch (Exception e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
            badRequest();
        }
    }

    public static void updateCalendar() {
        ServiceCalendar cal;
        AgencyTx tx = null;

        try {
            cal = Base.mapper.readValue(params.get("body"), ServiceCalendar.class);

            if (!VersionedDataStore.agencyExists(cal.agencyId)) {
                badRequest();
                return;
            }
            
            if (session.contains("agencyId") && !session.get("agencyId").equals(cal.agencyId))
                badRequest();

            tx = VersionedDataStore.getAgencyTx(cal.agencyId);
            
            if (!tx.calendars.containsKey(cal.id)) {
                tx.rollback();
                badRequest();
                return;
            }
            
            // check if gtfsServiceId is specified, if not create from DB id
            if(cal.gtfsServiceId == null) {
                cal.gtfsServiceId = "CAL_" + cal.id.toString();
            }
            
            cal.addDerivedInfo(tx);
            
            tx.calendars.put(cal.id, cal);
            
            String json = Base.toJson(cal, false);
            
            tx.commit();

            renderJSON(json);
        } catch (Exception e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
            badRequest();
        }
    }

    public static void deleteCalendar(String id, String agencyId) {
        AgencyTx tx = VersionedDataStore.getAgencyTx(agencyId);

        if (id == null || !tx.calendars.containsKey(id)) {
            tx.rollback();
            notFound();
            return;
        }

        // we just don't let you delete calendars unless there are no trips on them
        Long count = tx.tripCountByCalendar.get(id);
        if (count != null && count > 0) {
            tx.rollback();
            badRequest();
            return;
        }

        // drop this calendar from any schedule exceptions
        for (ScheduleException ex : tx.getExceptionsByCalendar(id)) {
            ex.customSchedule.remove(id);
            tx.exceptions.put(ex.id, ex);
        }

        tx.calendars.remove(id);

        tx.commit();

        ok();
    }
}
