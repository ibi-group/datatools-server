package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
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
import spark.Request;
import spark.Response;

import static spark.Spark.*;

import java.util.Calendar;
import java.util.Collection;
import java.util.Set;


public class CalendarController {
    public static JsonManager<Calendar> json =
            new JsonManager<>(Calendar.class, JsonViews.UserInterface.class);
    public static Object getCalendar(Request req, Response res) {
        String id = req.params("id");
        String agencyId = req.queryParams("agencyId");
        String patternId = req.queryParams("patternId");

        if (agencyId == null) {
            agencyId = req.session().attribute("agencyId");
        }

        if (agencyId == null) {
            halt(400);
        }

        final AgencyTx tx = VersionedDataStore.getAgencyTx(agencyId);

        try {
            if (id != null) {
                if (!tx.calendars.containsKey(id)) {
                    halt(404);
                    tx.rollback();
                }

                else {
                    ServiceCalendar c = tx.calendars.get(id);
                    c.addDerivedInfo(tx);
                    return c;
                }
            }
            else if (patternId != null) {
                if (!tx.tripPatterns.containsKey(patternId)) {
                    tx.rollback();
                    halt(404);
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

                return ret;
            }
            else {
                Collection<ServiceCalendar> cals = tx.calendars.values();
                for (ServiceCalendar c : cals) {
                    c.addDerivedInfo(tx);
                }
                return cals;
            }

            tx.rollback();
        } catch (Exception e) {
            tx.rollback();
            e.printStackTrace();
            halt(400);
        }
        return null;
    }

    public static Object createCalendar(Request req, Response res) {
        ServiceCalendar cal;
        AgencyTx tx = null;

        try {
            cal = Base.mapper.readValue(req.params("body"), ServiceCalendar.class);

            if (!VersionedDataStore.agencyExists(cal.agencyId)) {
                halt(400);
            }

            if (req.session().attribute("agencyId") != null && !req.session().attribute("agencyId").equals(cal.agencyId))
                halt(400);
            
            tx = VersionedDataStore.getAgencyTx(cal.agencyId);
            
            if (tx.calendars.containsKey(cal.id)) {
                tx.rollback();
                halt(400);
            }
            
            // check if gtfsServiceId is specified, if not create from DB id
            if(cal.gtfsServiceId == null) {
                cal.gtfsServiceId = "CAL_" + cal.id.toString();
            }
            
            cal.addDerivedInfo(tx);
            
            tx.calendars.put(cal.id, cal);
            tx.commit();

            return cal;
        } catch (Exception e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
            halt(400);
        }
        return null;
    }

    public static Object updateCalendar(Request req, Response res) {
        ServiceCalendar cal;
        AgencyTx tx = null;

        try {
            cal = Base.mapper.readValue(req.params("body"), ServiceCalendar.class);

            if (!VersionedDataStore.agencyExists(cal.agencyId)) {
                halt(400);
            }

            if (req.session().attribute("agencyId") != null && !req.session().attribute("agencyId").equals(cal.agencyId))
                halt(400);

            tx = VersionedDataStore.getAgencyTx(cal.agencyId);
            
            if (!tx.calendars.containsKey(cal.id)) {
                tx.rollback();
                halt(400);
            }
            
            // check if gtfsServiceId is specified, if not create from DB id
            if(cal.gtfsServiceId == null) {
                cal.gtfsServiceId = "CAL_" + cal.id.toString();
            }
            
            cal.addDerivedInfo(tx);
            
            tx.calendars.put(cal.id, cal);
            
            String json = Base.toJson(cal, false);
            
            tx.commit();

            return json;
        } catch (Exception e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
            halt(400);
        }
        return null;
    }

    public static Object deleteCalendar(Request req, Response res) {
        String id = req.params("id");
        String agencyId = req.queryParams("agencyId");

        AgencyTx tx = VersionedDataStore.getAgencyTx(agencyId);

        if (id == null || !tx.calendars.containsKey(id)) {
            tx.rollback();
            halt(404);
        }

        // we just don't let you delete calendars unless there are no trips on them
        Long count = tx.tripCountByCalendar.get(id);
        if (count != null && count > 0) {
            tx.rollback();
            halt(400);
        }

        // drop this calendar from any schedule exceptions
        for (ScheduleException ex : tx.getExceptionsByCalendar(id)) {
            ex.customSchedule.remove(id);
            tx.exceptions.put(ex.id, ex);
        }

        tx.calendars.remove(id);

        tx.commit();

        return true; // ok();
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/calendar/:id", CalendarController::getCalendar, json::write);
        options(apiPrefix + "secure/calendar", (q, s) -> "");
        get(apiPrefix + "secure/calendar", CalendarController::getCalendar, json::write);
        post(apiPrefix + "secure/calendar", CalendarController::createCalendar, json::write);
        put(apiPrefix + "secure/calendar/:id", CalendarController::updateCalendar, json::write);
        delete(apiPrefix + "secure/calendar/:id", CalendarController::deleteCalendar, json::write);
    }
}
