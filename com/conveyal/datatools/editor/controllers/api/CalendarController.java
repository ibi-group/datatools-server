package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.editor.datastore.FeedTx;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.transit.ScheduleException;
import com.conveyal.datatools.editor.models.transit.ServiceCalendar;
import com.conveyal.datatools.editor.models.transit.ServiceCalendar.ServiceCalendarForPattern;
import com.conveyal.datatools.editor.models.transit.Trip;
import org.mapdb.Fun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.HaltException;
import spark.Request;
import spark.Response;

import static com.conveyal.datatools.common.utils.SparkUtils.formatJSON;
import static spark.Spark.*;

import java.util.Calendar;
import java.util.Collection;
import java.util.Set;


public class CalendarController {
    public static JsonManager<Calendar> json =
            new JsonManager<>(Calendar.class, JsonViews.UserInterface.class);
    private static final Logger LOG = LoggerFactory.getLogger(CalendarController.class);
    public static Object getCalendar(Request req, Response res) {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");
        String patternId = req.queryParams("patternId");

        if (feedId == null) {
            feedId = req.session().attribute("feedId");
        }

        if (feedId == null) {
            halt(400);
        }

        final FeedTx tx = VersionedDataStore.getFeedTx(feedId);

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

    public static Object createCalendar(Request req, Response res) {
        ServiceCalendar cal;
        FeedTx tx = null;

        try {
            cal = Base.mapper.readValue(req.body(), ServiceCalendar.class);

            if (!VersionedDataStore.feedExists(cal.feedId)) {
                halt(400);
            }

            if (req.session().attribute("feedId") != null && !req.session().attribute("feedId").equals(cal.feedId))
                halt(400);
            
            tx = VersionedDataStore.getFeedTx(cal.feedId);
            
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

    public static Object updateCalendar(Request req, Response res) {
        ServiceCalendar cal;
        FeedTx tx = null;

        try {
            cal = Base.mapper.readValue(req.body(), ServiceCalendar.class);

            if (!VersionedDataStore.feedExists(cal.feedId)) {
                halt(400);
            }

            if (req.session().attribute("feedId") != null && !req.session().attribute("feedId").equals(cal.feedId))
                halt(400);

            tx = VersionedDataStore.getFeedTx(cal.feedId);
            
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
            
            Object json = Base.toJson(cal, false);
            
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

    public static Object deleteCalendar(Request req, Response res) {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");

        FeedTx tx = VersionedDataStore.getFeedTx(feedId);

        if (id == null || !tx.calendars.containsKey(id)) {
            tx.rollback();
            halt(404);
        }

        // we just don't let you delete calendars unless there are no trips on them
        Long count = tx.tripCountByCalendar.get(id);
        if (count != null && count > 0) {
            tx.rollback();
            halt(400, formatJSON("Cannot delete calendar that is referenced by trips.", 400));
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
