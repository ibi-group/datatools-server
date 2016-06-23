package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.datastore.FeedTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.transit.ScheduleException;
import com.conveyal.datatools.editor.models.transit.Trip;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import org.mapdb.Fun;
import spark.Request;
import spark.Response;

import java.util.Calendar;
import java.util.Collection;
import java.util.Set;

import static spark.Spark.*;
import static spark.Spark.delete;

/**
 * Created by landon on 6/22/16.
 */
public class FareController {
    public static JsonManager<Calendar> json =
            new JsonManager<>(Calendar.class, JsonViews.UserInterface.class);
    public static Object getFare(Request req, Response res) {
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

                Collection<ServiceCalendar.ServiceCalendarForPattern> ret =
                        Collections2.transform(serviceCalendarIds, new Function<String, ServiceCalendar.ServiceCalendarForPattern>() {

                            @Override
                            public ServiceCalendar.ServiceCalendarForPattern apply(String input) {
                                ServiceCalendar cal = tx.calendars.get(input);

                                Long count = tx.tripCountByPatternAndCalendar.get(new Fun.Tuple2(patternId, cal.id));

                                if (count == null) count = 0L;

                                return new ServiceCalendar.ServiceCalendarForPattern(cal, tx.tripPatterns.get(patternId), count);
                            }

                        });

                return ret;
            }
            else {
                Collection<ServiceCalendar> cals = tx.calendars.values();
                System.out.println("# of cals: " + cals.size());
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

    public static Object createFare(Request req, Response res) {
        ServiceCalendar cal;
        FeedTx tx = null;

        try {
            cal = Base.mapper.readValue(req.body(), ServiceCalendar.class);

            if (!VersionedDataStore.agencyExists(cal.feedId)) {
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
        } catch (Exception e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
            halt(400);
        }
        return null;
    }

    public static Object updateFare(Request req, Response res) {
        ServiceCalendar cal;
        FeedTx tx = null;

        try {
            cal = Base.mapper.readValue(req.body(), ServiceCalendar.class);

            if (!VersionedDataStore.agencyExists(cal.feedId)) {
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
        } catch (Exception e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
            halt(400);
        }
        return null;
    }

    public static Object deleteFare(Request req, Response res) {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");

        FeedTx tx = VersionedDataStore.getFeedTx(feedId);

        if (id == null || !tx.calendars.containsKey(id)) {
            tx.rollback();
            halt(404);
        }

//        // we just don't let you delete calendars unless there are no trips on them
//        Long count = tx.tripCountByCalendar.get(id);
//        if (count != null && count > 0) {
//            tx.rollback();
//            halt(400);
//        }


        tx.fares.remove(id);

        tx.commit();

        return true; // ok();
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
