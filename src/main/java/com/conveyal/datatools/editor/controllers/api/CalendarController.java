package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.datastore.FeedTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.transit.ScheduleException;
import com.conveyal.datatools.editor.models.transit.ServiceCalendar;
import com.conveyal.datatools.editor.models.transit.ServiceCalendar.ServiceCalendarForPattern;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.google.common.collect.Sets;
import org.mapdb.Fun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.HaltException;
import spark.Request;
import spark.Response;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static com.conveyal.datatools.common.utils.SparkUtils.haltWithMessage;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.options;
import static spark.Spark.post;
import static spark.Spark.put;


public class CalendarController {
    public static final JsonManager<Calendar> json =
            new JsonManager<>(Calendar.class, JsonViews.UserInterface.class);
    private static final Logger LOG = LoggerFactory.getLogger(CalendarController.class);

    public static Object getCalendar(Request req, Response res) {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");
        String patternId = req.queryParams("patternId");

        if (feedId == null) {
            haltWithMessage(req, 400, "feedId is required");
        }

        FeedTx tx = null;

        try {
            tx = VersionedDataStore.getFeedTx(feedId);
            if (id != null) {
                if (!tx.calendars.containsKey(id)) {
                    haltWithMessage(req, 404, "calendar not found in database");
                }

                else {
                    ServiceCalendar c = tx.calendars.get(id);
                    c.addDerivedInfo(tx);
                    return c;
                }
            }
            else if (patternId != null) {
                if (!tx.tripPatterns.containsKey(patternId)) {
                    haltWithMessage(req, 404, "pattern not found in database");
                }

                Set<String> serviceCalendarIds = Sets.newHashSet();
                serviceCalendarIds.addAll(tx.getTripsByPattern(patternId).stream()
                        .map(trip -> trip.calendarId)
                        .collect(Collectors.toList()));

                Collection<ServiceCalendarForPattern> ret = new HashSet<>();

                for (String calendarId : serviceCalendarIds) {
                    ServiceCalendar cal = tx.calendars.get(calendarId);
                    Long count = tx.tripCountByPatternAndCalendar.get(new Fun.Tuple2(patternId, cal.id));
                    if (count == null) count = 0L;
                    ret.add(new ServiceCalendarForPattern(cal, tx.tripPatterns.get(patternId), count));
                }
                return ret;
            }
            else {
                 // put values into a new ArrayList to avoid returning MapDB BTreeMap
                 // (and possible access error once transaction is closed)
                Collection<ServiceCalendar> cals = new ArrayList<>(tx.calendars.values());
                for (ServiceCalendar c : cals) {
                    c.addDerivedInfo(tx);
                }
                return cals;
            }
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            haltWithMessage(req, 500, "an unexpected error occurred", e);
        } finally {
            if (tx != null) tx.rollbackIfOpen();
        }
        return null;
    }

    public static ServiceCalendar createCalendar(Request req, Response res) {
        ServiceCalendar cal;
        FeedTx tx = null;

        try {
            cal = Base.mapper.readValue(req.body(), ServiceCalendar.class);

            if (!VersionedDataStore.feedExists(cal.feedId)) {
                haltWithMessage(req, 400, "feed does not exist");
            }

            if (req.session().attribute("feedId") != null && !req.session().attribute("feedId").equals(cal.feedId))
                haltWithMessage(req, 400, "feed does not match");

            tx = VersionedDataStore.getFeedTx(cal.feedId);

            if (tx.calendars.containsKey(cal.id)) {
                haltWithMessage(req, 404, "calendar with this id already exists in database");
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
            e.printStackTrace();
            haltWithMessage(req, 500, "an unexpected error occurred", e);
        } finally {
            if (tx != null) tx.rollbackIfOpen();
        }
        return null;
    }

    public static ServiceCalendar updateCalendar(Request req, Response res) {
        ServiceCalendar cal;
        FeedTx tx = null;

        try {
            cal = Base.mapper.readValue(req.body(), ServiceCalendar.class);

            if (!VersionedDataStore.feedExists(cal.feedId)) {
                haltWithMessage(req, 400, "feed does not exist");
            }

            tx = VersionedDataStore.getFeedTx(cal.feedId);

            if (!tx.calendars.containsKey(cal.id)) {
                haltWithMessage(req, 400, "calendar does not exist in database");
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
            e.printStackTrace();
            haltWithMessage(req, 500, "an unexpected error occurred", e);
        } finally {
            if (tx != null) tx.rollbackIfOpen();
        }
        return null;
    }

    public static ServiceCalendar deleteCalendar(Request req, Response res) {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");

        FeedTx tx = null;

        try {
            tx = VersionedDataStore.getFeedTx(feedId);

            if (id == null || !tx.calendars.containsKey(id)) {
                haltWithMessage(req, 404, "calendar not found in database");
            }

            // we just don't let you delete calendars unless there are no trips on them
            Long count = tx.tripCountByCalendar.get(id);
            if (count != null && count > 0) {
                haltWithMessage(req, 400, "Cannot delete calendar that is referenced by trips.");
            }

            // drop this calendar from any schedule exceptions
            for (ScheduleException ex : tx.getExceptionsByCalendar(id)) {
                ex.customSchedule.remove(id);
                tx.exceptions.put(ex.id, ex);
            }
            ServiceCalendar cal = tx.calendars.get(id);

            tx.calendars.remove(id);

            tx.commit();

            return cal;
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            haltWithMessage(req, 500, "an unexpected error occurred", e);
        } finally {
            if (tx != null) tx.rollbackIfOpen();
        }
        return null;
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
