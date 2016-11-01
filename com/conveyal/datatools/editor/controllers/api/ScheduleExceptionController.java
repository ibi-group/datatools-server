package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.datastore.FeedTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.transit.ScheduleException;
import java.time.LocalDate;

import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import spark.HaltException;
import spark.Request;
import spark.Response;

import static spark.Spark.*;


public class ScheduleExceptionController {
    public static JsonManager<ScheduleException> json =
            new JsonManager<>(ScheduleException.class, JsonViews.UserInterface.class);
    /** Get all of the schedule exceptions for an agency */
    public static Object getScheduleException (Request req, Response res) {
        String exceptionId = req.params("exceptionId");
        String feedId = req.queryParams("feedId");
        Object json = null;
        if (feedId == null)
            feedId = req.session().attribute("feedId");

        if (feedId == null) {
            halt(400);
        }

        FeedTx tx = null;

        try {
            tx = VersionedDataStore.getFeedTx(feedId);

            if (exceptionId != null) {
                if (!tx.exceptions.containsKey(exceptionId))
                    halt(400);
                else
                    json = Base.toJson(tx.exceptions.get(exceptionId), false);
            }
            else {
                json = Base.toJson(tx.exceptions.values(), false);
            }
            tx.rollback();
        } catch (HaltException e) {
            throw e;
        } catch (Exception e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
            halt(400);
        }
        return json;
    }
    
    public static Object createScheduleException (Request req, Response res) {
        FeedTx tx = null;
        try {
            ScheduleException ex = Base.mapper.readValue(req.body(), ScheduleException.class);

            if (!VersionedDataStore.feedExists(ex.feedId)) {
                halt(400);
            }

            if (req.session().attribute("feedId") != null && !req.session().attribute("feedId").equals(ex.feedId))
                halt(400);

            tx = VersionedDataStore.getFeedTx(ex.feedId);

            if (ex.customSchedule != null) {
                for (String cal : ex.customSchedule) {
                    if (!tx.calendars.containsKey(cal)) {
                        tx.rollback();
                        halt(400);
                    }
                }
            }
            if (ex.addedService != null) {
                for (String cal : ex.addedService) {
                    if (!tx.calendars.containsKey(cal)) {
                        tx.rollback();
                        halt(400);
                    }
                }
            }
            if (ex.removedService != null) {
                for (String cal : ex.removedService) {
                    if (!tx.calendars.containsKey(cal)) {
                        tx.rollback();
                        halt(400);
                    }
                }
            }

            if (tx.exceptions.containsKey(ex.id)) {
                tx.rollback();
                halt(400);
            }
            if (ex.dates != null) {
                for (LocalDate date : ex.dates) {
                    if (tx.scheduleExceptionCountByDate.containsKey(date) && tx.scheduleExceptionCountByDate.get(date) > 0) {
                        tx.rollback();
                        halt(400);
                    }
                }
            }

            tx.exceptions.put(ex.id, ex);

            tx.commit();

            return Base.toJson(ex, false);
        } catch (HaltException e) {
            throw e;
        } catch (Exception e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
            halt(400);
        }
        return null;
    }
    
    public static Object updateScheduleException (Request req, Response res) {
        FeedTx tx = null;
        try {
            ScheduleException ex = Base.mapper.readValue(req.body(), ScheduleException.class);

            if (req.session().attribute("feedId") != null && !req.session().attribute("feedId").equals(ex.feedId))
                halt(400);

            if (!VersionedDataStore.feedExists(ex.feedId)) {
                halt(400);
            }

            tx = VersionedDataStore.getFeedTx(ex.feedId);

            if (ex.customSchedule != null) {
                for (String cal : ex.customSchedule) {
                    if (!tx.calendars.containsKey(cal)) {
                        tx.rollback();
                        halt(400);
                    }
                }
            }
            if (ex.addedService != null) {
                for (String cal : ex.addedService) {
                    if (!tx.calendars.containsKey(cal)) {
                        tx.rollback();
                        halt(400);
                    }
                }
            }
            if (ex.removedService != null) {
                for (String cal : ex.removedService) {
                    if (!tx.calendars.containsKey(cal)) {
                        tx.rollback();
                        halt(400);
                    }
                }
            }

            if (!tx.exceptions.containsKey(ex.id)) {
                tx.rollback();
                halt(400);
            }

            tx.exceptions.put(ex.id, ex);

            tx.commit();

            return ex;
        } catch (HaltException e) {
            throw e;
        } catch (Exception e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
            halt(400);
        }
        return null;
    }
    
    public static Object deleteScheduleException (Request req, Response res) {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");

        if (feedId == null)
            feedId = req.session().attribute("feedId");

        if (feedId == null) {
            halt(400);
        }

        FeedTx tx = VersionedDataStore.getFeedTx(feedId);
        tx.exceptions.remove(id);
        tx.commit();

        return true; // ok();
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/scheduleexception/:id", ScheduleExceptionController::getScheduleException, json::write);
        options(apiPrefix + "secure/scheduleexception", (q, s) -> "");
        get(apiPrefix + "secure/scheduleexception", ScheduleExceptionController::getScheduleException, json::write);
        post(apiPrefix + "secure/scheduleexception", ScheduleExceptionController::createScheduleException, json::write);
        put(apiPrefix + "secure/scheduleexception/:id", ScheduleExceptionController::updateScheduleException, json::write);
        delete(apiPrefix + "secure/scheduleexception/:id", ScheduleExceptionController::deleteScheduleException, json::write);
    }
}
