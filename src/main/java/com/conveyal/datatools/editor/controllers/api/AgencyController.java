package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.datastore.FeedTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.transit.Agency;
import com.conveyal.datatools.common.utils.S3Utils;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.HaltException;
import spark.Request;
import spark.Response;

import java.util.ArrayList;
import java.util.Collection;

import static spark.Spark.*;

public class AgencyController {
    public static final JsonManager<Agency> json =
            new JsonManager<>(Agency.class, JsonViews.UserInterface.class);
    private static Logger LOG = LoggerFactory.getLogger(AgencyController.class);

    public static Object getAgency(Request req, Response res) {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");
        FeedTx tx = null;
        try {
            tx = VersionedDataStore.getFeedTx(feedId);
            if(id != null) {
                if (!tx.agencies.containsKey(id)) {
                    halt(404);
                }

                return tx.agencies.get(id);
            }
            else {
                return new ArrayList<>(tx.agencies.values());
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

    public static Agency createAgency(Request req, Response res) {
        Agency agency;
        String feedId = req.queryParams("feedId");
        if (feedId == null)
            halt(400, "You must provide a valid feedId");

        FeedTx tx = null;
        try {
            agency = Base.mapper.readValue(req.body(), Agency.class);
            
            tx = VersionedDataStore.getFeedTx(feedId);
            if (tx.agencies.containsKey(agency.id)) {
                halt(400, "Agency " + agency.id + " already exists");
            }
            
            tx.agencies.put(agency.id, agency);
            tx.commit();

            return agency;
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


    public static Agency updateAgency(Request req, Response res) {
        Agency agency;
        String id = req.params("id");
        String feedId = req.queryParams("feedId");

        FeedTx tx = null;
        try {
            agency = Base.mapper.readValue(req.body(), Agency.class);
            
            tx = VersionedDataStore.getFeedTx(feedId);
            if(!tx.agencies.containsKey(agency.id)) {
                halt(400);
            }
            
            tx.agencies.put(id, agency);
            tx.commit();

            return agency;
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

    public static Agency uploadAgencyBranding(Request req, Response res) {
        Agency agency;
        String id = req.params("id");
        String feedId = req.queryParams("feedId");

        FeedTx tx = null;
        try {
            if (feedId == null) {
                halt(400);
            }
            tx = VersionedDataStore.getFeedTx(feedId);

            if (!tx.agencies.containsKey(id)) {
                halt(404);
            }

            agency = tx.agencies.get(id);

            String url = S3Utils.uploadBranding(req, id);

            // set agencyBrandingUrl to s3 location
            agency.agencyBrandingUrl = url;

            tx.agencies.put(id, agency);
            tx.commit();

            return agency;
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

    public static Agency deleteAgency(Request req, Response res) {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");
        if (id == null) {
            halt(400);
        }
        FeedTx tx = null;

        try {
            if (feedId == null) {
                halt(400);
            }
            tx = VersionedDataStore.getFeedTx(feedId);

            if(!tx.agencies.containsKey(id)) {
                halt(400);
            }

            // ensure that no routes reference agency
            tx.routes.values().stream().forEach(route -> {
                if (route.agencyId.equals(id)) {
                    halt(400, SparkUtils.formatJSON("Cannot delete agency referenced by routes.", 400));
                }
            });

            Agency agency = tx.agencies.get(id);

            tx.agencies.remove(id);
            tx.commit();

            return agency;
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
    
    /** duplicate an agency */
    // TODO: move this is feedInfo?? (Below function is for old editor)
//    public static Object duplicateAgency(Request req, Response res) {
//
//        String id = req.params("id");
//        String feedId = req.queryParams("feedId");
//
//        // make sure the agency exists
////        GlobalTx gtx = VersionedDataStore.getGlobalTx();
//        FeedTx tx = VersionedDataStore.getFeedTx(feedId);
//        if (!tx.agencies.containsKey(id)) {
//            tx.rollback();
//            halt(404);
//        }
//        tx.rollback();
//
//        FeedTx.duplicate(id);
//        return true; // ok();
//    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/agency/:id", AgencyController::getAgency, json::write);
        options(apiPrefix + "secure/agency", (q, s) -> "");
        get(apiPrefix + "secure/agency", AgencyController::getAgency, json::write);
        post(apiPrefix + "secure/agency", AgencyController::createAgency, json::write);
        put(apiPrefix + "secure/agency/:id", AgencyController::updateAgency, json::write);
//        post(apiPrefix + "secure/agency/:id/duplicate", AgencyController::duplicateAgency, json::write);
        post(apiPrefix + "secure/agency/:id/uploadbranding", AgencyController::uploadAgencyBranding, json::write);
        delete(apiPrefix + "secure/agency/:id", AgencyController::deleteAgency, json::write);

        // Public routes
//        retrieveById(apiPrefix + "public/agency/:id", AgencyController::feedSource, json::write);
//        retrieveById(apiPrefix + "public/agency", AgencyController::getAllFeedSources, json::write);
    }
}
