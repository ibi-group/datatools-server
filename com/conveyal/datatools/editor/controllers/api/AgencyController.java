package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.editor.controllers.Application;
import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.controllers.Secure;
import com.conveyal.datatools.editor.controllers.Security;
import com.conveyal.datatools.editor.datastore.AgencyTx;
import com.conveyal.datatools.editor.datastore.GlobalTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.transit.Agency;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import spark.Request;
import spark.Response;

import static spark.Spark.*;

public class AgencyController {
    public static JsonManager<Agency> json =
            new JsonManager<>(Agency.class, JsonViews.UserInterface.class);
    public static Object getAgency(Request req, Response res) {
        String id = req.params("id");
        String json = null;
        try {
            GlobalTx tx = VersionedDataStore.getGlobalTx();

            if(id != null) {
                if (!tx.agencies.containsKey(id)) {
                    tx.rollback();
                    halt(404);
                }

                json = Base.toJson(tx.agencies.get(id), false);
            }
            else {
                json = Base.toJson(tx.agencies.values(), false);
            }
            
            tx.rollback();
        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
        }
        return json;
    }

    public static Object createAgency(Request req, Response res) {
        Agency agency;

        try {
            agency = Base.mapper.readValue(req.params("body"), Agency.class);
            
            // check if gtfsAgencyId is specified, if not create from DB id
            if(agency.gtfsAgencyId == null) {
                agency.gtfsAgencyId = "AGENCY_" + agency.id;
            }
            
            GlobalTx tx = VersionedDataStore.getGlobalTx();
            
            if (tx.agencies.containsKey(agency.id)) {
                tx.rollback();
                halt(400);
            }
            
            tx.agencies.put(agency.id, agency);
            tx.commit();

            return Base.toJson(agency, false);
        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
        }
        return null;
    }


    public static Object updateAgency(Request req, Response res) {
        Agency agency;

        try {
            agency = Base.mapper.readValue(req.params("body"), Agency.class);
            
            GlobalTx tx = VersionedDataStore.getGlobalTx();

            if(!tx.agencies.containsKey(agency.id)) {
                tx.rollback();
                halt(400);
            }
            
            // check if gtfsAgencyId is specified, if not create from DB id
            if(agency.gtfsAgencyId == null)
                agency.gtfsAgencyId = "AGENCY_" + agency.id.toString();

            tx.agencies.put(agency.id, agency);
            tx.commit();

            return Base.toJson(agency, false);
        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
        }
        return null;
    }

    public static Object deleteAgency(Request req, Response res) {
        GlobalTx tx = VersionedDataStore.getGlobalTx();
        String id = req.params("id");
        if(id == null) {
            halt(400);
        }
        
        if (!tx.agencies.containsKey(id)) {
            halt(404);
        }

        tx.agencies.remove(id);
        tx.commit();
        
        return true; // ok();
    }
    
    /** duplicate an agency */
    public static Object duplicateAgency(Request req, Response res) {

        String id = req.params("id");
        // make sure the agency exists
        GlobalTx gtx = VersionedDataStore.getGlobalTx();
        if (!gtx.agencies.containsKey(id)) {
            gtx.rollback();
            halt(404);
        }
        gtx.rollback();

        AgencyTx.duplicate(id);
        return true; // ok();
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/agency/:id", AgencyController::getAgency, json::write);
        options(apiPrefix + "secure/agency", (q, s) -> "");
        get(apiPrefix + "secure/agency", AgencyController::getAgency, json::write);
        post(apiPrefix + "secure/agency", AgencyController::createAgency, json::write);
        put(apiPrefix + "secure/agency/:id", AgencyController::updateAgency, json::write);
        post(apiPrefix + "secure/agency/:id/duplicate", AgencyController::duplicateAgency, json::write);
        delete(apiPrefix + "secure/agency/:id", AgencyController::deleteAgency, json::write);

        // Public routes
//        get(apiPrefix + "public/agency/:id", AgencyController::getFeedSource, json::write);
//        get(apiPrefix + "public/agency", AgencyController::getAllFeedSources, json::write);
    }
}
