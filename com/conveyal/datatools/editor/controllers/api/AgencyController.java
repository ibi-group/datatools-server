package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.editor.controllers.Application;
import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.controllers.Secure;
import com.conveyal.datatools.editor.controllers.Security;
import com.conveyal.datatools.editor.datastore.AgencyTx;
import com.conveyal.datatools.editor.datastore.GlobalTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.transit.Agency;
import spark.Request;
import spark.Response;

import static spark.Spark.*;

public class AgencyController {

    public static void getAgency(Request req, Response res) {
        String id = req.params("id");
        try {
            GlobalTx tx = VersionedDataStore.getGlobalTx();

            if(id != null) {
                if (!tx.agencies.containsKey(id)) {
                    tx.rollback();
                    halt(404);
                    return;
                }

                renderJSON(Base.toJson(tx.agencies.get(id), false));
            }
            else {
                renderJSON(Base.toJson(tx.agencies.values(), false));
            }
            
            tx.rollback();
        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
        }
    }

    public static void createAgency(Request req, Response res) {
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
                return;
            }
            
            tx.agencies.put(agency.id, agency);
            tx.commit();

            renderJSON(Base.toJson(agency, false));
        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
        }
    }


    public static void updateAgency(Request req, Response res) {
        Agency agency;

        try {
            agency = Base.mapper.readValue(req.params("body"), Agency.class);
            
            GlobalTx tx = VersionedDataStore.getGlobalTx();

            if(!tx.agencies.containsKey(agency.id)) {
                tx.rollback();
                halt(400);
                return;
            }
            
            // check if gtfsAgencyId is specified, if not create from DB id
            if(agency.gtfsAgencyId == null)
                agency.gtfsAgencyId = "AGENCY_" + agency.id.toString();

            tx.agencies.put(agency.id, agency);
            tx.commit();

            renderJSON(Base.toJson(agency, false));
        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
        }
    }

    public static void deleteAgency(Request req, Response res) {
        GlobalTx tx = VersionedDataStore.getGlobalTx();
        String id = req.params("id");
        if(id == null) {
            halt(400);
            return;
        }
        
        if (!tx.agencies.containsKey(id)) {
            halt(404);
            return;
        }

        tx.agencies.remove(id);
        tx.commit();
        
        return true; // ok();
    }
    
    /** duplicate an agency */
    public static void duplicateAgency(Request req, Response res) {

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
}
