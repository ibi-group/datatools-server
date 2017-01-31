package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.Organization;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static spark.Spark.*;
import static spark.Spark.get;

/**
 * Created by landon on 1/30/17.
 */
public class OrganizationController {
    public static JsonManager<Organization> json = new JsonManager<>(Organization.class, JsonViews.UserInterface.class);
    public static final Logger LOG = LoggerFactory.getLogger(OrganizationController.class);
    private static ObjectMapper mapper = new ObjectMapper();

    public static Organization getOrganization (Request req, Response res) {
        String id = req.params("id");
        if (id == null) {
            halt(400, "Must specify valid organization id");
        }
        Organization org = Organization.get(id);
        return org;
    }

    public static Collection<Organization> getAllOrganizations (Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        if (userProfile.canAdministerApplication()) {
            return Organization.getAll();
        } else {
            halt(401, "Must be application admin to view organizations");
        }
        return null;
    }

    public static Organization createOrganization (Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        if (userProfile.canAdministerApplication()) {
            Organization org = null;
            try {
                org = mapper.readValue(req.body(), Organization.class);
                org.save();
            } catch (IOException e) {
                LOG.warn("Could not create organization", e);
                halt(400, e.getMessage());
            }
            return org;
        } else {
            halt(401, "Must be application admin to view organizations");
        }
        return null;
    }

    public static Organization updateOrganization (Request req, Response res) throws IOException {
        Organization org = requestOrganizationById(req);
        applyJsonToOrganization(org, req.body());
        org.save();
        return org;
    }

    private static void applyJsonToOrganization(Organization org, String json) throws IOException {
        JsonNode node = mapper.readTree(json);
        Iterator<Map.Entry<String, JsonNode>> fieldsIter = node.fields();
        while (fieldsIter.hasNext()) {
            Map.Entry<String, JsonNode> entry = fieldsIter.next();

            if(entry.getKey().equals("name")) {
                org.name = entry.getValue().asText();
            } else if(entry.getKey().equals("logoUrl")) {
                org.logoUrl = entry.getValue().asText();
            } else if(entry.getKey().equals("usageTier")) {
                org.usageTier = Organization.UsageTier.valueOf(entry.getValue().asText());
            } else if(entry.getKey().equals("active")) {
                org.active = entry.getValue().asBoolean();
//            } else if(entry.getKey().equals("extensions")) {
//                org.extensions = entry.getValue().asBoolean();
//            } else if(entry.getKey().equals("subscriptionBeginDate")) {
//                org.subscriptionBeginDate = Date.from;
            }
        }
    }

    public static Organization deleteOrganization (Request req, Response res) {
        Organization org = requestOrganizationById(req);
        org.delete();
        return org;
    }

    private static Organization requestOrganizationById(Request req) {
        Auth0UserProfile userProfile = req.attribute("user");
        String id = req.params("id");
        if (id == null) {
            halt(400, "Must specify valid organization id");
        }
        if (userProfile.canAdministerApplication()) {
            Organization org = Organization.get(id);
            if (org == null) {
                halt(400, "Organization does not exist");
            }
            return org;
        } else {
            halt(401, "Must be application admin to modify organization");
        }
        return null;
    }

    public static void register (String apiPrefix) {
        options(apiPrefix + "secure/organization", (q, s) -> "");
        options(apiPrefix + "secure/organization/:id", (q, s) -> "");
        get(apiPrefix + "secure/organization/:id", OrganizationController::getOrganization, json::write);
        get(apiPrefix + "secure/organization", OrganizationController::getAllOrganizations, json::write);
        post(apiPrefix + "secure/organization", OrganizationController::createOrganization, json::write);
        put(apiPrefix + "secure/organization/:id", OrganizationController::updateOrganization, json::write);
        delete(apiPrefix + "secure/organization/:id", OrganizationController::deleteOrganization, json::write);
    }
}
