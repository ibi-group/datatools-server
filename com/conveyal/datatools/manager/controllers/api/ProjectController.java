package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.FetchProjectFeedsJob;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import spark.Request;
import spark.Response;

import static spark.Spark.*;

/**
 * Created by demory on 3/14/16.
 */

public class ProjectController {

    public static JsonManager<Project> json =
            new JsonManager<>(Project.class, JsonViews.UserInterface.class);

    public static final Logger LOG = LoggerFactory.getLogger(ProjectController.class);

    public static Collection<Project> getAllProjects(Request req, Response res) throws JsonProcessingException {

        Auth0UserProfile userProfile = req.attribute("user");

        Collection<Project> filteredProjects = new ArrayList<Project>();

        System.out.println("found projects: " + Project.getAll().size());
        for (Project proj : Project.getAll()) {
            // Get feedSources if making a public call
            if (req.pathInfo().contains("public")) {
                proj.feedSources = proj.getProjectFeedSources().stream().filter(fs -> fs.isPublic).collect(Collectors.toList());
            }
            else {
                proj.feedSources = null;
            }
            if (userProfile.canAdministerApplication() || userProfile.hasProject(proj.id) || req.pathInfo().contains("public")) {
                filteredProjects.add(proj);
            }
        }

        return filteredProjects;
    }

    public static Project getProject(Request req, Response res) {
        String id = req.params("id");
        Project proj = Project.get(id);

        // Get feedSources if making a public call
        if (req.pathInfo().contains("public")) {
            proj.feedSources = proj.getProjectFeedSources().stream().filter(fs -> fs.isPublic).collect(Collectors.toList());
        }
        else {
            proj.feedSources = null;
        }
        return proj;
    }

    public static Project createProject(Request req, Response res) throws IOException {
        Project proj = new Project();

        applyJsonToProject(proj, req.body());
        proj.save();

        return proj;
    }

    public static Project updateProject(Request req, Response res) throws IOException {
        String id = req.params("id");
        Project proj = Project.get(id);

        applyJsonToProject(proj, req.body());
        proj.save();

        return proj;
    }

    public static Project deleteProject(Request req, Response res) throws IOException {
        String id = req.params("id");
        Project proj = Project.get(id);

        proj.delete();

        return proj;

    }

    public static Boolean fetch(Request req, Response res) {
        String id = req.params("id");
        System.out.println("project fetch for " + id);
        Project proj = Project.get(id);
        FetchProjectFeedsJob job = new FetchProjectFeedsJob(proj);
        job.run();
        return true;
    }

    public static void applyJsonToProject(Project proj, String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(json);
        Iterator<Map.Entry<String, JsonNode>> fieldsIter = node.fields();
        while (fieldsIter.hasNext()) {
            Map.Entry<String, JsonNode> entry = fieldsIter.next();
            if(entry.getKey().equals("name")) {
                proj.name = entry.getValue().asText();
            }
            else if(entry.getKey().equals("defaultLocationLat")) {
                proj.defaultLocationLat = entry.getValue().asDouble();
                LOG.info("updating default lat");
            }
            else if(entry.getKey().equals("defaultLocationLon")) {
                proj.defaultLocationLon = entry.getValue().asDouble();
                LOG.info("updating default lon");
            }
            else if(entry.getKey().equals("north")) {
                proj.north = entry.getValue().asDouble();
            }
            else if(entry.getKey().equals("south")) {
                proj.south = entry.getValue().asDouble();
            }
            else if(entry.getKey().equals("east")) {
                proj.east = entry.getValue().asDouble();
            }
            else if(entry.getKey().equals("west")) {
                proj.west = entry.getValue().asDouble();
            }
            else if(entry.getKey().equals("defaultLanguage")) {
                proj.defaultLanguage = entry.getValue().asText();
            }
            else if(entry.getKey().equals("defaultTimeZone")) {
                proj.defaultTimeZone = entry.getValue().asText();
            }
        }
    }

    public static Project thirdPartySync(Request req, Response res) throws Exception {
//        String token = getToken();
//        if (token == null) return unauthorized("Could not find authorization token");
//        Auth0UserProfile userProfile = verifyUser();
//        if (userProfile == null) return unauthorized();
        String id = req.params("id");
        Project proj = Project.get(id);

        String syncType = req.params("type");

//        if (!userProfile.canAdministerProject(proj.id))
//            return unauthorized();

        LOG.info("syncing with third party " + syncType);

        if(DataManager.feedResources.containsKey(syncType)) {
            DataManager.feedResources.get(syncType).importFeedsForProject(proj, req.headers("Authorization"));
            return proj;
        }

        halt(404);
        return null;
    }

    public static void register (String apiPrefix) {
        options(apiPrefix + "secure/project", (q, s) -> "");
        options(apiPrefix + "secure/project/:id", (q, s) -> "");
        get(apiPrefix + "secure/project/:id", ProjectController::getProject, json::write);
        get(apiPrefix + "secure/project", ProjectController::getAllProjects, json::write);
        post(apiPrefix + "secure/project", ProjectController::createProject, json::write);
        put(apiPrefix + "secure/project/:id", ProjectController::updateProject, json::write);
        delete(apiPrefix + "secure/project/:id", ProjectController::deleteProject, json::write);
        get(apiPrefix + "secure/project/:id/thirdPartySync/:type", ProjectController::thirdPartySync, json::write);
        post(apiPrefix + "secure/project/:id/fetch", ProjectController::fetch, json::write);

        get(apiPrefix + "public/project/:id", ProjectController::getProject, json::write);
        get(apiPrefix + "public/project", ProjectController::getAllProjects, json::write);
    }

}
