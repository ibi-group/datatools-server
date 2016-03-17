package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.utils.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;

import spark.Request;
import spark.Response;

import static spark.Spark.*;

/**
 * Created by demory on 3/14/16.
 */
public class ProjectController {

    public static final Logger LOG = LoggerFactory.getLogger(ProjectController.class);

    public static Collection<Project> getAllProjects(Request req, Response res) throws JsonProcessingException {
        /*String token = getToken();
        if (token == null) return unauthorized("Could not find authorization token");
        Auth0UserProfile userProfile = verifyUser();
        if (userProfile == null) return unauthorized();*/

        Collection<Project> filteredProjects = new ArrayList<Project>();

        System.out.println("found projects: " + Project.getAll().size());
        for (Project proj : Project.getAll()) {
            /*if (userProfile.canAdministerApplication() || userProfile.hasProject(proj.id)) {
                filteredFCs.add(proj);
            }*/
            filteredProjects.add(proj);
        }

        return filteredProjects;
    }

    public static Project getProject(Request req, Response res) {
        String id = req.params("id");
        return Project.get(id);
    }

    public static Project createProject(Request req, Response res) {

        Project proj = new Project();

        // TODO: fail gracefully
        System.out.println();
        proj.name = req.queryParams("name");
        //System.out.println("new proj name=" + proj.name);
        //c.setUser(userProfile);

        proj.save();

        return proj;
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "project/:id", ProjectController::getProject, JsonUtil.objectMapper::writeValueAsString);
        get(apiPrefix + "project", ProjectController::getAllProjects, JsonUtil.objectMapper::writeValueAsString);
        post(apiPrefix + "project", ProjectController::createProject, JsonUtil.objectMapper::writeValueAsString);
    }

}
