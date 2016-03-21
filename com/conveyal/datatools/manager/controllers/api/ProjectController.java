package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.utils.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

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

        proj.save();

        return proj;
    }

    public static Project updateProject(Request req, Response res) throws IOException {
        String id = req.params("id");
        Project proj = Project.get(id);
        System.out.println("updating project " + id);
        System.out.println(req.body());
        System.out.println("proj=" + proj);

        /*jObject = new JSONObject(contents.trim());
        Iterator<?> keys = jObject.keys();

        while( keys.hasNext() ) {
            String key = (String)keys.next();
            if ( jObject.get(key) instanceof JSONObject ) {

            }
        }*/

        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(req.body());
        Iterator<Map.Entry<String, JsonNode>> fieldsIter = node.fields();
        while (fieldsIter.hasNext()) {
            Map.Entry<String, JsonNode> entry = fieldsIter.next();
            System.out.println("entry key=" + entry.getKey());

            if(entry.getKey().equals("name")) {
                System.out.println(entry.getValue().toString());
                //proj.name = entry.getValue().asText();
            }
        }

        //proj.save();

        return proj;
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "project/:id", ProjectController::getProject, JsonUtil.objectMapper::writeValueAsString);
        get(apiPrefix + "project", ProjectController::getAllProjects, JsonUtil.objectMapper::writeValueAsString);
        post(apiPrefix + "project", ProjectController::createProject, JsonUtil.objectMapper::writeValueAsString);
        put(apiPrefix + "project/:id", ProjectController::updateProject, JsonUtil.objectMapper::writeValueAsString);
    }

}
