package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import spark.Request;
import spark.Response;

import java.net.URL;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import static spark.Spark.*;

/**
 * Created by demory on 5/25/16.
 */

public class DumpController {

    public static boolean loadLegacy (Request req, Response res) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(req.body());

        Iterator<Map.Entry<String, JsonNode>> fieldsIter = node.fields();
        while (fieldsIter.hasNext()) {
            Map.Entry<String, JsonNode> entry = fieldsIter.next();
            switch(entry.getKey()) {
                case "feedCollections":
                    for(int i=0; i< entry.getValue().size(); i++) {
                        loadLegacyProject(entry.getValue().get(i));
                    }
                    Project.commit();
                    break;
                case "feedSources":
                    for(int i=0; i< entry.getValue().size(); i++) {
                        loadLegacyFeedSource(entry.getValue().get(i));
                    }
                    FeedSource.commit();
                    break;
                case "feedVersions":
                    for(int i=0; i< entry.getValue().size(); i++) {
                        loadLegacyFeedVersion(entry.getValue().get(i));
                    }
                    FeedVersion.commit();
                    break;

            }
        }
        return true;
    }

    private static void loadLegacyProject (JsonNode node) {
        System.out.println("load legacy project " + node.findValue("name"));
        Project project = new Project();
        project.id = node.findValue("id").asText();
        project.name = node.findValue("name").asText();
        project.save(false);
    }

    private static void loadLegacyFeedSource (JsonNode node) throws Exception {
        System.out.println("load legacy FeedSource " + node.findValue("name"));
        FeedSource fs = new FeedSource();
        fs.id = node.findValue("id").asText();
        fs.projectId = node.findValue("feedCollectionId").asText();
        fs.name = node.findValue("name").asText();
        switch(node.findValue("retrievalMethod").asText()) {
            case "FETCHED_AUTOMATICALLY":
                fs.retrievalMethod = FeedSource.FeedRetrievalMethod.FETCHED_AUTOMATICALLY;
                break;
            case "MANUALLY_UPLOADED":
                fs.retrievalMethod = FeedSource.FeedRetrievalMethod.MANUALLY_UPLOADED;
                break;
            case "PRODUCED_IN_HOUSE":
                fs.retrievalMethod = FeedSource.FeedRetrievalMethod.PRODUCED_IN_HOUSE;
                break;
        }
        fs.snapshotVersion = node.findValue("snapshotVersion").asText();
        Object url = node.findValue("url").asText();
        fs.url = url != null && !url.equals("null") ? new URL(url.toString()) : null;

        //fs.lastFetched = new Date(node.findValue("lastFetched").asText());
        //System.out.println("wrote lastFetched");

        fs.deployable = node.findValue("deployable").asBoolean();
        fs.isPublic = node.findValue("isPublic").asBoolean();
        fs.save(false);
    }

    private static void loadLegacyFeedVersion (JsonNode node) throws Exception {
        System.out.println("load legacy FeedVersion " + node.findValue("id"));
        FeedVersion version = new FeedVersion();
        version.id = node.findValue("id").asText();
        version.version = node.findValue("version").asInt();
        version.feedSourceId = node.findValue("feedSourceId").asText();
        version.hash = node.findValue("hash").asText();
        version.updated = new Date(node.findValue("updated").asLong());
        System.out.println("updated= " + node.findValue("updated").asText());
        version.save(false);
    }

    public static boolean validateAll (Request req, Response res) throws Exception {
        for(FeedVersion version: FeedVersion.getAll()) {
            if(version.validationResult != null) continue;
            System.out.println("Validating " + version.id);
            version.validate();
            version.save();
        }
        return true;
    }

    public static void register (String apiPrefix) {
        post(apiPrefix + "loadLegacy", DumpController::loadLegacy, JsonUtil.objectMapper::writeValueAsString);
        post(apiPrefix + "validateAll", DumpController::validateAll, JsonUtil.objectMapper::writeValueAsString);
    }
}
