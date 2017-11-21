package com.conveyal.datatools.manager.controllers;

import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedValidationResult;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.Note;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import static spark.Spark.*;

/**
 * Created by demory on 5/25/16.
 */

public class DumpController {
    public static final Logger LOG = LoggerFactory.getLogger(DumpController.class);
    /**
     * Represents a snapshot of the database. This require loading the entire database into RAM.
     * This shouldn't be an issue, though, as the feeds are stored separately. This is only metadata.
     */
    public static class DatabaseState {
        public Collection<Project> projects;
        public Collection<FeedSource> feedSources;
        public Collection<FeedVersion> feedVersions;
        public Collection<Note> notes;
        //        public Collection<Auth0UserProfile> users;
        public Collection<Deployment> deployments;
        public Collection<ExternalFeedSourceProperty> externalProperties;
    }
//
    private static JsonManager<DatabaseState> json =
            new JsonManager<DatabaseState>(DatabaseState.class, JsonViews.DataDump.class);
//
    public static DatabaseState dump (Request req, Response res) throws JsonProcessingException {
//        // FIXME this appears to be capable of using unbounded amounts of memory (it copies an entire database into memory)
        DatabaseState db = new DatabaseState();
//        db.projects = Persistence.projects.getAll();
//        db.feedSources = Persistence.feedSources.getAll();
//        db.feedVersions = Persistence.feedVersions.getAll();
//        db.notes = Persistence.notes.getAll();
//        db.deployments = Persistence.deployments.getAll();
//        db.externalProperties = Persistence.externalFeedSourceProperties.getAll();
        return db;
    }
//
//    // this is not authenticated, because it has to happen with a bare database (i.e. no users)
//    // this method in particular is coded to allow up to 500MB of data to be posted
////    @BodyParser.Of(value=BodyParser.Json.class, maxLength = 500 * 1024 * 1024)
    public static boolean load (Request req, Response res) {
//        // TODO: really ought to check all tables
//        LOG.info("loading data...");
//        DatabaseState db = null;
//        try {
//            db = json.read(req.body());
//            LOG.info("data loaded successfully");
//        } catch (IOException e) {
//            e.printStackTrace();
//            LOG.error("data load error.  check json validity.");
//            return false;
//        }
//        for (Project c : db.projects) {
//            LOG.info("loading project {}", c.id);
//            c.save(false);
//        }
//        Project.commit();
//
//        for (FeedSource s : db.feedSources) {
//            LOG.info("loading feed source {}", s.id);
//            s.save(false);
//        }
//        FeedSource.commit();
//
//        for (FeedVersion v : db.feedVersions) {
//            LOG.info("loading version {}", v.id);
//            v.save(false);
//        }
//        FeedVersion.commit();
//
//        for (Note n : db.notes) {
//            LOG.info("loading note {}", n.id);
//            n.save(false);
//        }
//        Note.commit();
//
//        for (Deployment d : db.deployments) {
//            LOG.info("loading deployment {}", d.id);
//            d.save(false);
//        }
//        Deployment.commit();
//
//        for (ExternalFeedSourceProperty d : db.externalProperties) {
//            LOG.info("loading external properties {}", d.id);
//            d.save(false);
//        }
//        ExternalFeedSourceProperty.commit();
//
//        LOG.info("load completed.");
        return true;
    }
//
    public static boolean loadLegacy (Request req, Response res) throws Exception {
//        ObjectMapper mapper = new ObjectMapper();
//        JsonNode node = mapper.readTree(req.body());
//
//        Iterator<Map.Entry<String, JsonNode>> fieldsIter = node.fields();
//        while (fieldsIter.hasNext()) {
//            Map.Entry<String, JsonNode> entry = fieldsIter.next();
//            LOG.info("Loading {} {}...", entry.getValue().size(), entry.getKey());
//            switch(entry.getKey()) {
//                case "feedCollections":
//                    for(int i=0; i< entry.getValue().size(); i++) {
//                        loadLegacyProject(entry.getValue().get(i));
//                    }
//                    Project.commit();
//                    break;
//                case "projects":
//                    for(int i=0; i< entry.getValue().size(); i++) {
//                        loadLegacyProject(entry.getValue().get(i));
//                    }
//                    Project.commit();
//                    break;
//                case "feedSources":
//                    for(int i=0; i< entry.getValue().size(); i++) {
//                        loadLegacyFeedSource(entry.getValue().get(i));
//                    }
//                    FeedSource.commit();
//                    break;
//                case "feedVersions":
//                    for(int i=0; i< entry.getValue().size(); i++) {
//                        loadLegacyFeedVersion(entry.getValue().get(i));
//                    }
//                    FeedVersion.commit();
//                    break;
//                default:
//                    break;
//            }
//        }
        return true;
    }
//
//    private static void loadLegacyProject (JsonNode node) {
//        String name = node.findValue("name").asText();
//        String id = node.findValue("id").asText();
//        if (Project.retrieve(id) == null) {
//            LOG.info("load legacy project " + name);
//            Project project = new Project();
//            project.id = id;
//            project.name = name;
//            project.save(false);
//        }
//        else {
//            LOG.warn("legacy project {} already exists... skipping", name);
//        }
//    }
//
//    private static void loadLegacyFeedSource (JsonNode node) throws Exception {
//        String name = node.findValue("name").asText();
//        String id = node.findValue("id").asText();
//        if (Persistence.feedSources.getById(id) == null) {
//            LOG.info("load legacy FeedSource " + name);
//            FeedSource fs = new FeedSource();
//            fs.id = id;
//            fs.projectId = node.findValue("feedCollectionId").asText();
//            fs.name = name;
//            switch(node.findValue("retrievalMethod").asText()) {
//                case "FETCHED_AUTOMATICALLY":
//                    fs.retrievalMethod = FeedSource.FeedRetrievalMethod.FETCHED_AUTOMATICALLY;
//                    break;
//                case "MANUALLY_UPLOADED":
//                    fs.retrievalMethod = FeedSource.FeedRetrievalMethod.MANUALLY_UPLOADED;
//                    break;
//                case "PRODUCED_IN_HOUSE":
//                    fs.retrievalMethod = FeedSource.FeedRetrievalMethod.PRODUCED_IN_HOUSE;
//                    break;
//            }
//            fs.snapshotVersion = node.findValue("snapshotVersion").asText();
//            Object url = node.findValue("url").asText();
//            fs.url = url != null && !url.equals("null") ? new URL(url.toString()) : null;
//
//            //fs.lastFetched = new Date(node.findValue("lastFetched").asText());
//            //System.out.println("wrote lastFetched");
//
//            fs.deployable = node.findValue("deployable").asBoolean();
//            fs.isPublic = node.findValue("isPublic").asBoolean();
//            fs.save(false);
//        }
//        else {
//            LOG.warn("legacy FeedSource {} already exists... skipping", name);
//        }
//
//    }
//
    private static void loadLegacyFeedVersion (JsonNode node) throws Exception {
        String id = node.findValue("id").asText();
        if (Persistence.feedVersions.getById(id) == null) {
            LOG.info("load legacy FeedVersion " + node.findValue("id"));
            FeedVersion version = new FeedVersion();
            version.id = node.findValue("id").asText();
            version.version = node.findValue("version").asInt();
            version.feedSourceId = node.findValue("feedSourceId").asText();
            version.hash = node.findValue("hash").asText();
            version.updated = new Date(node.findValue("updated").asLong());
            LOG.info("updated= " + node.findValue("updated").asText());
            Persistence.feedVersions.create(version);
        }
        else {
            LOG.warn("legacy FeedVersion {} already exists... skipping", id);
        }
    }
//
    public static boolean validateAll (Request req, Response res) throws Exception {
//        LOG.info("validating all feeds...");
//        Collection<FeedVersion> allVersions = FeedVersion.retrieveAll();
//        for(FeedVersion version: allVersions) {
//            boolean force = req.queryParams("force") != null ? req.queryParams("force").equals("true") : false;
//            FeedValidationResult result = version.validationResult;
//            if(!force && result != null && result.loadStatus.equals(LoadStatus.SUCCESS)) {
//                continue;
//            }
//            LOG.info("Validating {}", version.id);
//            try {
//                version.validate();
//                version.save();
//            } catch (Exception e) {
//                LOG.error("Could not validate", e);
////                halt(400, "Error validating feed");
//            }
//        }
//        LOG.info("Finished validation...");
        return true;
    }
//
    public static void register (String apiPrefix) {
        post(apiPrefix + "loadLegacy", DumpController::loadLegacy, json::write);
        post(apiPrefix + "load", DumpController::load, json::write);
        post(apiPrefix + "validateAll", DumpController::validateAll, json::write);
        get(apiPrefix + "dump", DumpController::dump, json::write);
        System.out.println("registered dump w/ prefix " + apiPrefix);
    }
}
