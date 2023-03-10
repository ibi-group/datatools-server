package com.conveyal.datatools.manager.controllers;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.ProcessSingleFeedJob;
import com.conveyal.datatools.manager.jobs.ValidateFeedJob;
import com.conveyal.datatools.manager.jobs.ValidateMobilityDataFeedJob;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedRetrievalMethod;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.Note;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.models.Snapshot;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.JobUtils;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.conveyal.gtfs.validator.ValidationResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static spark.Spark.get;
import static spark.Spark.post;

/**
 * This class contains HTTP endpoints that should ONLY be used in controlled environments (i.e., when the application is
 * not accessible on the Internet. The endpoints allow for dumping the entirety of the manager application data
 * (projects, feed sources, feed versions, etc.) into a JSON file. NOTE: this does not include actual GTFS feed contents
 * stored in PostgreSQL, but rather the metadata about these feeds and how they are organized into feed sources and
 * projects. This allows for backing up and restoring the MongoDB data.
 */

public class DumpController {
    public static final Logger LOG = LoggerFactory.getLogger(DumpController.class);
    /**
     * Represents a snapshot of the database. This requires loading the entire database into RAM.
     * This shouldn't be an issue, though, as the feeds are stored separately. This is only metadata.
     */
    public static class DatabaseState {
        public Collection<Project> projects;
        public Collection<FeedSource> feedSources;
        public Collection<FeedVersion> feedVersions;
        public Collection<Note> notes;
        // Users are maintained in Auth0 database.
        // public Collection<Auth0UserProfile> users;
        public Collection<Deployment> deployments;
        public Collection<ExternalFeedSourceProperty> externalProperties;
        public Collection<Snapshot> snapshots;
    }
//
    private static JsonManager<DatabaseState> json =
        new JsonManager<>(DatabaseState.class, JsonViews.DataDump.class);


    /**
     * Method to handle a web request for a legacy object
     */
    private static boolean getLegacy(Request req, Response response) {
        try {
            return loadLegacy(req.body());
        } catch (IOException e) {
            logMessageAndHalt(req, 400, "Error loading legacy JSON", e);
            return false;
        }
    }

    /**
     * Copies each table containing application data into the database state object and returns entire set of data. This,
     * along with the other methods in this class, should only be used in a controlled environment where no outside access
     * is permitted (e.g., using a cloned database on a local development machine). Otherwise, application data is
     * visible to the entire world.
     */
    public static DatabaseState dump (Request req, Response res) throws JsonProcessingException {
//        // FIXME this appears to be capable of using unbounded amounts of memory (it copies an entire database into memory)
        DatabaseState db = new DatabaseState();
        db.projects = Persistence.projects.getAll();
        db.feedSources = Persistence.feedSources.getAll();
        db.feedVersions = Persistence.feedVersions.getAll();
        db.notes = Persistence.notes.getAll();
        db.deployments = Persistence.deployments.getAll();
        db.externalProperties = Persistence.externalFeedSourceProperties.getAll();
        db.snapshots = Persistence.snapshots.getAll();
        return db;
    }
    // FIXME: This can now be authenticated because users are stored in Auth0.
    // this is not authenticated, because it has to happen with a bare database (i.e. no users)
    // this method in particular is coded to allow up to 500MB of data to be posted
//    @BodyParser.Of(value=BodyParser.Json.class, maxLength = 500 * 1024 * 1024)

    /**
     * Load a JSON dump into the manager database. This should be performed with the python script load.py found
     * in the datatools-ui/scripts directory.
     */
    public static boolean load (String jsonString) {
        // TODO: really ought to check all tables
        LOG.info("loading data...");
        DatabaseState db;
        try {
            db = json.read(jsonString);
            LOG.info("data loaded successfully");
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("data load error.  check json validity.");
            return false;
        }
        for (Project project : db.projects) {
            LOG.info("loading project {}", project.id);
            Persistence.projects.create(project);
        }

        for (FeedSource feedSource : db.feedSources) {
            LOG.info("loading feed source {}", feedSource.id);
            Persistence.feedSources.create(feedSource);
        }

        for (FeedVersion feedVersion : db.feedVersions) {
            LOG.info("loading version {}", feedVersion.id);
            Persistence.feedVersions.create(feedVersion);
        }

        for (Note note : db.notes) {
            LOG.info("loading note {}", note.id);
            Persistence.notes.create(note);
        }

        for (Deployment deployment : db.deployments) {
            LOG.info("loading deployment {}", deployment.id);
            Persistence.deployments.create(deployment);
        }

        for (ExternalFeedSourceProperty externalFeedSourceProperty : db.externalProperties) {
            LOG.info("loading external properties {}", externalFeedSourceProperty.id);
            Persistence.externalFeedSourceProperties.create(externalFeedSourceProperty);
        }

        for (Snapshot snapshot : db.snapshots) {
            LOG.info("loading snapshot {}", snapshot.id);
            Persistence.snapshots.create(snapshot);
        }

        LOG.info("load completed.");
        return true;
    }

    /**
     * Updates snapshots in Mongo database with data from a list of snapshots in a JSON dump file. This is mainly intended
     * for a one-off import that did not load in the snapshots from a dump file, but rather generated them directly from
     * an editor mapdb. This method also deletes any duplicate snapshots (i.e., where the feedSourceId and version are
     * the same), leaving only one snapshot for that feedSourceId/version remaining.
     * @param jsonString
     * @return
     */
    public static boolean updateSnapshotMetadata (String jsonString) {
        LOG.info("loading data...");
        DatabaseState db;
        try {
            db = json.read(jsonString);
            LOG.info("data loaded successfully");
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("data load error.  check json validity.");
            return false;
        }

        if (db.snapshots == null || db.snapshots.size() == 0) {
            LOG.warn("No snapshots found in JSON!!");
            return false;
        }
        int updateCount = 0;
        int deleteCount = 0;
        for (Snapshot snapshotFromJSON : db.snapshots) {
            List<Snapshot> matchingSnapshots = Persistence.snapshots.getFiltered(and(
                    eq("version", snapshotFromJSON.version),
                    eq(Snapshot.FEED_SOURCE_REF, snapshotFromJSON.feedSourceId)));

            Iterator<Snapshot> snapshotIterator = matchingSnapshots.iterator();
            while (snapshotIterator.hasNext()) {
                Snapshot nextSnapshot = snapshotIterator.next();
                if (snapshotIterator.hasNext()) {
                    // Remove any duplicates that may have been created during import
                    LOG.warn("Removing duplicate snapshot for {}.{}", snapshotFromJSON.feedSourceId, snapshotFromJSON.version);
                    Persistence.snapshots.removeById(nextSnapshot.id);
                    deleteCount++;
                } else {
                    // Update snapshot from JSON with single remaining snapshot's id, namespace, and feed load result
                    LOG.info("updating snapshot {}.{}", snapshotFromJSON.feedSourceId, snapshotFromJSON.version);
                    snapshotFromJSON.id = nextSnapshot.id;
                    snapshotFromJSON.namespace = nextSnapshot.namespace;
                    snapshotFromJSON.feedLoadResult = nextSnapshot.feedLoadResult;
                    // Replace stored snapshot with snapshot from JSON.
                    Persistence.snapshots.replace(nextSnapshot.id, snapshotFromJSON);
                    updateCount++;
                }
            }
        }
        if (updateCount > 0 || deleteCount > 0) {
            LOG.info("{} snapshots updated, {} snapshots deleted (duplicates)", updateCount, deleteCount);
            return true;
        }
        else {
            LOG.warn("No snapshots updated or deleted.");
            return false;
        }
    }

    /**
     * Load a v2 JSON dump (i.e., objects with the class structure immediately before the MongoDB migration).
     */
    private static boolean loadLegacy(String jsonString) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(jsonString);
        Iterator<Map.Entry<String, JsonNode>> fieldsIter = node.fields();
        while (fieldsIter.hasNext()) {
            Map.Entry<String, JsonNode> entry = fieldsIter.next();
            LOG.info("Loading {} {}...", entry.getValue().size(), entry.getKey());
            switch(entry.getKey()) {
                case "projects":
                    for(int i=0; i< entry.getValue().size(); i++) {
                        loadLegacyProject(entry.getValue().get(i));
                    }
                    break;
                case "feedSources":
                    for(int i=0; i< entry.getValue().size(); i++) {
                        loadLegacyFeedSource(entry.getValue().get(i));
                    }
                    break;
                case "feedVersions":
                    for(int i=0; i< entry.getValue().size(); i++) {
                        loadLegacyFeedVersion(entry.getValue().get(i));
                    }
                    break;
                // FIXME: add deployments, etc.
                default:
                    break;
            }
        }
        return true;
    }

    /**
     * Load a v2 project (i.e., a project with the class structure immediately before the MongoDB migration).
     */
    private static void loadLegacyProject (JsonNode node) {
        String name = node.findValue("name").asText();
        String id = node.findValue("id").asText();
        if (Persistence.projects.getById(id) == null) {
            LOG.info("load legacy project " + name);
            Project project = new Project();
            project.id = id;
            project.name = name;
            Persistence.projects.create(project);
        }
        else {
            LOG.warn("legacy project {} already exists... skipping", name);
        }
    }

    /**
     * Load a v2 feed source (i.e., a feed source with the class structure immediately before the MongoDB migration).
     */
    private static void loadLegacyFeedSource (JsonNode node) {
        String name = node.findValue("name").asText();
        String id = node.findValue("id").asText();
        if (Persistence.feedSources.getById(id) == null) {
            LOG.info("load legacy FeedSource " + name);
            FeedSource feedSource = new FeedSource();
            feedSource.id = id;
            feedSource.projectId = node.findValue("feedCollectionId").asText();
            feedSource.name = name;
            switch(node.findValue("retrievalMethod").asText()) {
                case "FETCHED_AUTOMATICALLY":
                    feedSource.retrievalMethod = FeedRetrievalMethod.FETCHED_AUTOMATICALLY;
                    break;
                case "MANUALLY_UPLOADED":
                    feedSource.retrievalMethod = FeedRetrievalMethod.MANUALLY_UPLOADED;
                    break;
                case "PRODUCED_IN_HOUSE":
                    feedSource.retrievalMethod = FeedRetrievalMethod.PRODUCED_IN_HOUSE;
                    break;
            }
            feedSource.snapshotVersion = node.findValue("snapshotVersion").asText();
            Object url = node.findValue("url").asText();
            try {
                feedSource.url = url != null && !url.equals("null") ? new URL(url.toString()) : null;
            } catch (MalformedURLException e) {
                LOG.error("Failed to create feedsource url");
                e.printStackTrace();
            }

            //fs.lastFetched = new Date(node.findValue("lastFetched").asText());
            //System.out.println("wrote lastFetched");

            feedSource.deployable = node.findValue("deployable").asBoolean();
            feedSource.isPublic = node.findValue("isPublic").asBoolean();
            Persistence.feedSources.create(feedSource);
        }
        else {
            LOG.warn("legacy FeedSource {} already exists... skipping", name);
        }

    }

    /**
     * Load a v2 feed version (i.e., a feed version with the class structure immediately before the MongoDB migration).
     */
    private static void loadLegacyFeedVersion (JsonNode node) {
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

    /**
     * HTTP endpoint that will trigger the initial or re-validation of all feed versions contained in the application.
     * The intended use cases here are 1) to validate all versions after a fresh database copy has been loaded in and
     * 2) to trigger a revalidation of all feed versions should a new validation stage be added to the validation process
     * that needs to be applied to all feeds.
     */
    public static boolean validateAll (boolean load, boolean force, String filterFeedId) throws Exception {
        LOG.info("validating all feeds...");
        Collection<FeedVersion> allVersions = Persistence.feedVersions.getAll();
        for(FeedVersion version: allVersions) {
            ValidationResult result = version.validationResult;
            if(!force && result != null && result.fatalException != null) {
                // If the force option is not true and the validation result did not fail, re-validate.
                continue;
            }
            if (filterFeedId != null && !version.feedSourceId.equals(filterFeedId)) {
                // Skip all feeds except Cortland for now.
                continue;
            }
            Auth0UserProfile systemUser = Auth0UserProfile.createSystemUser();
            if (load) {
                JobUtils.heavyExecutor.execute(new ProcessSingleFeedJob(version, systemUser, false));
            } else {
                JobUtils.heavyExecutor.execute(new ValidateFeedJob(version, systemUser, false));
                JobUtils.heavyExecutor.execute(new ValidateMobilityDataFeedJob(version, systemUser, false));
            }
        }
        // ValidateAllFeedsJob validateAllFeedsJob = new ValidateAllFeedsJob("system", force, load);
        return true;
    }

    /**
     * Enables the HTTP controllers at the specified prefix.
     */
    public static void register (String apiPrefix) {
        post(apiPrefix + "loadLegacy", DumpController::getLegacy, json::write);
        post(apiPrefix + "load", (request, response) -> load(request.body()), json::write);
        post(apiPrefix + "validateAll", (request, response) -> {
            boolean force = request.queryParams("force") != null && request.queryParams("force").equals("true");
            boolean load = request.queryParams("load") != null && request.queryParams("load").equals("true");
            return validateAll(load, force, null);
        }, json::write);
        get(apiPrefix + "dump", DumpController::dump, json::write);
        LOG.warn("registered dump w/ prefix " + apiPrefix);
    }
}
