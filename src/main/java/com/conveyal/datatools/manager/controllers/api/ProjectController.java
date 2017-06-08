package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.common.utils.Consts;
import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.FetchProjectFeedsJob;
import com.conveyal.datatools.manager.jobs.MakePublicJob;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.Organization;
import com.conveyal.datatools.manager.models.OtpBuildConfig;
import com.conveyal.datatools.manager.models.OtpRouterConfig;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.http.concurrent.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import spark.Request;
import spark.Response;

import javax.servlet.http.HttpServletResponse;

import static spark.Spark.*;

/**
 * Created by demory on 3/14/16.
 */

public class ProjectController {

    public static JsonManager<Project> json =
            new JsonManager<>(Project.class, JsonViews.UserInterface.class);

    public static final Logger LOG = LoggerFactory.getLogger(ProjectController.class);
    private static ObjectMapper mapper = new ObjectMapper();
    public static Collection<Project> getAllProjects(Request req, Response res) throws JsonProcessingException {

        Auth0UserProfile userProfile = req.attribute("user");
        Collection<Project> filteredProjects = new ArrayList<Project>();

        LOG.info("found projects: " + Project.getAll().size());
        for (Project proj : Project.getAll()) {
            // Get feedSources if making a public call
            if (req.pathInfo().contains("public")) {
                proj.feedSources = proj.getProjectFeedSources().stream().filter(fs -> fs != null && fs.isPublic).collect(Collectors.toList());
            }
            else {
                proj.feedSources = null;
            }
            if (req.pathInfo().contains("public") || userProfile.canAdministerApplication() || userProfile.hasProject(proj.id, proj.organizationId)) {
                filteredProjects.add(proj);
            }
        }

        return filteredProjects;
    }

    public static Project getProject(Request req, Response res) {
        String id = req.params("id");
        Project proj = Project.get(id);
        if (proj == null) {
            halt(404, "No project with id: " + id);
        }
        // Get feedSources if making a public call
        if (req.pathInfo().contains("public")) {
            Collection<FeedSource> feeds = proj.getProjectFeedSources().stream().filter(fs -> fs.isPublic).collect(Collectors.toList());
            proj.feedSources = feeds;
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
        Project proj = requestProjectById(req, "manage");

        applyJsonToProject(proj, req.body());
        proj.save();

        return proj;
    }

    public static Project deleteProject(Request req, Response res) throws IOException {
        Project proj = requestProjectById(req, "manage");
        proj.delete();

        return proj;

    }

    public static Boolean fetch(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        Project proj = requestProjectById(req, "manage");

        FetchProjectFeedsJob fetchProjectFeedsJob = new FetchProjectFeedsJob(proj, userProfile.getUser_id());

        // this is runnable because sometimes we schedule the task for a later time, but here we call it immediately
        // because it is short lived and just cues more work
        fetchProjectFeedsJob.run();
        return true;
    }

    public static void applyJsonToProject(Project proj, String json) throws IOException {
        JsonNode node = mapper.readTree(json);
        boolean updateFetchSchedule = false;
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
            else if(entry.getKey().equals("organizationId")) {
                proj.organizationId = entry.getValue().asText();
            }
            else if(entry.getKey().equals("osmNorth")) {
                proj.osmNorth = entry.getValue().asDouble();
            }
            else if(entry.getKey().equals("osmSouth")) {
                proj.osmSouth = entry.getValue().asDouble();
            }
            else if(entry.getKey().equals("osmEast")) {
                proj.osmEast = entry.getValue().asDouble();
            }
            else if(entry.getKey().equals("osmWest")) {
                proj.osmWest = entry.getValue().asDouble();
            }
            else if(entry.getKey().equals("useCustomOsmBounds")) {
                proj.useCustomOsmBounds = entry.getValue().asBoolean();
            }
            else if(entry.getKey().equals("defaultLanguage")) {
                proj.defaultLanguage = entry.getValue().asText();
            }
            else if(entry.getKey().equals("defaultTimeZone")) {
                proj.defaultTimeZone = entry.getValue().asText();
            }
            else if(entry.getKey().equals("autoFetchHour")) {
                proj.autoFetchHour = entry.getValue().asInt();
                updateFetchSchedule = true;
            }
            else if(entry.getKey().equals("autoFetchMinute")) {
                proj.autoFetchMinute = entry.getValue().asInt();
                updateFetchSchedule = true;
            }
            else if(entry.getKey().equals("autoFetchFeeds")) {
                proj.autoFetchFeeds = entry.getValue().asBoolean();
                updateFetchSchedule = true;
            }
            else if(entry.getKey().equals("otpServers")) {
                updateOtpServers(proj, entry.getValue());
            }
            else if (entry.getKey().equals("buildConfig")) {
                updateBuildConfig(proj, entry.getValue());
            }
            else if (entry.getKey().equals("routerConfig")) {
                updateRouterConfig(proj, entry.getValue());
            }
        }
        if (updateFetchSchedule) {
            // If auto fetch flag is turned on
            if (proj.autoFetchFeeds){
                int interval = 1; // once per day interval
                DataManager.autoFetchMap.put(proj.id, scheduleAutoFeedFetch(proj.id, proj.autoFetchHour, proj.autoFetchMinute, interval, proj.defaultTimeZone));
            }
            // otherwise, cancel any existing task for this id
            else{
                cancelAutoFetch(proj.id);
            }
        }
    }

    private static void updateOtpServers(Project proj, JsonNode otpServers) {
        if (otpServers.isArray()) {
            proj.otpServers = new ArrayList<>();
            for (int i = 0; i < otpServers.size(); i++) {
                JsonNode otpServer = otpServers.get(i);

                OtpServer otpServerObj = new OtpServer();
                if (otpServer.has("name")) {
                    JsonNode name = otpServer.get("name");
                    otpServerObj.name = name.isNull() ? null : name.asText();
                }
                if (otpServer.has("admin")) {
                    JsonNode admin = otpServer.get("admin");
                    otpServerObj.admin = admin.isNull() ? false : admin.asBoolean();
                }
                if (otpServer.has("publicUrl")) {
                    JsonNode publicUrl = otpServer.get("publicUrl");
                    otpServerObj.publicUrl = publicUrl.isNull() ? null : publicUrl.asText();
                }
                if (otpServer.has("s3Bucket")) {
                    JsonNode s3Bucket = otpServer.get("s3Bucket");
                    otpServerObj.s3Bucket = s3Bucket.isNull() ? null : s3Bucket.asText();
                }
                if (otpServer.has("s3Credentials")) {
                    JsonNode s3Credentials = otpServer.get("s3Credentials");
                    otpServerObj.s3Credentials = s3Credentials.isNull() ? null : s3Credentials.asText();
                }
                if (otpServer.has("internalUrl") && otpServer.get("internalUrl").isArray()) {
                    JsonNode internalUrl = otpServer.get("internalUrl");
                    for (int j = 0; j < internalUrl.size(); j++) {
                        if (internalUrl.get(j).isNull()) {
                            continue;
                        }
                        String url = internalUrl.get(j).asText();
                        if (otpServerObj.internalUrl == null) {
                            otpServerObj.internalUrl = new ArrayList<>();
                        }
                        otpServerObj.internalUrl.add(url);
                    }
                }
                proj.otpServers.add(otpServerObj);
            }
        }
    }

    private static void updateBuildConfig(Project proj, JsonNode buildConfig) {
        if(proj.buildConfig == null) proj.buildConfig = new OtpBuildConfig();
        if(buildConfig.has("subwayAccessTime")) {
            JsonNode subwayAccessTime = buildConfig.get("subwayAccessTime");
            // allow client to un-set option via 'null' value
            proj.buildConfig.subwayAccessTime = subwayAccessTime.isNull() || subwayAccessTime.asText().equals("") ? null : subwayAccessTime.asDouble();
        }
        if(buildConfig.has("fetchElevationUS")) {
            JsonNode fetchElevationUS = buildConfig.get("fetchElevationUS");
            proj.buildConfig.fetchElevationUS = fetchElevationUS.isNull() || fetchElevationUS.asText().equals("") ? null : fetchElevationUS.asBoolean();
        }
        if(buildConfig.has("stationTransfers")) {
            JsonNode stationTransfers = buildConfig.get("stationTransfers");
            proj.buildConfig.stationTransfers = stationTransfers.isNull() || stationTransfers.asText().equals("") ? null : stationTransfers.asBoolean();
        }
        if (buildConfig.has("fares")) {
            JsonNode fares = buildConfig.get("fares");
            proj.buildConfig.fares = fares.isNull() || fares.asText().equals("") ? null : fares.asText();
        }
    }

    /**
     * Helper function returns feed source if user has permission for specified action.
     * @param req spark Request object from API request
     * @param action action type (either "view" or "manage")
     * @return
     */
    private static Project requestProjectById(Request req, String action) {
        String id = req.params("id");
        if (id == null) {
            halt(SparkUtils.formatJSON("Please specify id param", 400));
        }
        return requestProject(req, Project.get(id), action);
    }
    public static Project requestProject(Request req, Project p, String action) {
        Auth0UserProfile userProfile = req.attribute("user");
        Boolean publicFilter = Boolean.valueOf(req.queryParams("public"));

        // check for null project
        if (p == null)
            halt(400, SparkUtils.formatJSON("Project ID does not exist", 400));

        boolean authorized;
        switch (action) {
            case "manage":
                authorized = userProfile.canAdministerProject(p.id, p.organizationId);
                break;
            case "view":
                authorized = false; // userProfile.canViewProject(p.id, p.id);
                break;
            default:
                authorized = false;
                break;
        }

        // if requesting public sources
//        if (publicFilter){
//            // if feed not public and user not authorized, halt
//            if (!p.isPublic && !authorized)
//                halt(403, "User not authorized to perform action on feed source");
//                // if feed is public, but action is managerial, halt (we shouldn't ever get here, but just in case)
//            else if (p.isPublic && action.equals("manage"))
//                halt(403, "User not authorized to perform action on feed source");
//
//        }
//        else {
//            if (!authorized)
//                halt(403, "User not authorized to perform action on feed source");
//        }

        // if we make it here, user has permission and it's a valid feedsource
        return p;
    }

    private static void updateRouterConfig(Project proj, JsonNode routerConfig) {
        if (proj.routerConfig == null) proj.routerConfig = new OtpRouterConfig();

        if (routerConfig.has("numItineraries")) {
            JsonNode numItineraries = routerConfig.get("numItineraries");
            proj.routerConfig.numItineraries = numItineraries.isNull() ? null : numItineraries.asInt();
        }

        if (routerConfig.has("walkSpeed")) {
            JsonNode walkSpeed = routerConfig.get("walkSpeed");
            proj.routerConfig.walkSpeed = walkSpeed.isNull() ? null : walkSpeed.asDouble();
        }

        if (routerConfig.has("carDropoffTime")) {
            JsonNode carDropoffTime = routerConfig.get("carDropoffTime");
            proj.routerConfig.carDropoffTime = carDropoffTime.isNull() ? null : carDropoffTime.asDouble();
        }

        if (routerConfig.has("stairsReluctance")) {
            JsonNode stairsReluctance = routerConfig.get("stairsReluctance");
            proj.routerConfig.stairsReluctance = stairsReluctance.isNull() ? null : stairsReluctance.asDouble();
        }

        if (routerConfig.has("requestLogFile")) {
            JsonNode requestLogFile = routerConfig.get("requestLogFile");
            proj.routerConfig.requestLogFile = requestLogFile.isNull() || requestLogFile.asText().equals("") ? null : requestLogFile.asText();
        }

        if (routerConfig.has("updaters")) {
            updateProjectUpdaters(proj, routerConfig.get("updaters"));
        }
    }

    private static void updateProjectUpdaters(Project proj, JsonNode updaters) {
        if (updaters.isArray()) {
            proj.routerConfig.updaters = new ArrayList<>();
            for (int i = 0; i < updaters.size(); i++) {
                JsonNode updater = updaters.get(i);

                OtpRouterConfig.Updater updaterObj = new OtpRouterConfig.Updater();
                if(updater.has("type")) {
                    JsonNode type = updater.get("type");
                    updaterObj.type = type.isNull() ? null : type.asText();
                }

                if(updater.has("sourceType")) {
                    JsonNode sourceType = updater.get("sourceType");
                    updaterObj.sourceType = sourceType.isNull() ? null : sourceType.asText();
                }

                if(updater.has("defaultAgencyId")) {
                    JsonNode defaultAgencyId = updater.get("defaultAgencyId");
                    updaterObj.defaultAgencyId = defaultAgencyId.isNull() ? null : defaultAgencyId.asText();
                }

                if(updater.has("url")) {
                    JsonNode url = updater.get("url");
                    updaterObj.url = url.isNull() ? null : url.asText();
                }

                if(updater.has("frequencySec")) {
                    JsonNode frequencySec = updater.get("frequencySec");
                    updaterObj.frequencySec = frequencySec.isNull() ? null : frequencySec.asInt();
                }

                proj.routerConfig.updaters.add(updaterObj);
            }
        }
    }

    private static HttpServletResponse downloadMergedFeed(Request req, Response res) throws IOException {
        String id = req.params("id");
        Project p = Project.get(id);

        if(p == null) halt(500, "Project is null");

        // get feed sources in project
        Collection<FeedSource> feeds = p.getProjectFeedSources();

        // create temp merged zip file to add feed content to
        File mergedFile = null;
        try {
            mergedFile = File.createTempFile(p.id + "-merged", ".zip");
            mergedFile.deleteOnExit();

        } catch (IOException e) {
            LOG.error("Could not create temp file");
            e.printStackTrace();

            halt(400, SparkUtils.formatJSON("Unknown error while merging feeds.", 400));
        }

        // create the zipfile
        ZipOutputStream out;
        try {
            out = new ZipOutputStream(new FileOutputStream(mergedFile));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        LOG.info("Created project merge file: " + mergedFile.getAbsolutePath());

        // map of feed versions to table entries contained within version's GTFS
        Map<FeedSource, ZipFile> feedSourceMap = new HashMap<>();

        // collect zipFiles for each feedSource before merging tables
        for (FeedSource fs : feeds) {
            // check if feed source has version (use latest)
            FeedVersion version = fs.getLatest();
            if (version == null) {
                LOG.info("Skipping {} because it has no feed versions", fs.name);
                continue;
            }
            // modify feed version to use prepended feed id
            LOG.info("Adding {} feed to merged zip", fs.name);
            try {
                File file = version.getGtfsFile();
                ZipFile zipFile = new ZipFile(file);
                feedSourceMap.put(fs, zipFile);
            } catch(Exception e) {
                e.printStackTrace();
                LOG.error("Zipfile for version {} not found", version.id);
            }
        }

        // loop through GTFS tables
        for(int i = 0; i < DataManager.gtfsConfig.size(); i++) {
            JsonNode tableNode = DataManager.gtfsConfig.get(i);
            byte[] tableOut = mergeTables(tableNode, feedSourceMap);

            // if at least one feed has the table, include it
            if (tableOut != null) {
                String tableName = tableNode.get("name").asText();

                // create entry for zip file
                ZipEntry tableEntry = new ZipEntry(tableName);
                out.putNextEntry(tableEntry);
                LOG.info("Writing {} to merged feed", tableEntry.getName());
                out.write(tableOut);
                out.closeEntry();
            }
        }
        out.close();

        // Deliver zipfile
        res.raw().setContentType("application/octet-stream");
        res.raw().setHeader("Content-Disposition", "attachment; filename=" + mergedFile.getName());


        try {
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(res.raw().getOutputStream());
            BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(mergedFile));

            byte[] buffer = new byte[1024];
            int len;
            while ((len = bufferedInputStream.read(buffer)) > 0) {
                bufferedOutputStream.write(buffer, 0, len);
            }

            bufferedOutputStream.flush();
            bufferedOutputStream.close();
        } catch (Exception e) {
            halt(500, "Error serving GTFS file");
        }

        return res.raw();
    }

    /**
     * Merge the specified table for multiple GTFS feeds.
     * @param tableNode tableNode to merge
     * @param feedSourceMap map of feedSources to zipFiles from which to extract the .txt tables
     * @return
     * @throws IOException
     */
    private static byte[] mergeTables(JsonNode tableNode, Map<FeedSource, ZipFile> feedSourceMap) throws IOException {

        String tableName = tableNode.get("name").asText();
        ByteArrayOutputStream tableOut = new ByteArrayOutputStream();

        int feedIndex = 0;

        ArrayNode fieldsNode = (ArrayNode) tableNode.get("fields");
        List<String> headers = new ArrayList<>();
        for (int i = 0; i < fieldsNode.size(); i++) {
            JsonNode fieldNode = fieldsNode.get(i);
            String fieldName = fieldNode.get("name").asText();
            Boolean notInSpec = fieldNode.has("datatools") && fieldNode.get("datatools").asBoolean();
            if (notInSpec) {
                fieldsNode.remove(i);
            }
            headers.add(fieldName);
        }

        // write headers to table
        tableOut.write(String.join(",", headers).getBytes());
        tableOut.write("\n".getBytes());

        for ( Map.Entry<FeedSource, ZipFile> mapEntry : feedSourceMap.entrySet()) {
            FeedSource fs = mapEntry.getKey();
            ZipFile zipFile = mapEntry.getValue();
            final Enumeration<? extends ZipEntry> entries = zipFile.entries();
            while (entries.hasMoreElements()) {
                final ZipEntry entry = entries.nextElement();
                if(tableName.equals(entry.getName())) {
                    LOG.info("Adding {} table for {}", entry.getName(), fs.name);

                    InputStream inputStream = zipFile.getInputStream(entry);

                    BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
                    String line = in.readLine();
                    String[] fields = line.split(",");

                    List<String> fieldList = Arrays.asList(fields);

                    int rowIndex = 0;
                    while((line = in.readLine()) != null) {
                        String[] newValues = new String[fieldsNode.size()];
                        String[] values = line.split(Consts.COLUMN_SPLIT, -1);
                        if (values.length == 1) {
                            LOG.warn("Found blank line. Skipping...");
                            continue;
                        }
                        for(int v = 0; v < fieldsNode.size(); v++) {
                            JsonNode fieldNode = fieldsNode.get(v);
                            String fieldName = fieldNode.get("name").asText();

                            // get index of field from GTFS spec as it appears in feed
                            int index = fieldList.indexOf(fieldName);
                            String val = "";
                            try {
                                index = fieldList.indexOf(fieldName);
                                if(index != -1) {
                                    val = values[index];
                                }
                            } catch (ArrayIndexOutOfBoundsException e) {
                                LOG.warn("Index {} out of bounds for file {} and feed {}", index, entry.getName(), fs.name);
                                continue;
                            }

                            String fieldType = fieldNode.get("inputType").asText();

                            // if field is a gtfs identifier, prepend with feed id/name
                            if (fieldType.contains("GTFS") && !val.isEmpty()) {
                                newValues[v] = fs.name + ":" + val;
                            }
                            else {
                                newValues[v] = val;
                            }
                        }
                        String newLine = String.join(",", newValues);
                        tableOut.write(newLine.getBytes());
                        tableOut.write("\n".getBytes());
                        rowIndex++;
                    }
                }
            }
            feedIndex++;
        }
        return tableOut.toByteArray();
    }

    public static boolean deployPublic (Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        String id = req.params("id");
        if (id == null) {
            halt(400, "must provide project id!");
        }
        Project proj = Project.get(id);

        if (proj == null)
            halt(400, "no such project!");

        // run as sync job; if it gets too slow change to async
        new MakePublicJob(proj, userProfile.getUser_id()).run();
        return true;
    }

    public static Project thirdPartySync(Request req, Response res) throws Exception {
        Auth0UserProfile userProfile = req.attribute("user");
        String id = req.params("id");
        Project proj = Project.get(id);

        String syncType = req.params("type");

        if (!userProfile.canAdministerProject(proj.id, proj.organizationId))
            halt(403);

        LOG.info("syncing with third party " + syncType);

        if(DataManager.feedResources.containsKey(syncType)) {
            DataManager.feedResources.get(syncType).importFeedsForProject(proj, req.headers("Authorization"));
            return proj;
        }

        halt(404);
        return null;
    }

    public static ScheduledFuture scheduleAutoFeedFetch (String id, int hour, int minute, int intervalInDays, String timezoneId){
        TimeUnit minutes = TimeUnit.MINUTES;
        try {
            // First cancel any already scheduled auto fetch task for this project id.
            cancelAutoFetch(id);

            Project p = Project.get(id);
            if (p == null)
                return null;

            Cancellable task = null;

            ZoneId timezone;
            try {
                timezone = ZoneId.of(timezoneId);
            }catch(Exception e){
                timezone = ZoneId.of("America/New_York");
            }
            LOG.info("Scheduling autofetch for projectID: {}", p.id);

            long delayInMinutes = 0;


            // NOW in default timezone
            ZonedDateTime now = ZonedDateTime.ofInstant(Instant.now(), timezone);

            // SCHEDULED START TIME
            ZonedDateTime startTime = LocalDateTime.of(LocalDate.now(), LocalTime.of(hour, minute)).atZone(timezone);
            LOG.info("Now: {}", now.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
            LOG.info("Scheduled start time: {}", startTime.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));

            // Get diff between start time and current time
            long diffInMinutes = (startTime.toEpochSecond() - now.toEpochSecond()) / 60;
            if ( diffInMinutes >= 0 ){
                delayInMinutes = diffInMinutes; // delay in minutes
            }
            else{
                delayInMinutes = 24 * 60 + diffInMinutes; // wait for one day plus difference (which is negative)
            }

            LOG.info("Auto fetch begins in {} hours and runs every {} hours", String.valueOf(delayInMinutes / 60.0), TimeUnit.DAYS.toHours(intervalInDays));

            // system is defined as owner because owner field must not be null
            FetchProjectFeedsJob fetchProjectFeedsJob = new FetchProjectFeedsJob(p, "system");

            return DataManager.scheduler.scheduleAtFixedRate(fetchProjectFeedsJob, delayInMinutes, TimeUnit.DAYS.toMinutes(intervalInDays), minutes);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void cancelAutoFetch(String id){
        Project p = Project.get(id);
        if ( p != null && DataManager.autoFetchMap.get(p.id) != null) {
            LOG.info("Cancelling autofetch for projectID: {}", p.id);
            DataManager.autoFetchMap.get(p.id).cancel(true);
        }
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/project/:id", ProjectController::getProject, json::write);
        get(apiPrefix + "secure/project", ProjectController::getAllProjects, json::write);
        post(apiPrefix + "secure/project", ProjectController::createProject, json::write);
        put(apiPrefix + "secure/project/:id", ProjectController::updateProject, json::write);
        delete(apiPrefix + "secure/project/:id", ProjectController::deleteProject, json::write);
        get(apiPrefix + "secure/project/:id/thirdPartySync/:type", ProjectController::thirdPartySync, json::write);
        post(apiPrefix + "secure/project/:id/fetch", ProjectController::fetch, json::write);
        post(apiPrefix + "secure/project/:id/deployPublic", ProjectController::deployPublic, json::write);

        get(apiPrefix + "public/project/:id/download", ProjectController::downloadMergedFeed);

        get(apiPrefix + "public/project/:id", ProjectController::getProject, json::write);
        get(apiPrefix + "public/project", ProjectController::getAllProjects, json::write);
    }

}
