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
import com.conveyal.datatools.manager.models.OtpBuildConfig;
import com.conveyal.datatools.manager.models.OtpRouterConfig;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import spark.Request;
import spark.Response;

import javax.servlet.http.HttpServletResponse;

import static com.conveyal.datatools.manager.DataManager.publicPath;
import static spark.Spark.*;

/**
 * Created by demory on 3/14/16.
 */

@SuppressWarnings({"unused", "ThrowableNotThrown"})
public class ProjectController {

    public static JsonManager<Project> json =
            new JsonManager<>(Project.class, JsonViews.UserInterface.class);

    public static final Logger LOG = LoggerFactory.getLogger(ProjectController.class);
    private static ObjectMapper mapper = new ObjectMapper();

    private static Collection<Project> getAllProjects(Request req, Response res) throws JsonProcessingException {
        Auth0UserProfile userProfile = req.attribute("user");
        return Project.getAll().stream()
                .filter(p -> req.pathInfo().matches(publicPath) || userProfile.hasProject(p.id, p.organizationId))
                .map(p -> requestProject(req, p, "view"))
                .collect(Collectors.toList());
    }

    private static Project getProject(Request req, Response res) {
        return requestProjectById(req, "view");
    }

    private static Project createProject(Request req, Response res) {
        Project p = new Project();
        try {
            applyJsonToProject(p, req.body());
            p.save();
        } catch (Exception e) {
            e.printStackTrace();
            halt(400, SparkUtils.formatJSON("Error saving new project"));
        }
        return p;
    }

    private static Project updateProject(Request req, Response res) throws IOException {
        Project p = requestProjectById(req, "manage");
        try {
            applyJsonToProject(p, req.body());
            p.save();
        } catch (Exception e) {
            e.printStackTrace();
            halt(400, SparkUtils.formatJSON("Error saving project"));
        }
        return p;
    }

    private static Project deleteProject(Request req, Response res) throws IOException {
        Project p = requestProjectById(req, "manage");
        p.delete();
        return p;
    }

    public static Boolean fetch(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        Project p = requestProjectById(req, "manage");

        FetchProjectFeedsJob fetchProjectFeedsJob = new FetchProjectFeedsJob(p, userProfile.getUser_id());

        // this is runnable because sometimes we schedule the task for a later time, but here we call it immediately
        // because it is short lived and just cues more work
        fetchProjectFeedsJob.run();
        return true;
    }

    private static void applyJsonToProject(Project p, String json) throws IOException {
        JsonNode node = mapper.readTree(json);
        boolean updateFetchSchedule = false;
        Iterator<Map.Entry<String, JsonNode>> fieldsIter = node.fields();
        while (fieldsIter.hasNext()) {
            Map.Entry<String, JsonNode> entry = fieldsIter.next();
            if(entry.getKey().equals("name")) {
                p.name = entry.getValue().asText();
            }
            else if(entry.getKey().equals("defaultLocationLat")) {
                p.defaultLocationLat = entry.getValue().asDouble();
                LOG.info("updating default lat");
            }
            else if(entry.getKey().equals("defaultLocationLon")) {
                p.defaultLocationLon = entry.getValue().asDouble();
                LOG.info("updating default lon");
            }
            else if(entry.getKey().equals("north")) {
                p.north = entry.getValue().asDouble();
            }
            else if(entry.getKey().equals("south")) {
                p.south = entry.getValue().asDouble();
            }
            else if(entry.getKey().equals("east")) {
                p.east = entry.getValue().asDouble();
            }
            else if(entry.getKey().equals("west")) {
                p.west = entry.getValue().asDouble();
            }
            else if(entry.getKey().equals("organizationId")) {
                p.organizationId = entry.getValue().asText();
            }
            else if(entry.getKey().equals("osmNorth")) {
                p.osmNorth = entry.getValue().asDouble();
            }
            else if(entry.getKey().equals("osmSouth")) {
                p.osmSouth = entry.getValue().asDouble();
            }
            else if(entry.getKey().equals("osmEast")) {
                p.osmEast = entry.getValue().asDouble();
            }
            else if(entry.getKey().equals("osmWest")) {
                p.osmWest = entry.getValue().asDouble();
            }
            else if(entry.getKey().equals("useCustomOsmBounds")) {
                p.useCustomOsmBounds = entry.getValue().asBoolean();
            }
            else if(entry.getKey().equals("defaultLanguage")) {
                p.defaultLanguage = entry.getValue().asText();
            }
            else if(entry.getKey().equals("defaultTimeZone")) {
                p.defaultTimeZone = entry.getValue().asText();
            }
            else if(entry.getKey().equals("autoFetchHour")) {
                p.autoFetchHour = entry.getValue().asInt();
                updateFetchSchedule = true;
            }
            else if(entry.getKey().equals("autoFetchMinute")) {
                p.autoFetchMinute = entry.getValue().asInt();
                updateFetchSchedule = true;
            }
            else if(entry.getKey().equals("autoFetchFeeds")) {
                p.autoFetchFeeds = entry.getValue().asBoolean();
                updateFetchSchedule = true;
            }

            // NOTE: the below keys require the full objects to be included in the request json,
            // otherwise the missing fields/sub-classes will be set to null
            else if(entry.getKey().equals("otpServers")) {
                p.otpServers = mapper.readValue(String.valueOf(entry.getValue()), new TypeReference<List<OtpServer>>(){});
            }
            else if (entry.getKey().equals("buildConfig")) {
                p.buildConfig = mapper.treeToValue(entry.getValue(), OtpBuildConfig.class);
            }
            else if (entry.getKey().equals("routerConfig")) {
                p.routerConfig = mapper.treeToValue(entry.getValue(), OtpRouterConfig.class);
            }
        }
        if (updateFetchSchedule) {
            // If auto fetch flag is turned on
            if (p.autoFetchFeeds){
                int interval = 1; // once per day interval
                DataManager.autoFetchMap.put(p.id, scheduleAutoFeedFetch(p.id, p.autoFetchHour, p.autoFetchMinute, interval, p.defaultTimeZone));
            }
            // otherwise, cancel any existing task for this id
            else{
                cancelAutoFetch(p.id);
            }
        }
    }

    /**
     * Helper function returns feed source if user has permission for specified action.
     * @param req spark Request object from API request
     * @param action action type (either "view" or "manage")
     * @return requested project
     */
    private static Project requestProjectById(Request req, String action) {
        String id = req.params("id");
        if (id == null) {
            halt(SparkUtils.formatJSON("Please specify id param", 400));
        }
        return requestProject(req, Project.get(id), action);
    }
    private static Project requestProject(Request req, Project p, String action) {
        Auth0UserProfile userProfile = req.attribute("user");
        boolean publicFilter = req.pathInfo().matches(publicPath);

        // check for null project
        if (p == null) {
            halt(400, SparkUtils.formatJSON("Project ID does not exist", 400));
            return null;
        }

        boolean authorized;
        switch (action) {
            // TODO: limit create action to app/org admins?
//            case "create":
//                authorized = userProfile.canAdministerOrganization(p.organizationId);
//                break;
            case "manage":
                authorized = userProfile.canAdministerProject(p.id, p.organizationId);
                break;
            case "view":
                // request only authorized if not via public path and user can view
                authorized = !publicFilter && userProfile.hasProject(p.id, p.organizationId);
                break;
            default:
                authorized = false;
                break;
        }

        // if requesting all projects via public route, include public feed sources
        if (publicFilter){
            p.feedSources = p.getProjectFeedSources().stream()
                    .filter(fs -> fs.isPublic)
                    .collect(Collectors.toList());
        } else {
            p.feedSources = null;
            if (!authorized) {
                halt(403, SparkUtils.formatJSON("User not authorized to perform action on project", 403));
                return null;
            }
        }
        // if we make it here, user has permission and it's a valid project
        return p;
    }

    private static HttpServletResponse downloadMergedFeed(Request req, Response res) throws IOException {
        Project p = requestProjectById(req, "view");

        // get feed sources in project
        Collection<FeedSource> feeds = p.getProjectFeedSources();

        // create temp merged zip file to add feed content to
        File mergedFile;
        try {
            mergedFile = File.createTempFile(p.id + "-merged", ".zip");
            mergedFile.deleteOnExit();

        } catch (IOException e) {
            LOG.error("Could not create temp file");
            e.printStackTrace();
            halt(400, SparkUtils.formatJSON("Unknown error while merging feeds.", 400));
            return null;
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
            e.printStackTrace();
            halt(500, SparkUtils.formatJSON("Error serving GTFS file"));
        }

        return res.raw();
    }

    /**
     * Merge the specified table for multiple GTFS feeds.
     * @param tableNode tableNode to merge
     * @param feedSourceMap map of feedSources to zipFiles from which to extract the .txt tables
     * @return single merged table for feeds
     */
    private static byte[] mergeTables(JsonNode tableNode, Map<FeedSource, ZipFile> feedSourceMap) {

        String tableName = tableNode.get("name").asText();
        ByteArrayOutputStream tableOut = new ByteArrayOutputStream();

//        int feedIndex = 0;

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

        try {
            // write headers to table
            tableOut.write(String.join(",", headers).getBytes());
            tableOut.write("\n".getBytes());

            // iterate over feed source to zipfile map
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

//                    int rowIndex = 0;

                        // iterate over rows in table
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

                            // write line to table (plus new line char)
                            tableOut.write(newLine.getBytes());
                            tableOut.write("\n".getBytes());
//                        rowIndex++;
                        }
                    }
                }
//            feedIndex++;
            }
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("Error merging feed sources: {}", feedSourceMap.keySet().stream().map(fs -> fs.name).collect(Collectors.toList()).toString());
            halt(400, SparkUtils.formatJSON("Error merging feed sources", 400, e));
        }
        return tableOut.toByteArray();
    }

    private static boolean deployPublic(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        String id = req.params("id");
        if (id == null) {
            halt(400, "must provide project id!");
        }
        Project p = Project.get(id);

        if (p == null)
            halt(400, "no such project!");

        // run as sync job; if it gets too slow change to async
        new MakePublicJob(p, userProfile.getUser_id()).run();
        return true;
    }

    private static Project thirdPartySync(Request req, Response res) throws Exception {
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
            if (p == null) return null;

            ZoneId timezone;
            try {
                timezone = ZoneId.of(timezoneId);
            }catch(Exception e){
                timezone = ZoneId.of("America/New_York");
            }
            LOG.info("Scheduling auto-fetch for projectID: {}", p.id);

            // NOW in default timezone
            ZonedDateTime now = ZonedDateTime.ofInstant(Instant.now(), timezone);

            // SCHEDULED START TIME
            ZonedDateTime startTime = LocalDateTime.of(LocalDate.now(), LocalTime.of(hour, minute)).atZone(timezone);
            LOG.info("Now: {}", now.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
            LOG.info("Scheduled start time: {}", startTime.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));

            // Get diff between start time and current time
            long diffInMinutes = (startTime.toEpochSecond() - now.toEpochSecond()) / 60;
            long delayInMinutes;
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

    private static void cancelAutoFetch(String id){
        Project p = Project.get(id);
        if ( p != null && DataManager.autoFetchMap.get(p.id) != null) {
            LOG.info("Cancelling auto-fetch for projectID: {}", p.id);
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
