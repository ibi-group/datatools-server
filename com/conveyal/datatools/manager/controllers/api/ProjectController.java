package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.jobs.FetchProjectFeedsJob;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.*;

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

    public static Collection<Project> getAllProjectsWithPublicFeeds(Request req, Response res) throws JsonProcessingException {
        Collection<Project> filteredProjects = new ArrayList<Project>();

        System.out.println("found projects: " + Project.getAll().size());
        for (Project proj : Project.getAll()) {
            /*if (userProfile.canAdministerApplication() || userProfile.hasProject(proj.id)) {
                filteredFCs.add(proj);
            }*/
            Collection<FeedSource> feedSources = new ArrayList<>();
            for (FeedSource fs : proj.getFeedSources()){
                if (fs.isPublic){
                    feedSources.add(fs);
                }
            }
            if (!feedSources.isEmpty()) {
                proj.feedSources = feedSources;
                filteredProjects.add(proj);
            }
        }

        return filteredProjects;
    }

    public static Project getProject(Request req, Response res) {
        String id = req.params("id");
        return Project.get(id);
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

        switch (syncType) {
            case "MTC":
                return pullFromRtd(proj);
            case "TRANSITLAND":
                return pullFromTransitLand(proj);
            case "TRANSITFEEDS":
                return pullFromTransitFeeds(proj);
            default:
                halt(404);
                return null;
        }
    }

    public static Project pullFromTransitLand(Project proj) throws Exception {
        URL url;
        ObjectMapper mapper = new ObjectMapper();
        String locationFilter = "";
        if (proj.north != null && proj.south != null && proj.east != null && proj.west != null)
            locationFilter = "&bbox=" + proj.west + "," + + proj.south + "," + proj.east + "," + proj.north;
        url = new URL(
                DataManager.config.getProperty("application.extensions.transitland.api") + "?per_page=10000" + locationFilter
        );
        HttpURLConnection con = (HttpURLConnection) url.openConnection();

        // optional default is GET
        con.setRequestMethod("GET");

        //add request header
        con.setRequestProperty("User-Agent", "User-Agent");

        int responseCode = con.getResponseCode();
        System.out.println("\nSending 'GET' request to URL : " + url);
        System.out.println("Response Code : " + responseCode);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        String json = response.toString();
        JsonNode node = mapper.readTree(json);

        for (JsonNode feed : node.get("feeds")) {
            TransitLandFeed car = new TransitLandFeed(feed);

            FeedSource source = null;
            for (FeedSource existingSource : proj.getFeedSources()) {
                if (car.onestop_id.equals(existingSource.onestop_id)) {
                    source = existingSource;
                }
            }
            String feedName;
            feedName = car.onestop_id;

            if (source == null) source = new FeedSource(FeedSource.FeedSourceType.TRANSITLAND, feedName);
            else source.name = feedName;
            car.mapFeedSource(source);

            source.setName(feedName);
            System.out.println(source.name);

            source.setProject(proj);

            source.save();
        }
        return proj;
    }

    public static Project pullFromRtd(Project proj) throws Exception {
        URL url;
        ObjectMapper mapper = new ObjectMapper();
        // single list from MTC
        url = new URL(DataManager.config.getProperty("application.extensions.rtd_integration.api"));
        HttpURLConnection con = (HttpURLConnection) url.openConnection();

        // optional default is GET
        con.setRequestMethod("GET");

        //add request header
        con.setRequestProperty("User-Agent", "User-Agent");

        int responseCode = con.getResponseCode();
        System.out.println("\nSending 'GET' request to URL : " + url);
        System.out.println("Response Code : " + responseCode);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        String json = response.toString();
        System.out.println(json);
        RtdCarrier[] results = mapper.readValue(json, RtdCarrier[].class);
        for (int i = 0; i < results.length; i++) {
//                    String className = "RtdCarrier";
//                    Object car = Class.forName(className).newInstance();
            RtdCarrier car = results[i];
            System.out.println("car id=" + car.AgencyId + " name=" + car.AgencyName);

            FeedSource source = null;
            for (FeedSource existingSource : proj.getFeedSources()) {
                if (car.AgencyId.equals(existingSource.defaultGtfsId)) {
                    System.out.println("already exists: " + car.AgencyId);
                    source = existingSource;
                }
            }
            String feedName;
            if (car.AgencyName != null) {
                feedName = car.AgencyName;
            } else if (car.AgencyShortName != null) {
                feedName = car.AgencyShortName;
            } else {
                feedName = car.AgencyId;
            }
            if (source == null) source = new FeedSource(FeedSource.FeedSourceType.MTC, feedName);
            else source.name = feedName;

            // get map method for rtd class
//                    mapMethod = car.getClass().getMethod("mapFeedSource", FeedSource.class);
            car.mapFeedSource(source);
            source.setName(feedName);
            System.out.println(source.name);

            source.setProject(proj);

            source.save();
        }
        return proj;
    }

    public static Project pullFromTransitFeeds(Project proj) throws Exception {
        URL url;
        ObjectMapper mapper = new ObjectMapper();
        // multiple pages for transitfeeds because of 100 feed limit per page
        Boolean nextPage = true;
        int count = 1;
        String tfKey =  DataManager.config.getProperty("application.extensions.transitfeeds.key");
        do {
            url = new URL(
                    DataManager.config.getProperty("application.extensions.transitfeeds.api") +
                            "?key=" + tfKey + "&limit=100" + "&page=" + String.valueOf(count)
            );
            HttpURLConnection con = (HttpURLConnection) url.openConnection();

            // optional default is GET
            con.setRequestMethod("GET");

            //add request header
            con.setRequestProperty("User-Agent", "User-Agent");

            int responseCode = con.getResponseCode();
            System.out.println("\nSending 'GET' request to URL : " + url);
            System.out.println("Response Code : " + responseCode);

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            String json = response.toString();
//            System.out.println(json);
            JsonNode transitFeedNode = mapper.readTree(json);
//                System.out.println(node.get("feeds"));

            for (JsonNode feed : transitFeedNode.get("results").get("feeds")) {

                // test that feed is in fact GTFS
                if (!feed.get("t").asText().contains("GTFS")){
//                    System.out.println("no gtfs; skipping feed");
                    continue;
                }

                // test that feed falls in bounding box (if box exists)
                if (proj.north != null) {
                    Double lat = feed.get("l").get("lat").asDouble();
                    Double lng = feed.get("l").get("lng").asDouble();
                    if (lat < proj.south || lat > proj.north || lng < proj.west || lng > proj.east) {
//                        System.out.println("out of bounds; skipping feed " + lat + " " + lng);
                        continue;
                    }
                }
                    FeedSource car = new FeedSource();
                    car.onestop_id = feed.get("id").asText();
//                System.out.println("transitfeed_id=" + car.onestop_id);

                    FeedSource source = null;
                    for (FeedSource existingSource : proj.getFeedSources()) {
                        if (car.onestop_id.equals(existingSource.onestop_id)) {
                            System.out.println("already exists: " + car.onestop_id);
                            source = existingSource;
                        }
                    }
                    String feedName;
                    feedName = feed.get("t").asText();

                    if (source == null) source = new FeedSource(FeedSource.FeedSourceType.TRANSITLAND, feedName);
                    else source.name = feedName;
//                    car.mapFeedSource(source);

                    source.retrievalMethod = FeedSource.FeedRetrievalMethod.FETCHED_AUTOMATICALLY;
                    source.setName(feedName);
                    System.out.println(source.name);

                    if (feed.get("u") != null) {
                        if (feed.get("u").get("d") != null)
                            source.url = new URL(feed.get("u").get("d").asText());
                        else if (feed.get("u").get("i") != null)
                            source.url = new URL(feed.get("u").get("i").asText());
                    }

                    source.setProject(proj);

                    source.save();


            }
            if (transitFeedNode.get("results").get("page") == transitFeedNode.get("results").get("numPages")){
                LOG.info("finished last page of transitfeeds");
                nextPage = false;
            }
            count++;
        }while(nextPage);

        return proj;
    }

    public static void register (String apiPrefix) {
        options(apiPrefix + "secure/project/:id", (q, s) -> "");
        get(apiPrefix + "secure/project/:id", ProjectController::getProject, JsonUtil.objectMapper::writeValueAsString);
        get(apiPrefix + "secure/project", ProjectController::getAllProjects, JsonUtil.objectMapper::writeValueAsString);
        post(apiPrefix + "secure/project", ProjectController::createProject, JsonUtil.objectMapper::writeValueAsString);
        put(apiPrefix + "secure/project/:id", ProjectController::updateProject, JsonUtil.objectMapper::writeValueAsString);
        delete(apiPrefix + "secure/project/:id", ProjectController::deleteProject, JsonUtil.objectMapper::writeValueAsString);
        get(apiPrefix + "secure/project/:id/thirdPartySync/:type", ProjectController::thirdPartySync, JsonUtil.objectMapper::writeValueAsString);
        post(apiPrefix + "secure/project/:id/fetch", ProjectController::fetch, JsonUtil.objectMapper::writeValueAsString);

        get(apiPrefix + "public/project/:id", ProjectController::getProject, JsonUtil.objectMapper::writeValueAsString);
        get(apiPrefix + "public/project", ProjectController::getAllProjectsWithPublicFeeds, JsonUtil.objectMapper::writeValueAsString);
    }

    public static class RtdCarrier {

        @JsonProperty
        String AgencyId;

        @JsonProperty
        String AgencyName;

        @JsonProperty
        String AgencyPhone;

        @JsonProperty
        String RttAgencyName;

        @JsonProperty
        String RttEnabled;

        @JsonProperty
        String AgencyShortName;

        @JsonProperty
        String AgencyPublicId;

        @JsonProperty
        String AddressLat;

        @JsonProperty
        String AddressLon;

        @JsonProperty
        String DefaultRouteType;

        @JsonProperty
        String CarrierStatus;

        @JsonProperty
        String AgencyAddress;

        @JsonProperty
        String AgencyEmail;

        @JsonProperty
        String AgencyUrl;

        @JsonProperty
        String AgencyFareUrl;

        @JsonProperty
        String EditedBy;

        @JsonProperty
        String EditedDate;

        public RtdCarrier() {
        }

        public void mapFeedSource(FeedSource source){
            source.defaultGtfsId = this.AgencyId;
            source.shortName = this.AgencyShortName;
            source.AgencyPhone = this.AgencyPhone;
            source.RttAgencyName = this.RttAgencyName;
            source.RttEnabled = this.RttEnabled;
            source.AgencyShortName = this.AgencyShortName;
            source.AgencyPublicId = this.AgencyPublicId;
            source.AddressLat = this.AddressLat;
            source.AddressLon = this.AddressLon;
            source.DefaultRouteType = this.DefaultRouteType;
            source.CarrierStatus = this.CarrierStatus;
            source.AgencyAddress = this.AgencyAddress;
            source.AgencyEmail = this.AgencyEmail;
            source.AgencyUrl = this.AgencyUrl;
            source.AgencyFareUrl = this.AgencyFareUrl;

            source.save();
        }
    }

    public static class TransitLandFeeds {
        @JsonProperty
        List<TransitLandFeed> feeds = new ArrayList<TransitLandFeed>();

//        @JsonProperty
//        Map

//        @JsonProperty
//        List<TransitLandFeed> feeds;

        @JsonProperty
        Map<String, String> meta;
    }
    public static class TransitLandFeed {

        @JsonProperty
        String onestop_id;

        @JsonProperty
        String url;

        @JsonProperty
        String feed_format;

        @JsonProperty
        String tags;

        @JsonIgnore
        String geometry;

        @JsonIgnore
        String type;

        @JsonIgnore
        String coordinates;

        @JsonProperty
        String license_name;

        @JsonProperty
        String license_url;

        @JsonProperty
        String license_use_without_attribution;

        @JsonProperty
        String license_create_derived_product;

        @JsonProperty
        String license_redistribute;

        @JsonProperty
        String license_attribution_text;

        @JsonProperty
        String last_fetched_at;

        @JsonProperty
        String last_imported_at;

        @JsonProperty
        String latest_fetch_exception_log;

        @JsonProperty
        String import_status;

        @JsonProperty
        String created_at;

        @JsonProperty
        String updated_at;

        @JsonProperty
        String feed_versions_count;

        @JsonProperty
        String feed_versions_url;

        @JsonProperty
        String[] feed_versions;

        @JsonProperty
        String active_feed_version;

        @JsonProperty
        String import_level_of_active_feed_version;

        @JsonProperty
        String created_or_updated_in_changeset_id;

        @JsonIgnore
        String changesets_imported_from_this_feed;

        @JsonIgnore
        String operators_in_feed;

        @JsonIgnore
        String gtfs_agency_id;

        @JsonIgnore
        String operator_onestop_id;

        @JsonIgnore
        String feed_onestop_id;

        @JsonIgnore
        String operator_url;

        @JsonIgnore
        String feed_url;

        public TransitLandFeed(JsonNode jsonMap){
            this.url = jsonMap.get("url").asText();
            this.onestop_id = jsonMap.get("onestop_id").asText();
            this.feed_format = jsonMap.get("feed_format").asText();
            this.tags = jsonMap.get("tags").asText();
            this.license_name = jsonMap.get("license_name").asText();
            this.license_url = jsonMap.get("license_url").asText();
            this.license_use_without_attribution = jsonMap.get("license_use_without_attribution").asText();
            this.license_create_derived_product = jsonMap.get("license_create_derived_product").asText();
            this.license_redistribute = jsonMap.get("license_redistribute").asText();
            this.license_attribution_text = jsonMap.get("license_attribution_text").asText();
            this.last_fetched_at = jsonMap.get("last_fetched_at").asText();
            this.last_imported_at = jsonMap.get("last_imported_at").asText();
            this.latest_fetch_exception_log = jsonMap.get("latest_fetch_exception_log").asText();
            this.import_status = jsonMap.get("import_status").asText();
            this.created_at = jsonMap.get("created_at").asText();
            this.updated_at = jsonMap.get("updated_at").asText();
            this.feed_versions_count = jsonMap.get("feed_versions_count").asText();
            this.feed_versions_url = jsonMap.get("feed_versions_url").asText();
//            this.feed_versions = jsonMap.get("feed_versions").asText();
            this.active_feed_version = jsonMap.get("active_feed_version").asText();
            this.import_level_of_active_feed_version = jsonMap.get("import_level_of_active_feed_version").asText();
            this.created_or_updated_in_changeset_id = jsonMap.get("created_or_updated_in_changeset_id").asText();
            this.changesets_imported_from_this_feed = jsonMap.get("changesets_imported_from_this_feed").asText();
            this.operators_in_feed = jsonMap.get("operators_in_feed").asText();
//            this.gtfs_agency_id = jsonMap.get("gtfs_agency_id").asText();
//            this.operator_onestop_id = jsonMap.get("operator_onestop_id").asText();
//            this.feed_onestop_id = jsonMap.get("feed_onestop_id").asText();
//            this.operator_url = jsonMap.get("operator_url").asText();
//            this.feed_url = jsonMap.get("feed_url").asText();
        }
        public void mapFeedSource(FeedSource source){
            source.onestop_id = this.onestop_id;
            source.retrievalMethod = FeedSource.FeedRetrievalMethod.FETCHED_AUTOMATICALLY;
//            System.out.println(this.url);
            try {
                source.url = new URL(this.url);
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }

            source.feed_format = this.feed_format;
            source.tags = this.tags;
            source.license_name = this.license_name;
            source.license_url = this.license_url;
            source.license_use_without_attribution = this.license_use_without_attribution;
            source.license_create_derived_product = this.license_create_derived_product;
            source.license_redistribute = this.license_redistribute;
            source.license_attribution_text = this.license_attribution_text;
//            source.last_fetched_at = this.last_fetched_at;
            source.last_imported_at = this.last_imported_at;
            source.latest_fetch_exception_log = this.latest_fetch_exception_log;
            source.import_status = this.import_status;
            source.created_at = this.created_at;
            source.updated_at = this.updated_at;
            source.feed_versions_count = this.feed_versions_count;
            source.feed_versions_url = this.feed_versions_url;
            source.feed_versions = this.feed_versions;
            source.active_feed_version = this.active_feed_version;
            source.import_level_of_active_feed_version = this.import_level_of_active_feed_version;
            source.created_or_updated_in_changeset_id = this.created_or_updated_in_changeset_id;
            source.changesets_imported_from_this_feed = this.changesets_imported_from_this_feed;
            source.operators_in_feed = this.operators_in_feed;
            source.gtfs_agency_id = this.gtfs_agency_id;
            source.operator_onestop_id = this.operator_onestop_id;
            source.feed_onestop_id = this.feed_onestop_id;
            source.operator_url = this.operator_url;
            source.feed_url = this.feed_url;

            source.save();
        }
    }
}
