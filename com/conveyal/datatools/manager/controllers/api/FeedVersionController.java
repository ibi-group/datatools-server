package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.BuildTransportNetworkJob;
import com.conveyal.datatools.manager.jobs.CreateFeedVersionFromSnapshotJob;
import com.conveyal.datatools.manager.jobs.ProcessSingleFeedJob;
import com.conveyal.datatools.manager.jobs.ReadTransportNetworkJob;
import com.conveyal.datatools.manager.models.FeedDownloadToken;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.conveyal.r5.analyst.PointSet;
import com.conveyal.r5.analyst.cluster.AnalystClusterRequest;
import com.conveyal.r5.analyst.cluster.ResultEnvelope;
import com.conveyal.r5.analyst.cluster.TaskStatistics;
import com.conveyal.r5.api.util.LegMode;
import com.conveyal.r5.api.util.TransitModes;
import com.conveyal.r5.profile.ProfileRequest;
import com.conveyal.r5.profile.RepeatedRaptorProfileRouter;
import com.conveyal.r5.profile.StreetMode;
import com.conveyal.r5.streets.LinkedPointSet;
import com.conveyal.r5.transit.TransportNetwork;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletException;
import javax.servlet.http.Part;

import static com.conveyal.datatools.manager.controllers.api.FeedSourceController.requestFeedSource;
import static spark.Spark.*;

public class FeedVersionController  {
    enum Permission {
        VIEW, MANAGE
    }
    public static final Logger LOG = LoggerFactory.getLogger(FeedVersionController.class);
    private static ObjectMapper mapper = new ObjectMapper();
    public static JsonManager<FeedVersion> json =
            new JsonManager<FeedVersion>(FeedVersion.class, JsonViews.UserInterface.class);
    private static Set<String> readingNetworkVersionList = new HashSet<>();

    /**
     * Grab this feed version.
     * If you pass in ?summarized=true, don't include the full tree of validation results, only the counts.
     */
    public static FeedVersion getFeedVersion (Request req, Response res) throws JsonProcessingException {
        FeedVersion v = requestFeedVersion(req, "view");

        return v;
    }

    public static Collection<FeedVersion> getAllFeedVersions (Request req, Response res) throws JsonProcessingException {
        Auth0UserProfile userProfile = req.attribute("user");
        FeedSource s = requestFeedSourceById(req, "view");

        return s.getFeedVersions().stream()
                .collect(Collectors.toCollection(ArrayList::new));
    }
    private static FeedSource requestFeedSourceById(Request req, String action) {
        String id = req.queryParams("feedSourceId");
        if (id == null) {
            halt("Please specify feedsourceId param");
        }
        return requestFeedSource(req, FeedSource.get(id), action);
    }

    /**
     * Upload a feed version directly. This is done behind Backbone's back, and as such uses
     * x-multipart-formdata rather than a json blob. This is done because uploading files in a JSON
     * blob is not pretty, and we don't really need to get the Backbone object directly; page re-render isn't
     * a problem.
     * @return
     * @throws JsonProcessingException
     */
    public static Boolean createFeedVersion (Request req, Response res) throws IOException, ServletException {

        Auth0UserProfile userProfile = req.attribute("user");
        FeedSource s = requestFeedSourceById(req, "manage");

        if (FeedSource.FeedRetrievalMethod.FETCHED_AUTOMATICALLY.equals(s.retrievalMethod)) {
            halt(400, "Feed is autofetched! Cannot upload.");
        }
        FeedVersion latest = s.getLatest();
        FeedVersion v = new FeedVersion(s);
        v.setUser(userProfile);

        if (req.raw().getAttribute("org.eclipse.jetty.multipartConfig") == null) {
            MultipartConfigElement multipartConfigElement = new MultipartConfigElement(System.getProperty("java.io.tmpdir"));
            req.raw().setAttribute("org.eclipse.jetty.multipartConfig", multipartConfigElement);
        }

        Part part = req.raw().getPart("file");

        LOG.info("Saving feed from upload {}", s);


        InputStream uploadStream;
        try {
            uploadStream = part.getInputStream();
            v.newGtfsFile(uploadStream);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Unable to open input stream from upload");
            halt(400, "Unable to read uploaded feed");
        }

        v.hash();

        // Check that hashes don't match (as long as v and latest are not the same entry)
        if (latest != null && latest.hash.equals(v.hash)) {
            LOG.error("Upload matches latest version.");
            v.getGtfsFile().delete();
            // Uploaded feed is same as latest version
            v.delete();
            halt(304);
        }

        v.name = v.getFormattedTimestamp() + " Upload";

        v.save();
        new ProcessSingleFeedJob(v, userProfile.getUser_id()).run();

        /*if (DataManager.config.get("modules").get("validator").get("enabled").asBoolean()) {
            BuildTransportNetworkJob btnj = new BuildTransportNetworkJob(v);
            Thread tnThread = new Thread(btnj);
            tnThread.start();
        }*/

        return true;
    }

    public static Boolean createFeedVersionFromSnapshot (Request req, Response res) throws IOException, ServletException {

        Auth0UserProfile userProfile = req.attribute("user");
        // TODO: should this be edit privelege?
        FeedSource s = requestFeedSourceById(req, "manage");
        FeedVersion v = new FeedVersion(s);
        CreateFeedVersionFromSnapshotJob createFromSnapshotJob =
                new CreateFeedVersionFromSnapshotJob(v, req.queryParams("snapshotId"), userProfile.getUser_id());
        createFromSnapshotJob.addNextJob(new ProcessSingleFeedJob(v, userProfile.getUser_id()));
        new Thread(createFromSnapshotJob).start();

        return true;
    }

    public static FeedVersion deleteFeedVersion(Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, "manage");

        version.delete();

        // renumber the versions
        Collection<FeedVersion> versions = version.getFeedSource().getFeedVersions();
        FeedVersion[] versionArray = versions.toArray(new FeedVersion[versions.size()]);
        Arrays.sort(versionArray, (v1, v2) -> v1.updated.compareTo(v2.updated));
        for(int i = 0; i < versionArray.length; i++) {
            FeedVersion v = versionArray[i];
            v.version = i + 1;
            v.save();
        }

        return version;
    }

    private static FeedVersion requestFeedVersion(Request req, String action) {
        String id = req.params("id");

        FeedVersion version = FeedVersion.get(id);
        if (version == null) {
            halt(404, "Version ID does not exist");
        }
        // performs permissions checks for at feed source level and halts if any issues
        FeedSource s = requestFeedSource(req, version.getFeedSource(), "manage");
        return version;
    }

    public static Object getValidationResult(Request req, Response res) {
        return getValidationResult(req, res, false);
    }

    public static Object getPublicValidationResult(Request req, Response res) {
        return getValidationResult(req, res, true);
    }

    public static JsonNode getValidationResult(Request req, Response res, boolean checkPublic) {
        FeedVersion version = requestFeedVersion(req, "view");

        return version.getValidationResult();
    }

    public static JsonNode getIsochrones(Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, "view");

        Auth0UserProfile userProfile = req.attribute("user");
        // if tn is null, check first if it's being built, else try reading in tn
        if (version.transportNetwork == null) {
            buildOrReadTransportNetwork(version, userProfile);
        }
        else {
            // remove version from list of reading network
            if (readingNetworkVersionList.contains(version.id)) {
                readingNetworkVersionList.remove(version.id);
            }
            AnalystClusterRequest clusterRequest = buildProfileRequest(req);
            return getRouterResult(version.transportNetwork, clusterRequest);
        }
        return null;
    }

    private static void buildOrReadTransportNetwork(FeedVersion version, Auth0UserProfile userProfile) {
        InputStream is = null;
        try {
            if (!readingNetworkVersionList.contains(version.id)) {
                is = new FileInputStream(version.getTransportNetworkPath());
                readingNetworkVersionList.add(version.id);
                try {
//                    version.transportNetwork = TransportNetwork.read(is);
                    ReadTransportNetworkJob rtnj = new ReadTransportNetworkJob(version, userProfile.getUser_id());
                    Thread readThread = new Thread(rtnj);
                    readThread.start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            halt(202, "Try again later. Reading transport network");
        }
        // Catch exception if transport network not built yet
        catch (Exception e) {
            if (DataManager.isModuleEnabled("validator")) {
                LOG.warn("Transport network not found. Beginning build.");
                BuildTransportNetworkJob btnj = new BuildTransportNetworkJob(version, userProfile.getUser_id());
                Thread tnThread = new Thread(btnj);
                tnThread.start();
            }
            halt(202, "Try again later. Building transport network");
        }
    }

    private static JsonNode getRouterResult(TransportNetwork transportNetwork, AnalystClusterRequest clusterRequest) {
        PointSet targets = transportNetwork.getGridPointSet();
        StreetMode mode = StreetMode.WALK;
        final LinkedPointSet linkedTargets = targets.link(transportNetwork.streetLayer, mode);
        RepeatedRaptorProfileRouter router = new RepeatedRaptorProfileRouter(transportNetwork, clusterRequest, linkedTargets, new TaskStatistics());
        ResultEnvelope result = router.route();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            JsonGenerator jgen = new JsonFactory().createGenerator(out);
            jgen.writeStartObject();
            result.avgCase.writeIsochrones(jgen);
            jgen.writeEndObject();
            jgen.close();
            out.close();
            String outString = new String( out.toByteArray(), StandardCharsets.UTF_8 );
            return mapper.readTree(outString);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static AnalystClusterRequest buildProfileRequest(Request req) {
        // required fields?
        Double fromLat = Double.valueOf(req.queryParams("fromLat"));
        Double fromLon = Double.valueOf(req.queryParams("fromLon"));
        Double toLat = Double.valueOf(req.queryParams("toLat"));
        Double toLon = Double.valueOf(req.queryParams("toLon"));
        LocalDate date = req.queryParams("date") != null ? LocalDate.parse(req.queryParams("date"), DateTimeFormatter.ISO_LOCAL_DATE) : LocalDate.now(); // 2011-12-03

        // optional with defaults
        Integer fromTime = req.queryParams("fromTime") != null ? Integer.valueOf(req.queryParams("fromTime")) : 9 * 3600;
        Integer toTime = req.queryParams("toTime") != null ? Integer.valueOf(req.queryParams("toTime")) : 10 * 3600;

        // build request with transit as default mode
        AnalystClusterRequest clusterRequest = new AnalystClusterRequest();
        clusterRequest.profileRequest = new ProfileRequest();
        clusterRequest.profileRequest.transitModes = EnumSet.of(TransitModes.TRANSIT);
        clusterRequest.profileRequest.accessModes = EnumSet.of(LegMode.WALK);
        clusterRequest.profileRequest.date = date;
        clusterRequest.profileRequest.fromLat = fromLat;
        clusterRequest.profileRequest.fromLon = fromLon;
        clusterRequest.profileRequest.toLat = toLat;
        clusterRequest.profileRequest.toLon = toLon;
        clusterRequest.profileRequest.fromTime = fromTime;
        clusterRequest.profileRequest.toTime = toTime;
        clusterRequest.profileRequest.egressModes = EnumSet.of(LegMode.WALK);
        clusterRequest.profileRequest.zoneId = ZoneId.of("America/New_York");

        return clusterRequest;
    }

    private static Object downloadFeedVersion(FeedVersion version, Response res) {
        if(version == null) halt(500, "FeedVersion is null");

        File file = version.getGtfsFile();

        res.raw().setContentType("application/octet-stream");
        res.raw().setHeader("Content-Disposition", "attachment; filename=" + file.getName());

        try {
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(res.raw().getOutputStream());
            BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(file));

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

    public static Boolean renameFeedVersion (Request req, Response res) throws JsonProcessingException {
        FeedVersion v = requestFeedVersion(req, "manage");

        String name = req.queryParams("name");
        if (name == null) {
            halt(400, "Name parameter not specified");
        }

        v.name = name;
        v.save();
        return true;
    }

    private static Object downloadFeedVersionDirectly(Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, "view");
        return downloadFeedVersion(version, res);
    }

    private static FeedDownloadToken getDownloadToken (Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, "view");
        FeedDownloadToken token = new FeedDownloadToken(version);
        token.save();
        return token;
    }

    private static FeedDownloadToken getPublicDownloadToken (Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, "view");
        if(!version.getFeedSource().isPublic) {
            halt(401, "Not a public feed");
            return null;
        }
        FeedDownloadToken token = new FeedDownloadToken(version);
        token.save();
        return token;
    }
    private static FeedVersion publishToExternalResource (Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, "manage");

        // notify any extensions of the change
        for(String resourceType : DataManager.feedResources.keySet()) {
            DataManager.feedResources.get(resourceType).feedVersionCreated(version, null);
        }
        FeedSource fs = version.getFeedSource();
        fs.publishedVersionId = version.id;
        fs.save();
        return version;
    }
    private static Object downloadFeedVersionWithToken (Request req, Response res) {
        FeedDownloadToken token = FeedDownloadToken.get(req.params("token"));

        if(token == null || !token.isValid()) {
            halt(400, "Feed download token not valid");
        }

        FeedVersion version = token.getFeedVersion();

        token.delete();

        return downloadFeedVersion(version, res);
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/feedversion/:id", FeedVersionController::getFeedVersion, json::write);
        get(apiPrefix + "secure/feedversion/:id/download", FeedVersionController::downloadFeedVersionDirectly);
        get(apiPrefix + "secure/feedversion/:id/downloadtoken", FeedVersionController::getDownloadToken, json::write);
        get(apiPrefix + "secure/feedversion/:id/validation", FeedVersionController::getValidationResult, json::write);
        get(apiPrefix + "secure/feedversion/:id/isochrones", FeedVersionController::getIsochrones, json::write);
        get(apiPrefix + "secure/feedversion", FeedVersionController::getAllFeedVersions, json::write);
        post(apiPrefix + "secure/feedversion", FeedVersionController::createFeedVersion, json::write);
        post(apiPrefix + "secure/feedversion/fromsnapshot", FeedVersionController::createFeedVersionFromSnapshot, json::write);
        put(apiPrefix + "secure/feedversion/:id/rename", FeedVersionController::renameFeedVersion, json::write);
        post(apiPrefix + "secure/feedversion/:id/publish", FeedVersionController::publishToExternalResource, json::write);
        delete(apiPrefix + "secure/feedversion/:id", FeedVersionController::deleteFeedVersion, json::write);

        get(apiPrefix + "public/feedversion", FeedVersionController::getAllFeedVersions, json::write);
        get(apiPrefix + "public/feedversion/:id/validation", FeedVersionController::getPublicValidationResult, json::write);
        get(apiPrefix + "public/feedversion/:id/downloadtoken", FeedVersionController::getPublicDownloadToken, json::write);

        get(apiPrefix + "downloadfeed/:token", FeedVersionController::downloadFeedVersionWithToken);

    }
}
