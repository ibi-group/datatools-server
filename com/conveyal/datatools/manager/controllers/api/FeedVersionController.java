package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.jobs.BuildTransportNetworkJob;
import com.conveyal.datatools.manager.jobs.ProcessSingleFeedJob;
import com.conveyal.datatools.manager.models.FeedDownloadToken;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.conveyal.gtfs.validator.json.FeedValidationResult;
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
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletException;
import javax.servlet.http.Part;

import static spark.Spark.*;

public class FeedVersionController  {

    public static final Logger LOG = LoggerFactory.getLogger(FeedVersionController.class);

    public static JsonManager<FeedVersion> json =
            new JsonManager<FeedVersion>(FeedVersion.class, JsonViews.UserInterface.class);

    /**
     * Grab this feed version.
     * If you pass in ?summarized=true, don't include the full tree of validation results, only the counts.
     */
    public static FeedVersion getFeedVersion (Request req, Response res) throws JsonProcessingException {

        FeedVersion v = FeedVersion.get(req.params("id"));
        FeedSource s = v.getFeedSource();

        return v;
        // ways to have permission to do this:
        // 1) be an admin
        // 2) have access to this feed through project permissions
        /*if (userProfile.canAdministerProject(s.feedCollectionId) || userProfile.canViewFeed(s.feedCollectionId, s.id)) {
            if ("true".equals(request().getQueryString("summarized"))) {
                return ok(jsonSummarized.write(new SummarizedFeedVersion(v))).as("application/json");
            }
            else {
                return ok(json.write(v)).as("application/json");
            }
        }
        else {
            return unauthorized();
        }*/
    }

    /**
     * Grab this feed version's GTFS.
     */
    /*public static Result getGtfs (String id) throws JsonProcessingException {
        Auth0UserProfile userProfile = getSessionProfile();
        if(userProfile == null) return unauthorized();

        FeedVersion v = FeedVersion.get(id);
        FeedSource s = v.getFeedSource();

        if (userProfile.canAdministerProject(s.feedCollectionId) || userProfile.canViewFeed(s.feedCollectionId, s.id)) {
            return ok(v.getFeed());
        }
        else {
            return unauthorized();
        }
    }*/


    public static Collection<FeedVersion> getAllFeedVersions (Request req, Response res) throws JsonProcessingException {

        // parse the query parameters
        String sourceId = req.queryParams("feedSourceId");
        if (sourceId == null) {
            halt("Please specify a feedsource");
        }
        Boolean publicFilter = Boolean.valueOf(req.queryParams("public"));

        FeedSource s = FeedSource.get(sourceId);

        Collection<FeedVersion> versions = new ArrayList<>();

        for (FeedVersion v : s.getFeedVersions()){
            // if requesting public sources and source is not public; skip source
            if (publicFilter && !s.isPublic)
                continue;
            versions.add(v);
        }

        return versions;
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


        FeedSource s = FeedSource.get(req.queryParams("feedSourceId"));

        if (FeedSource.FeedRetrievalMethod.FETCHED_AUTOMATICALLY.equals(s.retrievalMethod))
            halt("Feed is autofetched! Cannot upload.");

        FeedVersion v = new FeedVersion(s);
        //v.setUser(req.attribute("auth0User"));

        if (req.raw().getAttribute("org.eclipse.jetty.multipartConfig") == null) {
            MultipartConfigElement multipartConfigElement = new MultipartConfigElement(System.getProperty("java.io.tmpdir"));
            req.raw().setAttribute("org.eclipse.jetty.multipartConfig", multipartConfigElement);
        }

        Part part = req.raw().getPart("file");

        LOG.info("Saving feed {} from upload", s);


        InputStream uploadStream;
        try {
            uploadStream = part.getInputStream();
            v.newFeed(uploadStream);
        } catch (Exception e) {
            LOG.error("Unable to open input stream from upload");
            halt("Unable to read uploaded feed");
        }

        v.hash();

        FeedVersion latest = s.getLatest();
        if (latest != null && latest.hash.equals(v.hash)) {
            v.getFeed().delete();
            return null;
        }

        // for now run sychronously so the user sees something after the redirect
        // it's pretty fast
        new ProcessSingleFeedJob(v).run();

        if (DataManager.config.get("modules").get("validator").get("enabled").asBoolean()) {
            new BuildTransportNetworkJob(v).run();
        }

        return true;
    }

    public static FeedVersion deleteFeedVersion(Request req, Response res) {
        String id = req.params("id");
        FeedVersion version = FeedVersion.get(id);
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

    public static FeedValidationResult getValidationResult(Request req, Response res) {
        String id = req.params("id");
        FeedVersion version = FeedVersion.get(id);
        return version.validationResult;
    }

    public static Object getIsochrones(Request req, Response res) {
        LOG.info(req.uri());

        String id = req.params("id");
        FeedVersion version = FeedVersion.get(id);
        Double fromLat = Double.valueOf(req.queryParams("fromLat"));
        Double fromLon = Double.valueOf(req.queryParams("fromLon"));
        Double toLat = Double.valueOf(req.queryParams("toLat"));
        Double toLon = Double.valueOf(req.queryParams("toLon"));

        if (version.transportNetwork == null) {
            InputStream is = null;
            try {
                is = new FileInputStream(DataManager.config.get("application").get("data").get("gtfs").asText() + "/"  + version.feedSourceId + "/" + version.id + "_network.dat");
                version.transportNetwork = TransportNetwork.read(is);
            } catch (Exception e) {
                e.printStackTrace();
                if (DataManager.config.get("modules").get("validator").get("enabled").asBoolean()) {
                    new BuildTransportNetworkJob(version).run();
                }
                halt(503, "Try again later. Building transport network");
            }
        }

        if (version.transportNetwork != null) {
            AnalystClusterRequest clusterRequest = new AnalystClusterRequest();
            clusterRequest.profileRequest = new ProfileRequest();
            clusterRequest.profileRequest.transitModes = EnumSet.of(TransitModes.TRANSIT);
            clusterRequest.profileRequest.accessModes = EnumSet.of(LegMode.WALK);
            clusterRequest.profileRequest.date = LocalDate.now();
            clusterRequest.profileRequest.fromLat = fromLat;
            clusterRequest.profileRequest.fromLon = fromLon;
            clusterRequest.profileRequest.toLat = toLat;
            clusterRequest.profileRequest.toLon = toLon;
            clusterRequest.profileRequest.fromTime = 9*3600;
            clusterRequest.profileRequest.toTime = 10*3600;
            clusterRequest.profileRequest.egressModes = EnumSet.of(LegMode.WALK);
            clusterRequest.profileRequest.zoneId = ZoneId.of("America/New_York");
            PointSet targets = version.transportNetwork.getGridPointSet();
            StreetMode mode = StreetMode.WALK;
            final LinkedPointSet linkedTargets = targets.link(version.transportNetwork.streetLayer, mode);
            RepeatedRaptorProfileRouter router = new RepeatedRaptorProfileRouter(version.transportNetwork, clusterRequest, linkedTargets, new TaskStatistics());
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
//            return outString;
                System.out.println(outString);
                ObjectMapper mapper = new ObjectMapper();
                return mapper.readTree(outString);
            } catch (IOException e) {
                e.printStackTrace();
            }


        }
        return null;
    }

    private static Object downloadFeedVersion(FeedVersion version, Response res) {
        if(version == null) halt(500, "FeedVersion is null");

        File file = version.getFeed();

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

    private static Object downloadFeedVersionDirectly(Request req, Response res) {
        FeedVersion version = FeedVersion.get(req.params("id"));
        return downloadFeedVersion(version, res);
    }

    private static FeedDownloadToken getDownloadToken (Request req, Response res) {
        FeedVersion version = FeedVersion.get(req.params("id"));
        FeedDownloadToken token = new FeedDownloadToken(version);
        token.save();
        return token;
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
        delete(apiPrefix + "secure/feedversion/:id", FeedVersionController::deleteFeedVersion, json::write);

        get(apiPrefix + "public/feedversion", FeedVersionController::getAllFeedVersions, json::write);
        get(apiPrefix + "downloadfeed/:token", FeedVersionController::downloadFeedVersionWithToken);

    }

}
