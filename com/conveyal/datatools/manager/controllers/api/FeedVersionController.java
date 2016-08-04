package com.conveyal.datatools.manager.controllers.api;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.BuildTransportNetworkJob;
import com.conveyal.datatools.manager.jobs.CreateFeedVersionFromSnapshotJob;
import com.conveyal.datatools.manager.jobs.ProcessSingleFeedJob;
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
import java.util.*;

import com.fasterxml.jackson.databind.JsonNode;
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
    private static ObjectMapper mapper = new ObjectMapper();
    public static JsonManager<FeedVersion> json =
            new JsonManager<FeedVersion>(FeedVersion.class, JsonViews.UserInterface.class);

    /**
     * Grab this feed version.
     * If you pass in ?summarized=true, don't include the full tree of validation results, only the counts.
     */
    public static FeedVersion getFeedVersion (Request req, Response res) throws JsonProcessingException {
        String id = req.params("id");
        if (id == null) {
            halt(404, "Must specify feed version ID");
        }
        FeedVersion v = FeedVersion.get(id);

        if (v == null) {
            halt(404, "Version ID does not exist");
        }
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
            return ok(v.getGtfsFile());
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

        Auth0UserProfile userProfile = req.attribute("user");
        FeedSource s = FeedSource.get(req.queryParams("feedSourceId"));

        if (FeedSource.FeedRetrievalMethod.FETCHED_AUTOMATICALLY.equals(s.retrievalMethod))
            halt(400, "Feed is autofetched! Cannot upload.");

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
            v.newGtfsFile(uploadStream);
        } catch (Exception e) {
            LOG.error("Unable to open input stream from upload");
            halt(400, "Unable to read uploaded feed");
        }

        v.hash();

        FeedVersion latest = s.getLatest();
        if (latest != null && latest.hash.equals(v.hash)) {
            v.getGtfsFile().delete();
            // Uploaded feed is same as latest version
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
        FeedSource s = FeedSource.get(req.queryParams("feedSourceId"));

        CreateFeedVersionFromSnapshotJob createFromSnapshotJob =
                new CreateFeedVersionFromSnapshotJob(s, req.queryParams("snapshotId"), userProfile.getUser_id());

        new Thread(createFromSnapshotJob).start();

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

    public static Object getValidationResult(Request req, Response res) {
        String id = req.params("id");
        FeedVersion version = FeedVersion.get(id);
        // TODO: separate this out if non s3 bucket
        String s3Bucket = DataManager.config.get("application").get("data").get("gtfs_s3_bucket").asText();
        String keyName = "validation/" + version.id + ".json";
        JsonNode n = null;
        InputStream objectData = null;
        if (s3Bucket != null && DataManager.getConfigProperty("application.data.use_s3_storage").asBoolean() == true) {
            AWSCredentials creds;
            // default credentials providers, e.g. IAM role
            creds = new DefaultAWSCredentialsProviderChain().getCredentials();
            try {
                LOG.info("Getting validation results from s3");
                AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
                S3Object object = s3Client.getObject(
                        new GetObjectRequest(s3Bucket, keyName));
                objectData = object.getObjectContent();
            } catch (AmazonS3Exception e) {
                // if json file does not exist, validate feed.
                version.validate();
                version.save();
                halt(503, "Try again later. Validating feed");
            } catch (AmazonServiceException ase) {
                LOG.error("Error downloading from s3");
                ase.printStackTrace();
            }

        }
        // if s3 upload set to false
        else {
            File file = new File(DataManager.getConfigPropertyAsText("application.data.gtfs") + "/validation/" + version.id + ".json");
            try {
                objectData = new FileInputStream(file);
//            } catch (IOException e) {
//                e.printStackTrace();
            } catch (Exception e) {
                LOG.warn("Validation does not exist.  Validating feed.");
                version.validate();
                version.save();
                halt(503, "Try again later. Validating feed");
            }
        }

        // Process the objectData stream.
        try {
            n = mapper.readTree(objectData);
            if (!n.has("errors") || !n.has("tripsPerDate")) {
                throw new Exception("Validation for feed version not up to date");
            }

            return n;
        } catch (IOException e) {
            // if json file does not exist, validate feed.
            version.validate();
            version.save();
            halt(503, "Try again later. Validating feed");
        } catch (Exception e) {
            e.printStackTrace();
            version.validate();
            version.save();
            halt(503, "Try again later. Validating feed");
        }

        return null;
    }

    public static Object getIsochrones(Request req, Response res) {
        LOG.info(req.uri());
        Auth0UserProfile userProfile = req.attribute("user");

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
            }
            // Catch if transport network not built yet
            catch (Exception e) {
                e.printStackTrace();
                if (DataManager.config.get("modules").get("validator").get("enabled").asBoolean()) {
//                    new BuildTransportNetworkJob(version).run();
                    BuildTransportNetworkJob btnj = new BuildTransportNetworkJob(version, userProfile.getUser_id());
                    Thread tnThread = new Thread(btnj);
                    tnThread.start();
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
                //System.out.println(outString);
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
        String id = req.params("id");
        if (id == null) {
            halt(404, "Must specify feed version ID");
        }
        FeedVersion v = FeedVersion.get(id);

        if (v == null) {
            halt(404, "Version ID does not exist");
        }

        String name = req.queryParams("name");
        if (name == null) {
            halt(400, "Name parameter not specified");
        }

        v.name = name;
        v.save();
        return true;
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
        post(apiPrefix + "secure/feedversion/fromsnapshot", FeedVersionController::createFeedVersionFromSnapshot, json::write);
        put(apiPrefix + "secure/feedversion/:id/rename", FeedVersionController::renameFeedVersion, json::write);
        delete(apiPrefix + "secure/feedversion/:id", FeedVersionController::deleteFeedVersion, json::write);

        get(apiPrefix + "public/feedversion", FeedVersionController::getAllFeedVersions, json::write);
        get(apiPrefix + "downloadfeed/:token", FeedVersionController::downloadFeedVersionWithToken);

    }

}
