package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.util.Collection;

//import jobs.ProcessSingleFeedJob;
import spark.Request;
import spark.Response;

import static spark.Spark.*;

public class FeedVersionController  {

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

        FeedSource s = FeedSource.get(sourceId);

        return s.getFeedVersions();
    }


    /**
     * Upload a feed version directly. This is done behind Backbone's back, and as such uses
     * x-multipart-formdata rather than a json blob. This is done because uploading files in a JSON
     * blob is not pretty, and we don't really need to get the Backbone object directly; page re-render isn't
     * a problem.
     * @return
     * @throws JsonProcessingException
     */
    /*public static Result create () throws JsonProcessingException {

        Auth0UserProfile userProfile = getSessionProfile();
        if(userProfile == null) return unauthorized();

        MultipartFormData body = request().body().asMultipartFormData();
        Map<String, String[]> params = body.asFormUrlEncoded();

        FeedSource s = FeedSource.get(params.get("feedSourceId")[0]);

        if (!userProfile.canAdministerProject(s.feedCollectionId) && !userProfile.canManageFeed(s.feedCollectionId, s.id))
            return unauthorized();

        if (FeedRetrievalMethod.FETCHED_AUTOMATICALLY.equals(s.retrievalMethod))
            return badRequest("Feed is autofetched! Cannot upload.");

        FeedVersion v = new FeedVersion(s);
        v.setUser(userProfile);

//        File toSave = v.newFeed(uploadStream);
        FilePart uploadPart = body.getFile("feed");
        File upload = uploadPart.getFile();

        Logger.info("Saving feed {} from upload", s);


        FileInputStream uploadStream;
        File toSave;
        try {
            uploadStream = new FileInputStream(upload);
            toSave = v.newFeed(uploadStream);
        } catch (FileNotFoundException e) {
            Logger.error("Unable to open input stream from upload {}", upload);

            return internalServerError("Unable to read uploaded feed");
        }

        v.hash();

        FeedVersion latest = s.getLatest();
        if (latest != null && latest.hash.equals(v.hash)) {
            v.getFeed().delete();
            return redirect("/#feed/" + s.id);
        }

        // for now run sychronously so the user sees something after the redirect
        // it's pretty fast
        new ProcessSingleFeedJob(v).run();

        return redirect("/#feed/" + s.id);
    }*/

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/feedversion/:id", FeedVersionController::getFeedVersion, JsonUtil.objectMapper::writeValueAsString);
        get(apiPrefix + "secure/feedversion", FeedVersionController::getAllFeedVersions, json::write);
        //post(apiPrefix + "secure/project", ProjectController::createProject, JsonUtil.objectMapper::writeValueAsString);
    }

}
