package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.datastore.GlobalTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.transit.EditorFeed;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.HaltException;
import spark.Request;
import spark.Response;

import java.io.IOException;

import static spark.Spark.*;
import static spark.Spark.delete;
import static spark.Spark.put;

/**
 * Created by landon on 6/14/16.
 */
public class FeedInfoController {
    public static final JsonManager<EditorFeed> json =
            new JsonManager<>(EditorFeed.class, JsonViews.UserInterface.class);
    private static final Logger LOG = LoggerFactory.getLogger(FeedInfoController.class);

    public static EditorFeed getFeedInfo(Request req, Response res) {
        String id = req.params("id");

        if (id == null) {
            return null;
            // TODO: return all feedInfos for project?
        }
        GlobalTx gtx = null;
        try {
            gtx = VersionedDataStore.getGlobalTx();
            if (!gtx.feeds.containsKey(id)) {
                // create new EditorFeed if id exists in manager
                if (FeedSource.get(id) != null) {
                    EditorFeed fs = new EditorFeed(id);
                    gtx.feeds.put(fs.id, fs);
                    gtx.commit();
                    return fs;
                }
                else {
                    halt(404, "Feed id does not exist");
                    return null;
                }
            }
            EditorFeed fs = gtx.feeds.get(id);
            return fs;
        } finally {
            gtx.rollbackIfOpen();
        }
    }

    public static Object createFeedInfo(Request req, Response res) {
        EditorFeed fs;
        GlobalTx gtx = VersionedDataStore.getGlobalTx();

        try {
            fs = Base.mapper.readValue(req.body(), EditorFeed.class);

            if (gtx.feeds.containsKey(fs.id)) {
                halt(404, "Feed id already exists in editor database");
            }

            gtx.feeds.put(fs.id, fs);
            gtx.commit();

            return Base.toJson(fs, false);
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            halt(404);
        } finally {
            if (gtx != null) gtx.rollbackIfOpen();
        }
        return null;
    }


    public static EditorFeed updateFeedInfo(Request req, Response res) throws IOException {
        String id = req.params("id");

        EditorFeed feed;
        GlobalTx gtx = null;
        try {
            feed = Base.mapper.readValue(req.body(), EditorFeed.class);
            gtx = VersionedDataStore.getGlobalTx();

            if(!gtx.feeds.containsKey(feed.id)) {
                halt(400);
            }

            gtx.feeds.put(feed.id, feed);
            gtx.commit();

            return feed;
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
        } finally {
            if (gtx != null) gtx.rollbackIfOpen();
        }
        return null;
    }

    /**
     * Delete NOT ONLY feed info, but also entirely delete the record of the feed in the editor database.
     * @param req
     * @param res
     * @return
     */
    public static Object deleteFeedInfoAndEntireFeedEntryInEditor(Request req, Response res) {
        String id = req.params("id");
        if (!VersionedDataStore.feedExists(id)) {
            halt(400, SparkUtils.formatJSON("Feed ID does not exist"));
        }
        try {
            VersionedDataStore.wipeFeedDB(id);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            halt(400, SparkUtils.formatJSON("Error deleting feed", 400, e));
        }
        return false;
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/feedinfo/:id", FeedInfoController::getFeedInfo, json::write);
        options(apiPrefix + "secure/feedinfo", (q, s) -> "");
        get(apiPrefix + "secure/feedinfo", FeedInfoController::getFeedInfo, json::write);
        post(apiPrefix + "secure/feedinfo/:id", FeedInfoController::createFeedInfo, json::write);
        put(apiPrefix + "secure/feedinfo/:id", FeedInfoController::updateFeedInfo, json::write);
        delete(apiPrefix + "secure/feedinfo/:id", FeedInfoController::deleteFeedInfoAndEntireFeedEntryInEditor, json::write);
    }
}
