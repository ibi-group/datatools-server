package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.utils.FeedUpdater;
import com.conveyal.gtfs.api.ApiMain;
import com.conveyal.gtfs.api.Routes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by landon on 4/12/16.
 */
public class GtfsApiController {
    public static final Logger LOG = LoggerFactory.getLogger(ProjectController.class);
    public static String feedBucket;
    public static FeedUpdater feedUpdater;
    public static String prefix;
    public static void register (String apiPrefix) throws IOException {

        // store list of GTFS feed eTags here
        List<String> eTagList = new ArrayList<>();

        // check for use extension...
        String extensionType = DataManager.config.get("modules").get("gtfsapi").get("use_extension").asText();
        // if use extension, don't worry about setting up controller to store service alerts.
        if (extensionType != "false" && extensionType != null){
            LOG.info("Using extension " + extensionType + " for service alerts module");
            feedBucket = DataManager.config.get("extensions").get(extensionType).get("s3_bucket").asText();
            prefix = DataManager.config.get("extensions").get(extensionType).get("s3_download_prefix").asText();

            // get all feeds in completed folder and save list of eTags from initialize
            eTagList.addAll(ApiMain.initialize(null, false, feedBucket, null, null, prefix));

            // set feedUpdater to poll for new feeds every half hour
            feedUpdater = new FeedUpdater(eTagList, 0, DataManager.config.get("modules").get("gtfsapi").get("update_frequency").asInt());
        }
        // else, set up GTFS Api to use normal data storage
        else {
            LOG.warn("No extension provided");
            // if work_offline, use local directory
            List<String> feeds = new ArrayList<>();
            for (FeedSource fs : FeedSource.getAll()) {
//                feeds.add(fs.getLatestVersionId());
//                feeds.add(fs.id + ".zip");
            }
            String[] feedList = feeds.toArray(new String[0]);
            if (!DataManager.config.get("application").get("data").get("use_s3_storage").asBoolean()) {
                String dir = DataManager.config.get("application").get("data").get("gtfs").asText();
                eTagList.addAll(ApiMain.initialize(dir, feedList));
            }
//         else, use s3
            else {
                feedBucket = DataManager.config.get("application").get("data").get("gtfs_s3_bucket").asText();

                // get all feeds in completed folder and save list of eTags from initialize
                eTagList.addAll(ApiMain.initialize(null, false, feedBucket, null, null, null));

                // set feedUpdater to poll for new feeds every half hour
                feedUpdater = new FeedUpdater(eTagList, 0, DataManager.config.get("modules").get("gtfsapi").get("update_frequency").asInt());
            }
        }

        // set gtfs-api routes with apiPrefix
        Routes.routes(apiPrefix);
    }
}
