package com.conveyal.datatools.manager.controllers.api;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.utils.FeedUpdater;
import com.conveyal.gtfs.api.ApiMain;
import com.conveyal.gtfs.api.Routes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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
    private static AmazonS3Client s3 = new AmazonS3Client();
    public static ApiMain gtfsApi;
    public static String cacheDirectory;
    public static String bucketFolder;
    public static void register (String apiPrefix) throws IOException {

        // store list of GTFS feed eTags here
        List<String> eTagList = new ArrayList<>();

        gtfsApi.initialize(DataManager.gtfsCache);

        // check for use extension...
        String extensionType = DataManager.getConfigPropertyAsText("modules.gtfsapi.use_extension");

        if ("mtc".equals(extensionType)){
            LOG.info("Using extension " + extensionType + " for service alerts module");
            feedBucket = DataManager.getConfigPropertyAsText("extensions." + extensionType + ".s3_bucket");
            bucketFolder = DataManager.getConfigPropertyAsText("extensions." + extensionType + ".s3_download_prefix");

            eTagList.addAll(registerS3Feeds(feedBucket, cacheDirectory));

            // set feedUpdater to poll for new feeds at specified frequency (in seconds)
            feedUpdater = new FeedUpdater(eTagList, 0, DataManager.getConfigProperty("modules.gtfsapi.update_frequency").asInt());
        }
        // if not using MTC extension
        else {
            LOG.warn("No extension provided for GTFS API");
            if ("true".equals(DataManager.getConfigPropertyAsText("application.data.use_s3_storage"))) {
                feedBucket = DataManager.getConfigPropertyAsText("application.data.gtfs_s3_bucket");
                bucketFolder = FeedStore.s3Prefix;
            }
            else {
                feedBucket = null;
            }
        }

        // check for load on startup
        if ("true".equals(DataManager.getConfigPropertyAsText("modules.gtfsapi.load_on_startup"))) {
            LOG.warn("Loading all feeds into gtfs api (this may take a while)...");
            // use s3
            if ("true".equals(DataManager.getConfigPropertyAsText("application.data.use_s3_storage"))) {
                eTagList.addAll(registerS3Feeds(feedBucket, cacheDirectory));

                // set feedUpdater to poll for new feeds at specified frequency (in seconds)
                feedUpdater = new FeedUpdater(eTagList, 0, DataManager.config.get("modules").get("gtfsapi").get("update_frequency").asInt());
            }
//         else, use local directory
            else {
                // iterate over latest feed versions
                for (FeedSource fs : FeedSource.getAll()) {
                    FeedVersion v = fs.getLatest();
                    if (v != null) {
                        try {
                            gtfsApi.registerFeedSource(fs.id, v.getGtfsFile());
                            eTagList.add(v.hash);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        // set gtfs-api routes with apiPrefix
        Routes.routes(apiPrefix);
    }

//    public static void registerFeedSourceLatest(String id) {
//        FeedSource fs = FeedSource.get(id);
//        FeedVersion v = fs.getLatest();
//        if (v != null) {
//            try {
//                gtfsApi.registerFeedSource(id, v.getGtfsFile());
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//    }

    public static List<String> registerS3Feeds (String bucket, String dir) {
        List<String> eTags = new ArrayList<>();
        // iterate over feeds in download_prefix folder and register to gtfsApi (MTC project)
        ObjectListing gtfsList = s3.listObjects(bucket, dir);
        for (S3ObjectSummary objSummary : gtfsList.getObjectSummaries()) {

            String eTag = objSummary.getETag();
            if (!eTags.contains(eTag)) {
                String keyName = objSummary.getKey();

                // don't add object if it is a dir
                if (keyName.equals(dir)){
                    continue;
                }
                LOG.info("Adding feed " + keyName);
                String feedId = keyName.split("/")[1];
                try {
                    gtfsApi.registerFeedSource(feedId, FeedVersion.get(feedId).getGtfsFile());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                eTags.add(eTag);
            }
        }
        return eTags;
    }
}
