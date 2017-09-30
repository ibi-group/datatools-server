package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.controllers.api.GtfsApiController;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.conveyal.datatools.manager.controllers.api.GtfsApiController.registerS3Feeds;

/**
 * Created by landon on 3/24/16.
 */
public class FeedUpdater {
    public Map<String, String> eTags;

    public static final Logger LOG = LoggerFactory.getLogger(FeedUpdater.class);

    public FeedUpdater(Map<String, String> eTagMap, int delay, int seconds) {
        this.eTags = eTagMap;
        LOG.info("Setting feed update to check every {} seconds", seconds);
        DataManager.scheduler.scheduleAtFixedRate(new UpdateFeedsTask(), delay, seconds, TimeUnit.SECONDS);
    }

    public void addFeedETag(String id, String eTag){
        this.eTags.put(id, eTag);
    }

    public void addFeedETags(Map<String, String> eTagList){
        this.eTags.putAll(eTagList);
    }

    class UpdateFeedsTask implements Runnable {
        public void run() {
            Map<String, String> updatedTags;
            boolean feedsUpdated = false;
            try {
                updatedTags = registerS3Feeds(eTags, GtfsApiController.feedBucket, GtfsApiController.bucketFolder);
                feedsUpdated = !updatedTags.isEmpty();
                addFeedETags(updatedTags);
            } catch (Exception any) {
                LOG.warn("Error updating feeds {}", any);
            }
            if (feedsUpdated) {
                LOG.info("New eTag list " + eTags);
            }
            // TODO: compare current list of eTags against list in completed folder

            // TODO: load feeds for any feeds with new eTags
//            ApiMain.loadFeedFromBucket()
        }
    }

}
