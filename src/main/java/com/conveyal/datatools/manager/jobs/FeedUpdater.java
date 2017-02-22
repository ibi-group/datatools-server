package com.conveyal.datatools.manager.jobs;

import com.amazonaws.services.s3.AmazonS3Client;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.controllers.api.GtfsApiController;

import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by landon on 3/24/16.
 */
public class FeedUpdater {
    public Map<String, String> eTags;
    private static Timer timer;
    private static AmazonS3Client s3;

    public static final Logger LOG = LoggerFactory.getLogger(FeedUpdater.class);

    public FeedUpdater(Map<String, String> eTagMap, int delay, int seconds) {
        this.eTags = eTagMap;
        DataManager.scheduler.scheduleAtFixedRate(new UpdateFeedsTask(), delay, seconds, TimeUnit.SECONDS);
    }

    public void addFeedETag(String id, String eTag){
        this.eTags.put(id, eTag);
    }

    public void addFeedETags(Map<String, String> eTagList){
        this.eTags.putAll(eTagList);
    }

    public void cancel(){
        this.timer.cancel(); //Terminate the timer thread
    }


    class UpdateFeedsTask implements Runnable {
        public void run() {
            LOG.info("Fetching feeds...");
            LOG.info("Current eTag list " + eTags.toString());
            Map<String, String> updatedTags = GtfsApiController.registerS3Feeds(eTags, GtfsApiController.feedBucket, GtfsApiController.bucketFolder);
            Boolean feedsUpdated = updatedTags.isEmpty() ? false : true;
            addFeedETags(updatedTags);
            if (!feedsUpdated) {
                LOG.info("No feeds updated...");
            }
            else {
                LOG.info("New eTag list " + eTags);
            }
            // TODO: compare current list of eTags against list in completed folder

            // TODO: load feeds for any feeds with new eTags
//            ApiMain.loadFeedFromBucket()
        }
    }

}
