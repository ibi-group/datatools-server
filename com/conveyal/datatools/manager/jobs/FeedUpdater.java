package com.conveyal.datatools.manager.jobs;

import com.amazonaws.services.s3.AmazonS3Client;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.controllers.api.GtfsApiController;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by landon on 3/24/16.
 */
public class FeedUpdater {
    public List<String> eTags;
    private static Timer timer;
    private static AmazonS3Client s3;

    public static final Logger LOG = LoggerFactory.getLogger(FeedUpdater.class);

    public FeedUpdater(List<String> eTagList, int delay, int seconds){
        this.eTags = eTagList;
//        this.timer = new Timer();

        // TODO: check for credentials??
//        this.s3 = new AmazonS3Client();

        DataManager.scheduler.scheduleAtFixedRate(fetchProjectFeedsJob, delay, seconds, TimeUnit.SECONDS);
        this.timer.schedule(new UpdateFeedsTask(), delay*1000, seconds*1000);


    }

    public void addFeedETag(String eTag){
        this.eTags.add(eTag);
    }

    public void addFeedETags(List<String> eTagList){
        this.eTags.addAll(eTagList);
    }

    public void cancel(){
        this.timer.cancel(); //Terminate the timer thread
    }


    class UpdateFeedsTask extends TimerTask {
        public void run() {
            LOG.info("Fetching feeds...");
            LOG.info("Current eTag list " + eTags.toString());
            List<String> updatedTags = GtfsApiController.registerS3Feeds(eTags, GtfsApiController.feedBucket, GtfsApiController.bucketFolder);
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
