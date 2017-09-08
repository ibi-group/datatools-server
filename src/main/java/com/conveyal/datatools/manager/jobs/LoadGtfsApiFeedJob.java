package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.gtfs.api.ApiMain;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by landon on 4/30/16.
 */
public class LoadGtfsApiFeedJob implements Runnable {
    public static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LoadGtfsApiFeedJob.class);

    public static FeedSource feedSource;

    public LoadGtfsApiFeedJob(FeedSource feedSource) {
        this.feedSource = feedSource;
    }

    @Override
    public void run() {
        File latest = feedSource.retrieveLatest() != null ? feedSource.retrieveLatest().retrieveGtfsFile() : null;
        if (latest != null)
            try {
                LOG.info("Loading feed into GTFS api: " + feedSource.id);
                ApiMain.registerFeedSource(feedSource.id, latest);
            } catch (Exception e) {
                e.printStackTrace();
            }
    }
}
