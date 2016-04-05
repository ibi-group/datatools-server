package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedVersion;


/**
 * Process/validate a single GTFS feed
 * @author mattwigway
 *
 */
public class ProcessSingleFeedJob implements Runnable {
    FeedVersion feedVersion;

    /**
     * Create a job for the given feed version.
     * @param feedVersion
     */
    public ProcessSingleFeedJob (FeedVersion feedVersion) {
        this.feedVersion = feedVersion;
    }

    public void run() {
        feedVersion.validate();
        feedVersion.save();

        for(String resourceType : DataManager.feedResources.keySet()) {
            DataManager.feedResources.get(resourceType).feedVersionUpdated(feedVersion, null);
        }
    }

}
