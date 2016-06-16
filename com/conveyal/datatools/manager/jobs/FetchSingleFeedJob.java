package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.controllers.api.GtfsApiController;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.gtfs.api.ApiMain;

public class FetchSingleFeedJob implements MonitorableJob {

    private FeedSource feedSource;
    public FeedVersion result;
    private Status status;

    public FetchSingleFeedJob (FeedSource feedSource, String owner) {
        this.feedSource = feedSource;
        this.result = null;
        this.status = new Status(owner);
    }

    @Override
    public void run() {
        // TODO: fetch automatically vs. manually vs. in-house
        result = feedSource.fetch();

        new ProcessSingleFeedJob(result, status.owner).run();
        if (DataManager.config.get("modules").get("validator").get("enabled").asBoolean()) {
            BuildTransportNetworkJob btnj = new BuildTransportNetworkJob(result);
            Thread tnThread = new Thread(btnj);
            tnThread.start();
        }
    }

    @Override
    public Status getStatus() {
        synchronized (status) {
            return status.clone();
        }
    }
}
