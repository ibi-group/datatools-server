package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.r5.transit.TransportNetwork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

/**
 * Created by landon on 10/11/16.
 */
public class ReadTransportNetworkJob extends MonitorableJob {
    private static final Logger LOG = LoggerFactory.getLogger(ReadTransportNetworkJob.class);
    public FeedVersion feedVersion;
    public TransportNetwork result;

    public ReadTransportNetworkJob (FeedVersion feedVersion, String owner) {
        super(owner, "Reading in Transport Network for " + feedVersion.parentFeedSource().name, JobType.BUILD_TRANSPORT_NETWORK);
        this.feedVersion = feedVersion;
        this.result = null;
        this.status = new Status();
        status.message = "Waiting to begin job...";
    }

    @Override
    public void jobLogic () {
        LOG.info("Reading network");
        File is;
        is = feedVersion.transportNetworkPath();
        try {
            feedVersion.transportNetwork = TransportNetwork.read(is);
            // check to see if distance tables are built yet... should be removed once better caching strategy is implemeneted.
            if (feedVersion.transportNetwork.transitLayer.stopToVertexDistanceTables == null) {
                feedVersion.transportNetwork.transitLayer.buildDistanceTables(null);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        synchronized (status) {
            status.message = "Transport network read successfully!";
            status.percentComplete = 100;
            status.completed = true;
        }
    }

}
