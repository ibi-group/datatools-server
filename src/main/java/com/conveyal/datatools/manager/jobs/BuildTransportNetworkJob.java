package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.r5.transit.TransportNetwork;

import java.util.Map;

/**
 * Created by landon on 4/30/16.
 */
public class BuildTransportNetworkJob extends MonitorableJob {

    public FeedVersion feedVersion;
    private TransportNetwork result;
    public Status status;

    public BuildTransportNetworkJob (FeedVersion feedVersion, String owner) {
        super(owner, "Building Transport Network for " + feedVersion.parentFeedSource().name, JobType.BUILD_TRANSPORT_NETWORK);
        this.feedVersion = feedVersion;
        this.result = null;
        this.status = new Status();
        status.message = "Waiting to begin job...";
    }

    @Override
    public void jobLogic() {
        System.out.println("Building network");
        try {
            if (feedVersion.validationResult != null) {
                feedVersion.buildTransportNetwork(status);
            }
            else {
                synchronized (status) {
                    status.message = "Transport network skipped because of bad validation.";
                    status.percentComplete = 100;
                    status.error = true;
                    status.completed = true;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            synchronized (status) {
                status.message = "Transport network failed!";
                status.percentComplete = 100;
                status.error = true;
                status.completed = true;
            }
        }
        if (!status.error) {
            synchronized (status) {
                status.message = "Transport network built successfully!";
                status.percentComplete = 100;
                status.completed = true;
            }
        }
    }

}
