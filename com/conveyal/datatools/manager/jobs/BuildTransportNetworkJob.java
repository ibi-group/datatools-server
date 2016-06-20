package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.r5.point_to_point.builder.TNBuilderConfig;
import com.conveyal.r5.transit.TransportNetwork;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static com.conveyal.datatools.manager.models.Deployment.getOsmExtract;

/**
 * Created by landon on 4/30/16.
 */
public class BuildTransportNetworkJob extends MonitorableJob {

    public FeedVersion feedVersion;
    public TransportNetwork result;
    public Status status;

    public BuildTransportNetworkJob (FeedVersion feedVersion, String owner) {
        super(owner, "Building Transport Network for " + feedVersion.getFeedSource().name, JobType.BUILD_TRANSPORT_NETWORK);
        this.feedVersion = feedVersion;
        this.result = null;
        this.status = new Status();
    }

    @Override
    public void run() {
        System.out.println("Building network");
        feedVersion.buildTransportNetwork();
        jobFinished();
    }

    @Override
    public Status getStatus() {
        synchronized (status) {
            return status.clone();
        }
    }


}
