package com.conveyal.datatools.manager.jobs;

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
public class BuildTransportNetworkJob implements Runnable {

    public FeedVersion feedVersion;
    public TransportNetwork result;

    public BuildTransportNetworkJob (FeedVersion feedVersion) {
        this.feedVersion = feedVersion;
        this.result = null;
    }

    @Override
    public void run() {
        System.out.println("Building network");
        result = feedVersion.buildTransportNetwork();
    }
}
