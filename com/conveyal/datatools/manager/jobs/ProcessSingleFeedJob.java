package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.r5.analyst.IsochroneFeature;
import com.conveyal.r5.analyst.ResultSet;
import com.conveyal.r5.analyst.cluster.AnalystClusterRequest;
import com.conveyal.r5.analyst.cluster.ResultEnvelope;
import com.conveyal.r5.analyst.cluster.TaskStatistics;
import com.conveyal.r5.point_to_point.builder.TNBuilderConfig;
import com.conveyal.r5.profile.RepeatedRaptorProfileRouter;
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

        String gtfsDir = DataManager.config.get("application").get("data").get("gtfs").asText() + "/";

        // Fetch OSM extract
        File osmExtract = new File(gtfsDir + feedVersion.feedSourceId + ".osm.pbf");
        InputStream is = getOsmExtract(feedVersion.validationResult.bounds);
        OutputStream out = null;
        try {
            out = new FileOutputStream(osmExtract);
            IOUtils.copy(is, out);
            is.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Create/save r5 network
        TransportNetwork tn = TransportNetwork.fromFiles(osmExtract.getAbsolutePath(), gtfsDir + feedVersion.id, TNBuilderConfig.defaultConfig());
        feedVersion.transportNetwork = tn;
        File tnFile = new File(gtfsDir + feedVersion.id + "_network.dat");
        OutputStream tnOut = null;
        try {
            tnOut = new FileOutputStream(tnFile);
            tn.write(tnOut);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        for(String resourceType : DataManager.feedResources.keySet()) {
            DataManager.feedResources.get(resourceType).feedVersionCreated(feedVersion, null);
        }
    }

}
