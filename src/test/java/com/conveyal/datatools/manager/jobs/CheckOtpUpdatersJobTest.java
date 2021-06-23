package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;

public class CheckOtpUpdatersJobTest extends DatatoolsTest {
    private static final Logger LOG = LoggerFactory.getLogger(CheckOtpUpdatersJobTest.class);
    public static final URL url = Resources.getResource("otp/router-config.json");
    public static final String ROUTER_CONFIG;

    static {
        try {
            ROUTER_CONFIG = Resources.toString(url, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeAll
    public static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();
    }
    /**
     * Can download GTFS-rt updater from URL.
     */
    @Test
    public void canDownloadGtfsRtFromUrl () throws IOException {
        Project project = new Project();
        project.name = "updater-test";
        FeedSource feedSource = new FeedSource("Houston");
        FeedVersion feedVersion = new FeedVersion(feedSource);
        // FIXME: Pre-loaded/validated GTFS file for Houston to match realtime data.
        feedVersion.namespace = "rffg_dyjtcxcrjdpznfyxyxijoi";
        Persistence.feedVersions.create(feedVersion);
        OtpServer otpServer = new OtpServer();
        otpServer.internalUrl = Collections.singletonList("https://houston-otp-server.ibi-transit.com");
        Deployment deployment = new Deployment(project);
        deployment.feedVersionIds.add(feedVersion.id);
        // FIXME: commented out because of git staging conflict. uncomment before running again!
        CheckOtpUpdatersJob checkOtpUpdatersJob = new CheckOtpUpdatersJob(deployment, otpServer, ROUTER_CONFIG);
        checkOtpUpdatersJob.run();
        CheckOtpUpdatersJob.UpdaterReport alertsReport = checkOtpUpdatersJob.validationReport.updaterReports.get(1);
        CheckOtpUpdatersJob.UpdaterReport updatesReport = checkOtpUpdatersJob.validationReport.updaterReports.get(2);
        Sets.SetView<String> cancelledStops = Sets.intersection(alertsReport.skippedStops, updatesReport.skippedStops);
        LOG.info("Alert cancelled stops: {}", String.join(", ", alertsReport.skippedStops));
        LOG.info("Cancelled stops in trip updates: {}", String.join(", ", cancelledStops));
        for (CheckOtpUpdatersJob.UpdaterReport report : checkOtpUpdatersJob.validationReport.updaterReports) {
            LOG.info(report.toString());
            LOG.info("{}/{}", report.otpStopTimes, report.rtStopTimes);
            Sets.SetView<String> tripsMissingInOtp = Sets.symmetricDifference(report.rtTrips, report.otpTrips);
            LOG.info("Trips missing in OTP: {}", String.join(", ", tripsMissingInOtp));
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        // Write output
        new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS).writeValue(new File("/Users/landonreed/" + dateFormat.format(new Date()) + ".json"), checkOtpUpdatersJob.validationReport);
    }
}
