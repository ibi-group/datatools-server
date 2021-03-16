package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.OtpRouterConfig;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.utils.HttpUtils;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.transit.realtime.GtfsRealtime;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.eclipse.jetty.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;

/**
 * This job runs follows a {@link DeployJob} or can be run for any actively running OTP server to check whether the
 * updaters for an OTP instance (defined in routerConfig) are working properly.
 */
public class CheckOtpUpdatersJob extends MonitorableJob {
    private static final Logger LOG = LoggerFactory.getLogger(CheckOtpUpdatersJob.class);
    private final Deployment deployment;
    private final OtpServer otpServer;
    private final String routerConfig;

    public CheckOtpUpdatersJob(Deployment deployment, OtpServer otpServer, String routerConfig) {
        this.deployment = deployment;
        this.otpServer = otpServer;
        this.routerConfig = routerConfig;
    }

    @Override
    public void jobLogic() throws Exception {
        Collection<OtpRouterConfig.Updater> updaters = getUpdaters(routerConfig);
        if (updaters.size() == 0) {
            status.completeSuccessfully("No updaters found in router config!");
        }
        for (OtpRouterConfig.Updater updater : updaters) {
            switch (updater.type) {
                case "bike-rental":
                    checkBikeRentalFeeds(updater);
                    break;
                case "real-time-alerts":
                    checkServiceAlerts(updater);
                    break;
                case "stop-time-updater":
                    checkTripUpdates(updater);
                    break;
                default:
                    missingCheckForUpdater(updater);
                    break;
            }
        }
    }

    private void checkServiceAlerts(OtpRouterConfig.Updater updater) {

    }

    private void checkTripUpdates(OtpRouterConfig.Updater updater) throws IOException {
        // Download trip updates feed
        URL url = new URL(updater.url);
        GtfsRealtime.FeedMessage feed = GtfsRealtime.FeedMessage.parseFrom(url.openStream());
        for (GtfsRealtime.FeedEntity entity : feed.getEntityList()) {
            if (entity.hasTripUpdate()) {
                System.out.println(entity.getTripUpdate());
            }
        }
    }

    private String getUpdaterFromUrl(OtpRouterConfig.Updater updater) {
        HttpResponse response = HttpUtils.httpRequestRawResponse(URI.create(updater.url), 5000, HttpMethod.GET, null);
        String result = null;
        if (response.getEntity() != null) {
            try {
                result = EntityUtils.toString(response.getEntity());
            } catch (IOException e) {
                LOG.error("An exception occurred while parsing response from Updater {}", updater.url);
                e.printStackTrace();
            }
        } else {
            LOG.warn("No response body available to parse from Updater request");
        }
        return result;
    }

    private void missingCheckForUpdater(OtpRouterConfig.Updater updater) {

    }

    private void checkBikeRentalFeeds(OtpRouterConfig.Updater updater) {

    }

    private Collection<OtpRouterConfig.Updater> getUpdaters(String routerConfigAsString) throws JsonProcessingException {
        OtpRouterConfig routerConfig = JsonUtil.objectMapper.readValue(routerConfigAsString, OtpRouterConfig.class);
        return routerConfig.updaters;
    }
}
