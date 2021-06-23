package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.OtpRouterConfig;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.HttpUtils;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.transit.realtime.GtfsRealtime;
import org.apache.commons.dbutils.DbUtils;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.eclipse.jetty.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_COMMENTS;
import static com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES;

/**
 * This job runs follows a {@link DeployJob} or can be run for any actively running OTP server to check whether the
 * updaters for an OTP instance (defined in routerConfig) are working properly.
 */
public class CheckOtpUpdatersJob extends MonitorableJob {
    private static final ObjectMapper routerConfigMapper = new ObjectMapper()
        .configure(ALLOW_COMMENTS, true)
        .configure(ALLOW_UNQUOTED_FIELD_NAMES, true);
    private static final Logger LOG = LoggerFactory.getLogger(CheckOtpUpdatersJob.class);
    private final Deployment deployment;
    private final OtpServer otpServer;
    private final String routerConfig;
    public ValidationReport validationReport = new ValidationReport();

    public CheckOtpUpdatersJob(Deployment deployment, OtpServer otpServer, String routerConfig) {
        this.deployment = deployment;
        this.otpServer = otpServer;
        this.routerConfig = routerConfig;
    }

    @Override
    public void jobLogic() throws Exception {
        if (otpServer.internalUrl == null || otpServer.internalUrl.size() == 0) {
            status.fail(String.format(
                "Please update the OTP server's (%s) internal URL to point to the OTP API root",
                otpServer.name
            ));
            return;
        }
        Collection<OtpRouterConfig.Updater> updaters = getUpdaters(routerConfig);
        if (updaters.size() == 0) {
            status.completeSuccessfully("No updaters found in router config!");
        }
        for (OtpRouterConfig.Updater updater : updaters) {
            LOG.info("Checking {} updater: {}", updater.type, updater.url);
            UpdaterReport report = checkUpdater(updater);
            validationReport.updaterReports.add(report);
        }
    }

    private UpdaterReport checkUpdater(OtpRouterConfig.Updater updater) {
        switch (updater.type) {
            case "bike-rental": return checkBikeRentalFeed(updater);
            case "real-time-alerts": return checkServiceAlerts(updater);
            case "stop-time-updater": return checkTripUpdates(updater);
            default: return missingCheckForUpdater(updater);
        }
    }

    private UpdaterReport checkServiceAlerts(OtpRouterConfig.Updater updater) {
        UpdaterReport report = new UpdaterReport(updater);
        try {
            GtfsRealtime.FeedMessage feed = getFeedFromUrl(report.updater.url);
            for (GtfsRealtime.FeedEntity entity : feed.getEntityList()) {
                if (entity.hasAlert()) {
                    GtfsRealtime.Alert alert = entity.getAlert();
                    // Skip alerts that are not NO_SERVICE.
                    if(!alert.getEffect().equals(GtfsRealtime.Alert.Effect.NO_SERVICE)) {
                        LOG.info("Skipping alert");
                        continue;
                    }
                    boolean alertIsActive = false;
                    for (GtfsRealtime.TimeRange period : alert.getActivePeriodList()) {
                        long now = System.currentTimeMillis() / 1000L;
                        if (period.getStart() <= now && now <= period.getEnd()) {
                            alertIsActive = true;
                        }
                    }
                    if (!alertIsActive) {
                        LOG.info("Skipping alert (inactive)");
                        continue;
                    }
                    // Iterate over each selector to determine what's affected.
                    for (GtfsRealtime.EntitySelector selector : alert.getInformedEntityList()) {
                        String routeId = selector.getRouteId();
                        if (selector.hasStopId()) {
                            String stopId = selector.getStopId();
                            // Route/stop alert
//                            String id = selector.hasRouteId() ? String.join("-", routeId, stopId) : stopId;
                            report.skippedStops.add(stopId);
                            continue;
                        }
                        if (selector.hasTrip()) {
                            // Trip specific alert
                            GtfsRealtime.TripDescriptor trip = selector.getTrip();
                            report.rtTripsCancelled.add(trip.getTripId());
                            report.cancelledTripsByRoute.merge(trip.getRouteId(), 1, Integer::sum);
                            continue;
                        }
                        if (selector.hasRouteId()) {
                            // Route wide alert
                            report.rtRoutes.add(routeId);
                            continue;
                        }
                    }
                    report.rtStopTimes++;
                }
            }
        } catch (IOException e) {
            report.fileIsUnreachable = true;
            LOG.error("Service alerts feed is unreachable: {}", report.updater.url);
        }
        return report;
    }

    private UpdaterReport checkTripUpdates(OtpRouterConfig.Updater updater) {
        UpdaterReport report = new UpdaterReport(updater);
        // Check how many trips are active right now
        for (String feedVersionId : deployment.feedVersionIds) {
            Set<String> activeTripIds = countActiveTrips(feedVersionId, 1, 60);
            FeedVersion version = Persistence.feedVersions.getById(feedVersionId);
            report.tripsActive60Min = activeTripIds.size();
            // For all active trips, count the number of stoptimes per route/direction/stop
            Feed feed = new Feed(DataManager.GTFS_DATA_SOURCE, version.namespace);
            Set<String> tripIds = new HashSet<>();
            Set<String> serviceIds = new HashSet<>();
            LocalTime now = LocalTime.now(ZoneId.of("America/Chicago"));
            int nowMinusBand = now.minusMinutes(0).toSecondOfDay();
            int nowPlusBand = now.plusMinutes(60).toSecondOfDay();
            for (String tripId : activeTripIds) {
                Trip trip = feed.trips.get(tripId);
                for (StopTime stopTime : feed.stopTimes.getOrdered(trip.trip_id)) {
                    if (stopTime.arrival_time < nowMinusBand || stopTime.arrival_time > nowPlusBand) {
                        continue;
                    }
                    String val = String.join(":", trip.route_id, Integer.toString(trip.direction_id), stopTime.stop_id, Integer.toString(stopTime.stop_sequence));
                    report.stopTimesPerRouteDirectionStop.merge(val, 1, Integer::sum);
                    if (val.equals("40401:1:5640")) {
                        tripIds.add(tripId);
                        serviceIds.add(trip.service_id);
                    }
                }
            }
            LOG.info("trips: {}", tripIds.size());
        }
        // Download trip updates feed and check entities.
        GtfsRealtime.FeedMessage feed = null;
        try {
            feed = getFeedFromUrl(report.updater.url);
        } catch (IOException e) {
            report.fileIsUnreachable = true;
            LOG.error("Trip updates feed is unreachable: {}", report.updater.url);
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        try {
            feed.writeTo(new FileOutputStream(String.format("/Users/landonreed/Downloads/gtfs-rt-updates-%s.pb", dateFormat.format(new Date()))));
        } catch (Exception e) {
            LOG.error("Could not write pb file to disk.");
        }
        LOG.info("{} trips found in TripUpdates", feed.getEntityList().size());
        LocalTime now = LocalTime.now(ZoneId.of("America/Chicago"));
        LOG.info("Now: {}", now);
        for (GtfsRealtime.FeedEntity entity : feed.getEntityList()) {
            if (entity.hasTripUpdate()) {
                GtfsRealtime.TripUpdate tripUpdate = entity.getTripUpdate();
                String tripId = tripUpdate.getTrip().getTripId();
                String routeId = tripUpdate.getTrip().getRouteId();
                GtfsRealtime.TripDescriptor.ScheduleRelationship scheduledRelationship = null;
                if (tripUpdate.hasTrip() && tripUpdate.getTrip().hasScheduleRelationship()) {
                    scheduledRelationship = tripUpdate.getTrip().getScheduleRelationship();
                }
                switch (scheduledRelationship) {
                    case SCHEDULED:
                        LOG.debug("Trip {} is scheduled ({} updates)", tripId, tripUpdate.getStopTimeUpdateCount());
                        break;
                    case CANCELED:
                        LOG.info("Trip {} is cancelled", tripId);
                        report.rtTripsCancelled.add(tripId);
                        report.cancelledTripsByRoute.merge(routeId, 1, Integer::sum);
                        break;
                    case ADDED:
                        LOG.info("Trip {} is added", tripId);
                        break;
                    case UNSCHEDULED:
                        LOG.info("Trip {} is unscheduled", tripId);
                        break;
                    default:
                        LOG.info("Trip {} has unknown schedule relationship");
                        break;
                }
                if (tripUpdate.getStopTimeUpdateCount() > 0) {
                    report.rtTrips.add(tripId);
                    report.rtRoutes.add(routeId);
                    int directionId = tripUpdate.getTrip().getDirectionId();
                    // Download GraphQL trips/stoptimes from OTP and check realtime.
                    ArrayNode stopTimesForTrip = getStopTimesForTrip(updater.feedId, tripId);
                    for (GtfsRealtime.TripUpdate.StopTimeUpdate st : tripUpdate.getStopTimeUpdateList()) {
                        String stopId = st.getStopId();
                        report.rtStops.add(stopId);
                        String val = String.join(":", routeId, Integer.toString(directionId), stopId, Integer.toString(st.getStopSequence()));
                        report.updatesPerRouteDirectionStop.merge(val, 1, Integer::sum);
                        boolean hasMatchingStopTime = false;
                        switch (st.getScheduleRelationship()) {
                            case SKIPPED:
                                // Expect stoptime to be missing
                                report.rtSkippedStopTimes++;
                                report.skippedStops.add(stopId);
                                break;
                            case SCHEDULED:
                                // Expect stoptime to be UPDATED
                                report.rtStopTimes++;
                                break;
                            case NO_DATA:
                                // Expect stoptime to be SCHEDULED
                                break;
                            default:
                                break;
                        }
                    }
                    for (JsonNode stopTime : stopTimesForTrip) {
                        String stopId = stopTime.get("stopId").asText();
                        switch (stopTime.get("realtimeState").asText()) {
                            case "UPDATED":
                            case "CANCELED":
                                report.otpStopTimes++;
                                report.otpRoutes.add(routeId);
                                report.otpTrips.add(tripId);
                                report.otpStops.add(stopId);
                            default:
                                report.otpStopTimesMissingRT.add(String.join("-", tripId, stopId, stopTime.get("stopIndex").asText()));
                        }
                    }
                } else {
                    LOG.info("Skipping check for OTP (no affected stop times)");
                }
            }
        }
        return report;
    }

    private Set<String> countActiveTrips(String feedVersionId, int minusMinutes, int plusMinutes) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        Set<String> tripIdsActiveInTimeBand = new HashSet<>();
        FeedVersion version = Persistence.feedVersions.getById(feedVersionId);
        if (version == null) {
            LOG.warn("Version does not exist! id={}", feedVersionId);
            return tripIdsActiveInTimeBand;
        }
        Connection connection = null;
        try {
            connection = DataManager.GTFS_DATA_SOURCE.getConnection();
            PreparedStatement statement = connection.prepareStatement(
                String.format("select service_id from %s.service_dates where service_date = ?", version.namespace)
            );
            statement.setString(1, formatter.format(new Date()));
            ResultSet resultSet = statement.executeQuery();
            Set<String> serviceIds = new HashSet<>();
            while (resultSet.next()) {
                serviceIds.add(resultSet.getString(1));
            }
            String questionMarks = String.join(", ", Collections.nCopies(serviceIds.size(), "?"));
            statement = connection.prepareStatement(
                String.format("select trip_id from %s.trips where service_id in (%s)", version.namespace, questionMarks)
            );
            int oneBasedIndex = 1;
            for (String id : serviceIds) {
                statement.setString(oneBasedIndex++, id);
            }
            Set<String> tripIdsActiveToday = new HashSet<>();
            ResultSet tripsResult = statement.executeQuery();
            while (tripsResult.next()) {
                tripIdsActiveToday.add(tripsResult.getString(1));
            }
            LocalTime now = LocalTime.now(ZoneId.of("America/Chicago"));
            int nowMinusBand = now.minusMinutes(minusMinutes).toSecondOfDay();
            int nowPlusBand = now.plusMinutes(plusMinutes).toSecondOfDay();
            questionMarks = String.join(", ", Collections.nCopies(tripIdsActiveToday.size(), "?"));
            String stopTimeSql = String.format(
                "select distinct trip_id from %s.stop_times where trip_id in (%s) and arrival_time >= ? and arrival_time <= ?",
                version.namespace,
                questionMarks
            );
            oneBasedIndex = 1;
            statement = connection.prepareStatement(stopTimeSql);
            for (String id : tripIdsActiveToday) {
                statement.setString(oneBasedIndex++, id);
            }
            statement.setInt(oneBasedIndex++, nowMinusBand);
            statement.setInt(oneBasedIndex++, nowPlusBand);
            LOG.info(statement.toString());
            ResultSet stopTimesResult = statement.executeQuery();
            while (stopTimesResult.next()) {
                tripIdsActiveInTimeBand.add(stopTimesResult.getString(1));
            }
            return tripIdsActiveInTimeBand;
        } catch (SQLException throwables) {
            DbUtils.closeQuietly(connection);
        }
        return new HashSet<>();
    }

    private boolean checkMatchingStopTimes(GtfsRealtime.TripUpdate.StopTimeUpdate st, JsonNode stopTime) {
        GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship scheduleRelationship = st.getScheduleRelationship();
        return scheduleRelationship == GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED &&
                "UPDATED".equals(stopTime.get("realtimeState").asText());
    }

    private String getOtpUrl() {
        return otpServer.internalUrl.get(0) + "/otp/routers/default";
    }

    private ArrayNode getStopTimesForTrip(String feedId, String tripId) {
        String url = String.format("%s/index/trips/%s:%s/stoptimes", getOtpUrl(), feedId, tripId);
        String stopTimesString = getStringFromUrl(url);
        try {
            return (ArrayNode) JsonUtil.objectMapper.readTree(stopTimesString);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private UpdaterReport checkBikeRentalFeed(OtpRouterConfig.Updater updater) {
        // Currently there is only support for GBFS bike rental feeds here.
        if (!"gbfs".equals(updater.sourceType.toLowerCase(Locale.ROOT))) {
            return missingCheckForUpdater(updater);
        }
        UpdaterReport report = new UpdaterReport(updater);
        String bikeRentalFile = getStringFromUrl(report.updater.url + "/gbfs.json");
        // TODO: Validate GBFS with https://github.com/MobilityData/gbfs-json-schema
        try {
            JsonNode bikeRentalJson = JsonUtil.objectMapper.readTree(bikeRentalFile);
            // Rigamaroll with checking which GBFS file this is (e.g., entry point?).
            for (JsonNode feed : bikeRentalJson.get("data").get("en").get("feeds")) {
                if ("station_status".equals(feed.get("name").asText())) {
                    String stationStatus = getStringFromUrl(feed.get("url").asText());
                    LOG.info("Downloading station status");
                    JsonNode stationStatusJson = JsonUtil.objectMapper.readTree(stationStatus);
                    ArrayNode stations = (ArrayNode) stationStatusJson.get("data").get("stations");
                    for (JsonNode station : stations) {
                        LOG.debug("station: {} bikes / {} docks", station.get("num_bikes_available"), station.get("num_docks_available"));
                        report.rtStopTimes++;
                    }
                }
            }
        } catch (Exception e) {
            report.fileIsUnparseable = true;
            LOG.error("Bike rental file is unparseable {}", updater.url, e);
        }
        String otpRentalLocations = getStringFromUrl(getOtpUrl() + "/bike_rental");
        try {
            JsonNode otpBikeRentalsJson = JsonUtil.objectMapper.readTree(otpRentalLocations);
            ArrayNode stations = (ArrayNode) otpBikeRentalsJson.get("stations");
            report.otpStopTimes = stations.size();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        return report;
    }

    private UpdaterReport missingCheckForUpdater(OtpRouterConfig.Updater updater) {
        UpdaterReport report = new UpdaterReport(updater);
        report.fatalerror = true;
        report.message = "There is no check logic defined for updater with type = " + report.updater.type;
        return report;
    }

    private GtfsRealtime.FeedMessage getFeedFromUrl(URL url) throws IOException {
        return GtfsRealtime.FeedMessage.parseFrom(url.openStream());
    }

    private GtfsRealtime.FeedMessage getFeedFromUrl(String url) throws IOException {
        return getFeedFromUrl(new URL(url));
    }

    private String getStringFromUrl(String url) {
        HttpResponse response = HttpUtils.httpRequestRawResponse(URI.create(url), 5000, HttpMethod.GET, null);
        try {
            return EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Collection<OtpRouterConfig.Updater> getUpdaters(String routerConfigAsString) throws JsonProcessingException {
        OtpRouterConfig routerConfig = routerConfigMapper.readValue(routerConfigAsString, OtpRouterConfig.class);
        return routerConfig.updaters;
    }

    public class ValidationReport {
        List<UpdaterReport> updaterReports = new ArrayList<>();
    }

    public class UpdaterReport {
        public Map<String, Integer> updatesPerRouteDirectionStop = new HashMap<>();
        public Map<String, Integer> stopTimesPerRouteDirectionStop = new HashMap<>();
        public Set<String> rtStops = new HashSet<>();
        public Set<String> rtTrips = new HashSet<>();
        public Set<String> rtRoutes = new HashSet<>();
        public Set<String> otpStops = new HashSet<>();
        public Set<String> otpTrips = new HashSet<>();
        public Set<String> otpRoutes = new HashSet<>();
        public Map<String, Integer> cancelledTripsByRoute = new HashMap<>();
        public int tripsActive60Min;
        public int tripsActive10Min;
        public Set<String> otpStopTimesMissingRT = new HashSet<>();
        public Set<String> otpStopTimesNoMatch = new HashSet<>();
        public Set<String> rtTripsCancelled = new HashSet<>();
        OtpRouterConfig.Updater updater;
        int specIssues;
        int otpStopTimes;
        Set<String> skippedStops = new HashSet<>();
        int rtStopTimes;
        int rtSkippedStopTimes;
        boolean fileIsUnreachable;
        boolean fileIsUnparseable;
        boolean fatalerror;
        String message;
        private UpdaterReport(OtpRouterConfig.Updater updater) {
            this.updater = updater;
        }

//        @Override
//        public String toString() {
//            return "UpdaterReport{" +
//                    "updater=" + updater +
//                    ", specIssues=" + specIssues +
//                    ", itemsInFile=" + itemsInFile +
//                    ", itemsInOtp=" + itemsInOtp +
//                    ", fileIsUnreachable=" + fileIsUnreachable +
//                    '}';
//        }

        @Override
        public String toString() {
            return "UpdaterReport{" +
                    "stopsWithRealtime=" + String.join(", ", rtStops) +
                    "\ntripsWithRealtime=" + String.join(", ", rtTrips) +
                    "\nroutesWithRealtime=" + String.join(", ", rtRoutes) +
                    "\nupdater=" + updater +
                    ", specIssues=" + specIssues +
                    ", itemsInOtp=" + otpStopTimes +
                    ", itemsInFile=" + rtStopTimes +
                    ", fileIsUnreachable=" + fileIsUnreachable +
                    ", fileIsUnparseable=" + fileIsUnparseable +
                    ", fatalerror=" + fatalerror +
                    ", message='" + message + '\'' +
                    '}';
        }
    }
}
