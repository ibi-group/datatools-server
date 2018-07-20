package com.conveyal.datatools.editor.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.editor.datastore.FeedTx;
import com.conveyal.datatools.editor.models.Snapshot;
import com.conveyal.datatools.editor.models.transit.Agency;
import com.conveyal.datatools.editor.models.transit.EditorFeed;
import com.conveyal.datatools.editor.models.transit.GtfsRouteType;
import com.conveyal.datatools.editor.models.transit.Route;
import com.conveyal.datatools.editor.models.transit.RouteType;
import com.conveyal.datatools.editor.models.transit.ServiceCalendar;
import com.conveyal.datatools.editor.models.transit.Stop;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.gtfs.loader.Feed;
import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.conveyal.datatools.editor.datastore.GlobalTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.awt.geom.Rectangle2D;

import org.mapdb.Fun.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ProcessGtfsSnapshotMerge extends MonitorableJob {
    public static final Logger LOG = LoggerFactory.getLogger(ProcessGtfsSnapshotMerge.class);
    /** map from GTFS agency IDs to Agencies */
    private Map<String, Agency> agencyIdMap = new HashMap<>();
    private Map<String, Route> routeIdMap = new HashMap<>();
    /** map from (gtfs stop ID, database agency ID) -> stop */
    private Map<Tuple2<String, String>, Stop> stopIdMap = Maps.newHashMap();
    private TIntObjectMap<String> routeTypeIdMap = new TIntObjectHashMap<>();

    private Feed inputFeedTables;
    private EditorFeed editorFeed;

    public FeedVersion feedVersion;

    /*public ProcessGtfsSnapshotMerge (File gtfsFile) {
        this(gtfsFile, null);
    }*/

    public ProcessGtfsSnapshotMerge (FeedVersion feedVersion, String owner) {
        super(owner, "Creating snapshot for " + feedVersion.parentFeedSource().name, JobType.PROCESS_SNAPSHOT_MERGE);
        this.feedVersion = feedVersion;
        status.update(false, "Waiting to begin job...", 0);
        LOG.info("GTFS Snapshot Merge for feedVersion {}", feedVersion.id);
    }

    public void jobLogic () {
        long agencyCount = 0;
        long routeCount = 0;
        long stopCount = 0;
        long stopTimeCount = 0;
        long tripCount = 0;
        long shapePointCount = 0;
        long serviceCalendarCount = 0;
        long fareCount = 0;

        GlobalTx gtx = VersionedDataStore.getGlobalTx();

        // create a new feed based on this version
        FeedTx feedTx = VersionedDataStore.getFeedTx(feedVersion.feedSourceId);

        editorFeed = new EditorFeed();
        editorFeed.setId(feedVersion.feedSourceId);
        Rectangle2D bounds = feedVersion.validationResult.fullBounds.toRectangle2D();
        if (bounds != null) {
            editorFeed.defaultLat = bounds.getCenterY();
            editorFeed.defaultLon = bounds.getCenterX();
        }


        try {
            synchronized (status) {
                status.message = "Wiping old data...";
                status.percentComplete = 2;
            }
            // clear the existing data
            for(String key : feedTx.agencies.keySet()) feedTx.agencies.remove(key);
            for(String key : feedTx.routes.keySet()) feedTx.routes.remove(key);
            for(String key : feedTx.stops.keySet()) feedTx.stops.remove(key);
            for(String key : feedTx.calendars.keySet()) feedTx.calendars.remove(key);
            for(String key : feedTx.exceptions.keySet()) feedTx.exceptions.remove(key);
            for(String key : feedTx.fares.keySet()) feedTx.fares.remove(key);
            for(String key : feedTx.tripPatterns.keySet()) feedTx.tripPatterns.remove(key);
            for(String key : feedTx.trips.keySet()) feedTx.trips.remove(key);
            LOG.info("Cleared old data");

            synchronized (status) {
                status.message = "Loading GTFS file...";
                status.percentComplete = 5;
            }

            // retrieveById Feed connection to SQL tables for the feed version
            inputFeedTables = feedVersion.retrieveFeed();
            if(inputFeedTables == null) return;

            LOG.info("GtfsImporter: importing feed...");
            synchronized (status) {
                status.message = "Beginning feed import...";
                status.percentComplete = 8;
            }
            // load feed_info.txt
            // FIXME add back in feed info!!
//            if(inputFeedTables.feedInfo.size() > 0) {
//                FeedInfo feedInfo = input.feedInfo.values().iterator().next();
//                editorFeed.feedPublisherName = feedInfo.feed_publisher_name;
//                editorFeed.feedPublisherUrl = feedInfo.feed_publisher_url;
//                editorFeed.feedLang = feedInfo.feed_lang;
//                editorFeed.feedEndDate = feedInfo.feed_end_date;
//                editorFeed.feedStartDate = feedInfo.feed_start_date;
//                editorFeed.feedVersion = feedInfo.feed_version;
//            }
            gtx.feeds.put(feedVersion.feedSourceId, editorFeed);

            // load the GTFS agencies
            Iterator<com.conveyal.gtfs.model.Agency> agencyIterator = inputFeedTables.agencies.iterator();
            while (agencyIterator.hasNext()) {
                com.conveyal.gtfs.model.Agency gtfsAgency = agencyIterator.next();
                Agency agency = new Agency(gtfsAgency, editorFeed);

                // don't save the agency until we've come up with the stop centroid, below.
                agencyCount++;

                // we do want to use the modified agency ID here, because everything that refers to it has a reference
                // to the agency object we updated.
                feedTx.agencies.put(agency.id, agency);
                agencyIdMap.put(gtfsAgency.agency_id, agency);
            }
            synchronized (status) {
                status.message = "Agencies loaded: " + agencyCount;
                status.percentComplete = 10;
            }
            LOG.info("Agencies loaded: " + agencyCount);

            LOG.info("GtfsImporter: importing stops...");
            synchronized (status) {
                status.message = "Importing stops...";
                status.percentComplete = 15;
            }
            // TODO: remove stop ownership inference entirely?
            // infer agency ownership of stops, if there are multiple feeds
//            SortedSet<Tuple2<String, String>> stopsByAgency = inferAgencyStopOwnership();

            // build agency centroids as we go
            // note that these are not actually centroids, but the center of the extent of the stops . . .
            Map<String, Envelope> stopEnvelopes = Maps.newHashMap();

            for (Agency agency : agencyIdMap.values()) {
                stopEnvelopes.put(agency.id, new Envelope());
            }

            GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);
            for (com.conveyal.gtfs.model.Stop gtfsStop : inputFeedTables.stops) {
                Stop stop = new Stop(gtfsStop, geometryFactory, editorFeed);
                feedTx.stops.put(stop.id, stop);
                stopIdMap.put(new Tuple2(gtfsStop.stop_id, editorFeed.id), stop);
                stopCount++;
            }

            LOG.info("Stops loaded: " + stopCount);
            synchronized (status) {
                status.message = "Stops loaded: " + stopCount;
                status.percentComplete = 25;
            }
            LOG.info("GtfsImporter: importing routes...");
            synchronized (status) {
                status.message = "Importing routes...";
                status.percentComplete = 30;
            }
            // import routes
            for (com.conveyal.gtfs.model.Route gtfsRoute : inputFeedTables.routes) {
                Agency agency = agencyIdMap.get(gtfsRoute.agency_id);

                if (!routeTypeIdMap.containsKey(gtfsRoute.route_type)) {
                    RouteType rt = new RouteType();
                    rt.gtfsRouteType = GtfsRouteType.fromGtfs(gtfsRoute.route_type);
                    gtx.routeTypes.put(rt.id, rt);
                    routeTypeIdMap.put(gtfsRoute.route_type, rt.id);
                }

                Route route = new Route(gtfsRoute, editorFeed, agency);

                feedTx.routes.put(route.id, route);
                routeIdMap.put(gtfsRoute.route_id, route);
                routeCount++;
            }

            LOG.info("Routes loaded: " + routeCount);
            synchronized (status) {
                status.message = "Routes loaded: " + routeCount;
                status.percentComplete = 35;
            }

            LOG.info("GtfsImporter: importing Service Calendars...");
            synchronized (status) {
                status.message = "Importing service calendars...";
                status.percentComplete = 38;
            }
            // we don't put service calendars in the database just yet, because we don't know what agency they're associated with
            // we copy them into the agency database as needed
            // GTFS service ID -> ServiceCalendar
            Map<String, ServiceCalendar> calendars = Maps.newHashMap();

            // FIXME: add back in services!
//            for (Service svc : input.services.values()) {
//
//                ServiceCalendar cal;
//
//                if (svc.calendar != null) {
//                    // easy case: don't have to infer anything!
//                    cal = new ServiceCalendar(svc.calendar, feed);
//                } else {
//                    // infer a calendar
//                    // number of mondays, etc. that this calendar is active
//                    int monday, tuesday, wednesday, thursday, friday, saturday, sunday;
//                    monday = tuesday = wednesday = thursday = friday = saturday = sunday = 0;
//                    LocalDate startDate = null;
//                    LocalDate endDate = null;
//
//                    for (CalendarDate cd : svc.calendar_dates.values()) {
//                        if (cd.exception_type == 2)
//                            continue;
//
//                        if (startDate == null || cd.date.isBefore(startDate))
//                            startDate = cd.date;
//
//                        if (endDate == null || cd.date.isAfter(endDate))
//                            endDate = cd.date;
//
//                        int dayOfWeek = cd.date.getDayOfWeek().getValue();
//
//                        switch (dayOfWeek) {
//                        case DateTimeConstants.MONDAY:
//                            monday++;
//                            break;
//                        case DateTimeConstants.TUESDAY:
//                            tuesday++;
//                            break;
//                        case DateTimeConstants.WEDNESDAY:
//                            wednesday++;
//                            break;
//                        case DateTimeConstants.THURSDAY:
//                            thursday++;
//                            break;
//                        case DateTimeConstants.FRIDAY:
//                            friday++;
//                            break;
//                        case DateTimeConstants.SATURDAY:
//                            saturday++;
//                            break;
//                        case DateTimeConstants.SUNDAY:
//                            sunday++;
//                            break;
//                        }
//                    }
//
//                    // infer the calendar. if there is service on more than half as many as the maximum number of
//                    // a particular day that has service, assume that day has service in general.
//                    int maxService = Ints.max(monday, tuesday, wednesday, thursday, friday, saturday, sunday);
//
//                    cal = new ServiceCalendar();
//                    cal.feedId = feed.id;
//
//                    if (startDate == null) {
//                        // no service whatsoever
//                        LOG.warn("Service ID " + svc.service_id + " has no service whatsoever");
//                        startDate = LocalDate.now().minusMonths(1);
//                        endDate = startDate.plusYears(1);
//                        cal.monday = cal.tuesday = cal.wednesday = cal.thursday = cal.friday = cal.saturday = cal.sunday = false;
//                    }
//                    else {
//                        // infer parameters
//
//                        int threshold = (int) Math.round(Math.ceil((double) maxService / 2));
//
//                        cal.monday = monday >= threshold;
//                        cal.tuesday = tuesday >= threshold;
//                        cal.wednesday = wednesday >= threshold;
//                        cal.thursday = thursday >= threshold;
//                        cal.friday = friday >= threshold;
//                        cal.saturday = saturday >= threshold;
//                        cal.sunday = sunday >= threshold;
//
//                        cal.startDate = startDate;
//                        cal.endDate = endDate;
//                    }
//
//                    cal.inferName();
//                    cal.gtfsServiceId = svc.service_id;
//                }
//
//                calendars.put(svc.service_id, cal);
//
//                serviceCalendarCount++;
//            }

            LOG.info("Service calendars loaded: " + serviceCalendarCount);
            synchronized (status) {
                status.message = "Service calendars loaded: " + serviceCalendarCount;
                status.percentComplete = 45;
            }
            LOG.info("GtfsImporter: importing trips...");
            synchronized (status) {
                status.message = "Importing trips...";
                status.percentComplete = 50;
            }
            // FIXME need to load patterns and trips
            // import trips, stop times and patterns all at once
//            Map<String, Pattern> patterns = input.patterns;
//            Set<String> processedTrips = new HashSet<>();
//            for (Entry<String, Pattern> pattern : patterns.entrySet()) {
//                // it is possible, though unlikely, for two routes to have the same stopping pattern
//                // we want to ensure they retrieveById different trip patterns
//                Map<String, TripPattern> tripPatternsByRoute = Maps.newHashMap();
//                for (String tripId : pattern.getValue().associatedTrips) {
//
//                    // TODO: figure out why trips are being added twice. This check prevents that.
//                    if (processedTrips.contains(tripId)) {
//                        continue;
//                    }
//                    synchronized (status) {
//                        status.message = "Importing trips... (id: " + tripId + ") " + tripCount + "/" + input.trips.size();
//                        status.percentComplete = 50 + 45 * tripCount / input.trips.size();
//                    }
//                    com.conveyal.gtfs.model.Trip gtfsTrip = input.trips.retrieveById(tripId);
//
//                    if (!tripPatternsByRoute.containsKey(gtfsTrip.route_id)) {
//                        TripPattern pat = createTripPatternFromTrip(gtfsTrip, feedTx);
//                        feedTx.tripPatterns.put(pat.id, pat);
//                        tripPatternsByRoute.put(gtfsTrip.route_id, pat);
//                    }
//
//                    // there is more than one pattern per route, but this map is specific to only this pattern
//                    // generally it will contain exactly one entry, unless there are two routes with identical
//                    // stopping patterns.
//                    // (in DC, suppose there were trips on both the E2/weekday and E3/weekend from Friendship Heights
//                    //  that short-turned at Missouri and 3rd).
//                    TripPattern pat = tripPatternsByRoute.retrieveById(gtfsTrip.route_id);
//
//                    ServiceCalendar cal = calendars.retrieveById(gtfsTrip.service_id);
//
//                    // if the service calendar has not yet been imported, import it
//                    if (feedTx.calendars != null && !feedTx.calendars.containsKey(cal.id)) {
//                        // no need to clone as they are going into completely separate mapdbs
//                        feedTx.calendars.put(cal.id, cal);
//                    }
//
//                    Trip trip = new Trip(gtfsTrip, routeIdMap.retrieveById(gtfsTrip.route_id), pat, cal);
//
//                    // TODO: query ordered stopTimes for a given trip id
//                    // FIXME: add back in stopTimes
//                    Collection<com.conveyal.gtfs.model.StopTime> stopTimes = new ArrayList<>();
//                            input.stopTimes.subMap(new Tuple2(gtfsTrip.trip_id, null), new Tuple2(gtfsTrip.trip_id, Fun.HI)).values();
//
//                    for (com.conveyal.gtfs.model.StopTime st : stopTimes) {
//                        trip.stopTimes.add(new StopTime(st, stopIdMap.retrieveById(new Tuple2<>(st.stop_id, feed.id)).id));
//                        stopTimeCount++;
//                    }
//
//                    feedTx.trips.put(trip.id, trip);
//                    processedTrips.add(tripId);
//                    tripCount++;
//
//                    // FIXME add back in total number of trips for QC
//                    if (tripCount % 1000 == 0) {
//                        LOG.info("Loaded {} / {} trips", tripCount); // input.trips.size()
//                    }
//                }
//            }

            LOG.info("Trips loaded: " + tripCount);
            synchronized (status) {
                status.message = "Trips loaded: " + tripCount;
                status.percentComplete = 90;
            }

            LOG.info("GtfsImporter: importing fares...");
            // FIXME add in fares
//            Map<String, com.conveyal.gtfs.model.Fare> fares = input.fares;
//            for (com.conveyal.gtfs.model.Fare f : fares.values()) {
//                Fare fare = new Fare(f.fare_attribute, f.fare_rules, feed);
//                feedTx.fares.put(fare.id, fare);
//                fareCount++;
//            }
            LOG.info("Fares loaded: " + fareCount);
            synchronized (status) {
                status.message = "Fares loaded: " + fareCount;
                status.percentComplete = 92;
            }
            LOG.info("Saving snapshot...");
            synchronized (status) {
                status.message = "Saving snapshot...";
                status.percentComplete = 95;
            }
            // commit the feed TXs first, so that we have orphaned data rather than inconsistent data on a commit failure
            feedTx.commit();
            gtx.commit();
            Snapshot.deactivateSnapshots(feedVersion.feedSourceId, null);
            // create an initial snapshot for this FeedVersion
            Snapshot snapshot = VersionedDataStore.takeSnapshot(editorFeed.id, feedVersion.id, "Snapshot of " + feedVersion.name, "none");


            LOG.info("Imported GTFS file: " + agencyCount + " agencies; " + routeCount + " routes;" + stopCount + " stops; " +  stopTimeCount + " stopTimes; " + tripCount + " trips;" + shapePointCount + " shapePoints");
            synchronized (status) {
                status.message = "Import complete!";
                status.percentComplete = 100;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            synchronized (status) {
                status.message = "Failed to process GTFS snapshot.";
                status.error = true;
            }
        }
        finally {
            feedTx.rollbackIfOpen();
            gtx.rollbackIfOpen();

            // FIXME: anything we need to do at the end of using Feed?
//            inputFeedTables.close();

        }
    }

    /** infer the ownership of stops based on what stops there
     * Returns a set of tuples stop ID, agency ID with GTFS IDs */
//    private SortedSet<Tuple2<String, String>> inferAgencyStopOwnership() {
//        SortedSet<Tuple2<String, String>> ret = Sets.newTreeSet();
//
//        for (com.conveyal.gtfs.model.StopTime st : input.stop_times.values()) {
//            String stopId = st.stop_id;
//            com.conveyal.gtfs.model.Trip trip = input.trips.retrieveById(st.trip_id);
//            if (trip != null) {
//                String routeId = trip.route_id;
//                String agencyId = input.routes.retrieveById(routeId).agency_id;
//                Tuple2<String, String> key = new Tuple2(stopId, agencyId);
//                ret.add(key);
//            }
//        }
//
//        return ret;
//    }

    /**
     * Create a trip pattern from the given trip.
     * Neither the TripPattern nor the TripPatternStops are saved.
     */
//    public TripPattern createTripPatternFromTrip (com.conveyal.gtfs.model.Trip gtfsTrip, FeedTx tx) {
//        TripPattern patt = new TripPattern();
//        com.conveyal.gtfs.model.Route gtfsRoute = input.routes.retrieveById(gtfsTrip.route_id);
//        patt.routeId = routeIdMap.retrieveById(gtfsTrip.route_id).id;
//        patt.feedId = feed.id;
//
//        String patternId = input.tripPatternMap.retrieveById(gtfsTrip.trip_id);
//        Pattern gtfsPattern = input.patterns.retrieveById(patternId);
//        patt.shape = gtfsPattern.geometry;
//        patt.id = gtfsPattern.pattern_id;
//
//        patt.patternStops = new ArrayList<>();
//        patt.patternDirection = TripDirection.fromGtfs(gtfsTrip.direction_id);
//
//        com.conveyal.gtfs.model.StopTime[] stopTimes =
//                input.stop_times.subMap(new Tuple2(gtfsTrip.trip_id, 0), new Tuple2(gtfsTrip.trip_id, Fun.HI)).values().toArray(new com.conveyal.gtfs.model.StopTime[0]);
//
//        if (gtfsTrip.trip_headsign != null && !gtfsTrip.trip_headsign.isEmpty())
//            patt.name = gtfsTrip.trip_headsign;
//        else
//            patt.name = gtfsPattern.name;
//
//        for (com.conveyal.gtfs.model.StopTime st : stopTimes) {
//            TripPatternStop tps = new TripPatternStop();
//
//            Stop stop = stopIdMap.retrieveById(new Tuple2(st.stop_id, patt.feedId));
//            tps.stopId = stop.id;
//
//            // set timepoint according to first gtfs value and then whether arrival and departure times are present
//            if (st.timepoint != Entity.INT_MISSING)
//                tps.timepoint = st.timepoint == 1;
//            else if (st.arrival_time != Entity.INT_MISSING && st.departure_time != Entity.INT_MISSING) {
//                tps.timepoint = true;
//            }
//            else
//                tps.timepoint = false;
//
//            if (st.departure_time != Entity.INT_MISSING && st.arrival_time != Entity.INT_MISSING)
//                tps.defaultDwellTime = st.departure_time - st.arrival_time;
//            else
//                tps.defaultDwellTime = 0;
//
//            patt.patternStops.add(tps);
//        }
//
//        patt.calcShapeDistTraveled(tx);
//
//        // infer travel times
//        if (stopTimes.length >= 2) {
//            int startOfBlock = 0;
//            // start at one because the first stop has no travel time
//            // but don't put nulls in the data
//            patt.patternStops.retrieveById(0).defaultTravelTime = 0;
//            for (int i = 1; i < stopTimes.length; i++) {
//                com.conveyal.gtfs.model.StopTime current = stopTimes[i];
//
//                if (current.arrival_time != Entity.INT_MISSING) {
//                    // interpolate times
//
//                    int timeSinceLastSpecifiedTime = current.arrival_time - stopTimes[startOfBlock].departure_time;
//
//                    double blockLength = patt.patternStops.retrieveById(i).shapeDistTraveled - patt.patternStops.retrieveById(startOfBlock).shapeDistTraveled;
//
//                    // go back over all of the interpolated stop times and interpolate them
//                    for (int j = startOfBlock + 1; j <= i; j++) {
//                        TripPatternStop tps = patt.patternStops.retrieveById(j);
//                        double distFromLastStop = patt.patternStops.retrieveById(j).shapeDistTraveled - patt.patternStops.retrieveById(j - 1).shapeDistTraveled;
//                        tps.defaultTravelTime = (int) Math.round(timeSinceLastSpecifiedTime * distFromLastStop / blockLength);
//                    }
//
//                    startOfBlock = i;
//                }
//            }
//        }
//
//        return patt;
//    }

}

