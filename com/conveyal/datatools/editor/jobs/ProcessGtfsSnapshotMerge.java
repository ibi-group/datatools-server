package com.conveyal.datatools.editor.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.editor.datastore.FeedTx;
import com.conveyal.datatools.editor.models.Snapshot;
import com.conveyal.datatools.editor.models.transit.Agency;
import com.conveyal.datatools.editor.models.transit.Fare;
import com.conveyal.datatools.editor.models.transit.Route;
import com.conveyal.datatools.editor.models.transit.Stop;
import com.conveyal.datatools.editor.models.transit.StopTime;
import com.conveyal.datatools.editor.models.transit.Trip;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.model.CalendarDate;
import com.conveyal.gtfs.model.Entity;
import com.conveyal.gtfs.model.Service;
import com.conveyal.gtfs.model.Shape;
import com.conveyal.gtfs.model.ShapePoint;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.conveyal.datatools.editor.datastore.GlobalTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import com.conveyal.datatools.editor.models.transit.*;
import org.joda.time.DateTimeConstants;

import java.awt.geom.Rectangle2D;
import java.time.LocalDate;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.Fun.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import play.i18n.Messages;

import java.io.File;
import java.util.*;
import java.util.Map.Entry;


public class ProcessGtfsSnapshotMerge extends MonitorableJob {
    public static final Logger LOG = LoggerFactory.getLogger(ProcessGtfsSnapshotMerge.class);
    /** map from GTFS agency IDs to Agencies */
    private Map<String, Agency> agencyIdMap = new HashMap<String, Agency>();
    private Map<String, Route> routeIdMap = new HashMap<String, Route>();
    /** map from (gtfs stop ID, database agency ID) -> stop */
    private Map<Tuple2<String, String>, Stop> stopIdMap = Maps.newHashMap();
    private TIntObjectMap<String> routeTypeIdMap = new TIntObjectHashMap<String>();
    private Map<String, LineString> shapes = DBMaker.newTempHashMap();

    private GTFSFeed input;
    private File gtfsFile;
    private Status status;
    private EditorFeed feed;

    /** once the merge runs this will have the ID of the created agency */
    //public String agencyId;

    public FeedVersion feedVersion;

    /*public ProcessGtfsSnapshotMerge (File gtfsFile) {
        this(gtfsFile, null);
    }*/

    public ProcessGtfsSnapshotMerge (FeedVersion feedVersion, String owner) {
        super(owner, "Processing snapshot for " + feedVersion.getFeedSource().name, JobType.PROCESS_SNAPSHOT);
        this.gtfsFile = feedVersion.getFeed();
        this.feedVersion = feedVersion;
        status = new Status();
        LOG.info("GTFS Snapshot Merge for feedVersion {}", feedVersion.id);
    }

    public void run () {
        long agencyCount = 0;
        long routeCount = 0;
        long stopCount = 0;
        long stopTimeCount = 0;
        long tripCount = 0;
        long shapePointCount = 0;
        long serviceCalendarCount = 0;
        long shapeCount = 0;
        long fareCount = 0;

        GlobalTx gtx = VersionedDataStore.getGlobalTx();

        // map from (non-gtfs) agency IDs to transactions.
        //Map<String, FeedTx> agencyTxs = Maps.newHashMap();


        // create a new feed based on this version
        FeedTx feedTx = VersionedDataStore.getFeedTx(feedVersion.feedSourceId);
        feed = new EditorFeed();
        feed.setId(feedVersion.feedSourceId);
        Rectangle2D bounds = feedVersion.getValidationSummary().bounds;
        feed.defaultLat = bounds.getCenterY();
        feed.defaultLon = bounds.getCenterX();
        gtx.feeds.put(feedVersion.feedSourceId, feed);


        try {
//            input = GTFSFeed.fromFile(gtfsFile.getAbsolutePath());
            input = DataManager.gtfsCache.get(feedVersion.id);

            LOG.info("GtfsImporter: importing feed...");

            // load the basic feed info


            // load the GTFS agencies
            for (com.conveyal.gtfs.model.Agency gtfsAgency : input.agency.values()) {
                Agency agency = new Agency(gtfsAgency, feed);
                //agencyId = agency.id;
                //agency.sourceId = sourceId;
                // don't save the agency until we've come up with the stop centroid, below.
                agencyCount++;
                // we do want to use the modified agency ID here, because everything that refers to it has a reference
                // to the agency object we updated.
                feedTx.agencies.put(agency.id, agency);
                agencyIdMap.put(gtfsAgency.agency_id, agency);
            }

            LOG.info("Agencies loaded: " + agencyCount);

            LOG.info("GtfsImporter: importing stops...");

            // infer agency ownership of stops, if there are multiple feeds
            SortedSet<Tuple2<String, String>> stopsByAgency = inferAgencyStopOwnership();

            // build agency centroids as we go
            // note that these are not actually centroids, but the center of the extent of the stops . . .
            Map<String, Envelope> stopEnvelopes = Maps.newHashMap();

            for (Agency agency : agencyIdMap.values()) {
                stopEnvelopes.put(agency.id, new Envelope());
            }

            GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);
            for (com.conveyal.gtfs.model.Stop gtfsStop : input.stops.values()) {
                Stop stop = new Stop(gtfsStop, geometryFactory, feed);
                feedTx.stops.put(stop.id, stop);
                stopIdMap.put(new Tuple2(gtfsStop.stop_id, feed.id), stop);
                stopCount++;

                /*
                // duplicate the stop for all of the feeds by which it is used
                Collection<Agency> agencies = Collections2.transform(
                        stopsByAgency.subSet(new Tuple2(gtfsStop.stop_id, null), new Tuple2(gtfsStop.stop_id, Fun.HI)),
                        new Function<Tuple2<String, String>, Agency>() {

                            @Override
                            public Agency apply(Tuple2<String, String> input) {
                                // TODO Auto-generated method stub
                                return agencyIdMap.get(input.b);
                            }
                        });

                // impossible to tell to whom unused stops belong, so give them to everyone
                if (agencies.size() == 0)
                    agencies = agencyIdMap.values();

                for (Agency agency : agencies) {
                    Stop stop = new Stop(gtfsStop, geometryFactory, agency);
                    agencyTxs.get(agency.id).stops.put(stop.id, stop);
                    stopIdMap.put(new Tuple2(gtfsStop.stop_id, agency.id), stop);
                    stopEnvelopes.get(agency.id).expandToInclude(gtfsStop.stop_lon, gtfsStop.stop_lat);
                }*/
            }

            LOG.info("Stops loaded: " + stopCount);

            LOG.info("GtfsImporter: importing routes...");

            // import routes
            for (com.conveyal.gtfs.model.Route gtfsRoute : input.routes.values()) {
                Agency agency = agencyIdMap.get(gtfsRoute.agency_id);

                if (!routeTypeIdMap.containsKey(gtfsRoute.route_type)) {
                    RouteType rt = new RouteType();
                    rt.gtfsRouteType = GtfsRouteType.fromGtfs(gtfsRoute.route_type);
                    rt.hvtRouteType = rt.gtfsRouteType.toHvt();
                    rt.description = agencyIdMap.values().iterator().next().name + " " + rt.gtfsRouteType.toString();
                    gtx.routeTypes.put(rt.id, rt);
                    routeTypeIdMap.put(gtfsRoute.route_type, rt.id);
                }

                Route route = new Route(gtfsRoute, feed, agency);

                feedTx.routes.put(route.id, route);
                routeIdMap.put(gtfsRoute.route_id, route);
                routeCount++;
            }

            LOG.info("Routes loaded:" + routeCount);

            LOG.info("GtfsImporter: importing Shapes...");

            // shapes are a part of trippatterns, so we don't actually import them into the model, we just make a map
            // shape id -> linestring which we use when building trip patterns
            // we put this map in mapdb because it can be big

            // import points
            String shapeId = null;
            List<ShapePoint> points = new ArrayList<>();
            for (ShapePoint point : input.shape_points.values()) {

                // if new shape_id is encountered
                if (!point.shape_id.equals(shapeId)) {

                    // process full list of shapePoints
                    if (shapeId != null) {

                        if (points.size() < 2) {
                            LOG.warn("Shape " + shapeId + " has fewer than two points. Using stop-to-stop geometries instead.");
                            continue;
                        }

                        Coordinate[] coords = new Coordinate[points.size()];

                        int lastSeq = Integer.MIN_VALUE;

                        int i = 0;
                        for (ShapePoint shape : points) {
                            if (shape.shape_pt_sequence <= lastSeq) {
                                LOG.warn("Shape {} has out-of-sequence points. This implies a bug in the GTFS importer. Using stop-to-stop geometries.");
                                continue;
                            }
                            lastSeq = shape.shape_pt_sequence;

                            coords[i++] = new Coordinate(shape.shape_pt_lon, shape.shape_pt_lat);
                        }

                        shapes.put(shapeId, geometryFactory.createLineString(coords));
                        shapePointCount += points.size();
                        shapeCount++;

                    }
                    // re-initalize shapeId and points
                    shapeId = point.shape_id;
                    points = new ArrayList<>();
                }
                points.add(point);

            }

            LOG.info("Shape points loaded: " + shapePointCount);
            LOG.info("Shapes loaded: " + shapeCount);

            LOG.info("GtfsImporter: importing Service Calendars...");

            // we don't put service calendars in the database just yet, because we don't know what agency they're associated with
            // we copy them into the agency database as needed
            // GTFS service ID -> ServiceCalendar
            Map<String, ServiceCalendar> calendars = Maps.newHashMap();

            for (Service svc : input.services.values()) {

                ServiceCalendar cal;

                if (svc.calendar != null) {
                    // easy case: don't have to infer anything!
                    cal = new ServiceCalendar(svc.calendar, feed);
                } else {
                    // infer a calendar
                    // number of mondays, etc. that this calendar is active
                    int monday, tuesday, wednesday, thursday, friday, saturday, sunday;
                    monday = tuesday = wednesday = thursday = friday = saturday = sunday = 0;

                    LocalDate startDate = null;
                    LocalDate endDate = null;

                    for (CalendarDate cd : svc.calendar_dates.values()) {
                        if (cd.exception_type == 2)
                            continue;

                        if (startDate == null || cd.date.isBefore(startDate))
                            startDate = cd.date;

                        if (endDate == null || cd.date.isAfter(endDate))
                            endDate = cd.date;

                        int dayOfWeek = cd.date.getDayOfWeek().getValue();

                        switch (dayOfWeek) {
                        case DateTimeConstants.MONDAY:
                            monday++;
                            break;
                        case DateTimeConstants.TUESDAY:
                            tuesday++;
                            break;
                        case DateTimeConstants.WEDNESDAY:
                            wednesday++;
                            break;
                        case DateTimeConstants.THURSDAY:
                            thursday++;
                            break;
                        case DateTimeConstants.FRIDAY:
                            friday++;
                            break;
                        case DateTimeConstants.SATURDAY:
                            saturday++;
                            break;
                        case DateTimeConstants.SUNDAY:
                            sunday++;
                            break;
                        }
                    }

                    // infer the calendar. if there is service on more than half as many as the maximum number of
                    // a particular day that has service, assume that day has service in general.
                    int maxService = Ints.max(monday, tuesday, wednesday, thursday, friday, saturday, sunday);

                    cal = new ServiceCalendar();
                    cal.feedId = feed.id;

                    if (startDate == null) {
                        // no service whatsoever
                        LOG.warn("Service ID " + svc.service_id + " has no service whatsoever");
                        startDate = LocalDate.now().minusMonths(1);
                        endDate = startDate.plusYears(1);
                        cal.monday = cal.tuesday = cal.wednesday = cal.thursday = cal.friday = cal.saturday = cal.sunday = false;
                    }
                    else {
                        // infer parameters

                        int threshold = (int) Math.round(Math.ceil((double) maxService / 2));

                        cal.monday = monday >= threshold;
                        cal.tuesday = tuesday >= threshold;
                        cal.wednesday = wednesday >= threshold;
                        cal.thursday = thursday >= threshold;
                        cal.friday = friday >= threshold;
                        cal.saturday = saturday >= threshold;
                        cal.sunday = sunday >= threshold;

                        cal.startDate = startDate;
                        cal.endDate = endDate;
                    }

                    cal.inferName();
                    cal.gtfsServiceId = svc.service_id;
                }

//                feedTx.calendars.put(cal.gtfsServiceId, cal);
                calendars.put(svc.service_id, cal);

                serviceCalendarCount++;
            }

            LOG.info("Service calendars loaded: " + serviceCalendarCount);

            LOG.info("GtfsImporter: importing trips...");

            // import trips, stop times and patterns all at once
            Map<List<String>, List<String>> patterns = input.findPatterns();

            for (Entry<List<String>, List<String>> pattern : patterns.entrySet()) {
                // it is possible, though unlikely, for two routes to have the same stopping pattern
                // we want to ensure they get different trip patterns
                Map<String, TripPattern> tripPatternsByRoute = Maps.newHashMap();

                for (String tripId : pattern.getValue()) {
                    com.conveyal.gtfs.model.Trip gtfsTrip = input.trips.get(tripId);
                    //String agencyId = agencyIdMap.get(gtfsTrip.route.agency.agency_id).id;
                    //FeedTx tx = agencyTxs.get(agencyId);

                    if (!tripPatternsByRoute.containsKey(gtfsTrip.route_id)) {
                        TripPattern pat = createTripPatternFromTrip(gtfsTrip, feedTx);
                        feedTx.tripPatterns.put(pat.id, pat);
                        tripPatternsByRoute.put(gtfsTrip.route_id, pat);
                    }

                    // there is more than one pattern per route, but this map is specific to only this pattern
                    // generally it will contain exactly one entry, unless there are two routes with identical
                    // stopping patterns.
                    // (in DC, suppose there were trips on both the E2/weekday and E3/weekend from Friendship Heights
                    //  that short-turned at Missouri and 3rd).
                    TripPattern pat = tripPatternsByRoute.get(gtfsTrip.route_id);

                    ServiceCalendar cal = calendars.get(gtfsTrip.service_id);

                    // if the service calendar has not yet been imported, import it
                    if (!feedTx.calendars.containsKey(cal.id)) {
                        // no need to clone as they are going into completely separate mapdbs
                        feedTx.calendars.put(cal.id, cal);
                    }

                    Trip trip = new Trip(gtfsTrip, routeIdMap.get(gtfsTrip.route_id), pat, cal);

                    Collection<com.conveyal.gtfs.model.StopTime> stopTimes =
                            input.stop_times.subMap(new Tuple2(gtfsTrip.trip_id, null), new Tuple2(gtfsTrip.trip_id, Fun.HI)).values();

                    for (com.conveyal.gtfs.model.StopTime st : stopTimes) {
                        trip.stopTimes.add(new StopTime(st, stopIdMap.get(new Tuple2<String, String>(st.stop_id, feed.id)).id));
                    }

                    feedTx.trips.put(trip.id, trip);

                    tripCount++;

                    if (tripCount % 1000 == 0) {
                        LOG.info("Loaded {} / {} trips", tripCount, input.trips.size());
                    }
                }
            }

            LOG.info("Trips loaded: " + tripCount);


            LOG.info("GtfsImporter: importing fares...");
            Map<String, com.conveyal.gtfs.model.Fare> fares = input.fares;
            for (com.conveyal.gtfs.model.Fare f : fares.values()) {
                com.conveyal.datatools.editor.models.transit.Fare fare = new com.conveyal.datatools.editor.models.transit.Fare(f.fare_attribute, f.fare_rules, feed);
                feedTx.fares.put(fare.id, fare);
                fareCount++;
            }
            LOG.info("Fares loaded: " + fareCount);
            // commit the feed TXs first, so that we have orphaned data rather than inconsistent data on a commit failure
            feedTx.commit();
            gtx.commit();

            // create an initial snapshot for this FeedVersion
            Snapshot snapshot = VersionedDataStore.takeSnapshot(feed.id, "Initial state for " + feedVersion.id, "none");


            LOG.info("Imported GTFS file: " + agencyCount + " agencies; " + routeCount + " routes;" + stopCount + " stops; " +  stopTimeCount + " stopTimes; " + tripCount + " trips;" + shapePointCount + " shapePoints");
        }
        finally {
            feedTx.rollbackIfOpen();
            gtx.rollbackIfOpen();

            // set job as complete
            jobFinished();
        }
    }

    /** infer the ownership of stops based on what stops there
     * Returns a set of tuples stop ID, agency ID with GTFS IDs */
    private SortedSet<Tuple2<String, String>> inferAgencyStopOwnership() {
        // agency
        SortedSet<Tuple2<String, String>> ret = Sets.newTreeSet();

        for (com.conveyal.gtfs.model.StopTime st : input.stop_times.values()) {
            String stopId = st.stop_id;
            String routeId = input.trips.get(st.trip_id).route_id;
            String agencyId = input.routes.get(routeId).agency_id;
            Tuple2<String, String> key = new Tuple2(stopId, agencyId);
            ret.add(key);
        }

        return ret;
    }

    /**
     * Create a trip pattern from the given trip.
     * Neither the trippattern nor the trippatternstops are saved.
     */
    public TripPattern createTripPatternFromTrip (com.conveyal.gtfs.model.Trip gtfsTrip, FeedTx tx) {
        TripPattern patt = new TripPattern();
        com.conveyal.gtfs.model.Route gtfsRoute = input.routes.get(gtfsTrip.route_id);
        patt.routeId = routeIdMap.get(gtfsTrip.route_id).id;
        patt.feedId = feed.id; //agencyId = agencyIdMap.get(gtfsTrip.route.agency.agency_id).id;
        if (gtfsTrip.shape_id != null) {
            if (!shapes.containsKey(gtfsTrip.shape_id)) {
                LOG.warn("Missing shape for shape ID {}, referenced by trip {}", gtfsTrip.shape_id, gtfsTrip.trip_id);
            }
            else {
                patt.shape = (LineString) shapes.get(gtfsTrip.shape_id).clone();
            }
        }

        patt.patternStops = new ArrayList<TripPatternStop>();

        com.conveyal.gtfs.model.StopTime[] stopTimes =
                input.stop_times.subMap(new Tuple2(gtfsTrip.trip_id, 0), new Tuple2(gtfsTrip.trip_id, Fun.HI)).values().toArray(new com.conveyal.gtfs.model.StopTime[0]);

        if (gtfsTrip.trip_headsign != null && !gtfsTrip.trip_headsign.isEmpty())
            patt.name = gtfsTrip.trip_headsign;
        else if (gtfsRoute.route_long_name != null)
            patt.name = String.format("{} to {} ({} stops)", gtfsRoute.route_long_name, input.stops.get(stopTimes[stopTimes.length - 1].stop_id).stop_name, stopTimes.length); // Messages.get("gtfs.named-route-pattern", gtfsTrip.route.route_long_name, input.stops.get(stopTimes[stopTimes.length - 1].stop_id).stop_name, stopTimes.length);
        else
            patt.name = String.format("to {} ({{} stops)", input.stops.get(stopTimes[stopTimes.length - 1].stop_id).stop_name, stopTimes.length); // Messages.get("gtfs.unnamed-route-pattern", input.stops.get(stopTimes[stopTimes.length - 1].stop_id).stop_name, stopTimes.length);

        for (com.conveyal.gtfs.model.StopTime st : stopTimes) {
            TripPatternStop tps = new TripPatternStop();
            Stop stop = stopIdMap.get(new Tuple2(st.stop_id, patt.feedId));
            tps.stopId = stop.id;

            if (st.timepoint != Entity.INT_MISSING)
                tps.timepoint = st.timepoint == 1;

            if (st.departure_time != Entity.INT_MISSING && st.arrival_time != Entity.INT_MISSING)
                tps.defaultDwellTime = st.departure_time - st.arrival_time;
            else
                tps.defaultDwellTime = 0;

            patt.patternStops.add(tps);
        }

        patt.calcShapeDistTraveled(tx);

        // infer travel times
        if (stopTimes.length >= 2) {
            int startOfBlock = 0;
            // start at one because the first stop has no travel time
            // but don't put nulls in the data
            patt.patternStops.get(0).defaultTravelTime = 0;
            for (int i = 1; i < stopTimes.length; i++) {
                com.conveyal.gtfs.model.StopTime current = stopTimes[i];

                if (current.arrival_time != Entity.INT_MISSING) {
                    // interpolate times

                    int timeSinceLastSpecifiedTime = current.arrival_time - stopTimes[startOfBlock].departure_time;

                    double blockLength = patt.patternStops.get(i).shapeDistTraveled - patt.patternStops.get(startOfBlock).shapeDistTraveled;

                    // go back over all of the interpolated stop times and interpolate them
                    for (int j = startOfBlock + 1; j <= i; j++) {
                        TripPatternStop tps = patt.patternStops.get(j);
                        double distFromLastStop = patt.patternStops.get(j).shapeDistTraveled - patt.patternStops.get(j - 1).shapeDistTraveled;
                        tps.defaultTravelTime = (int) Math.round(timeSinceLastSpecifiedTime * distFromLastStop / blockLength);
                    }

                    startOfBlock = i;
                }
            }
        }

        return patt;
    }

    @Override
    public Status getStatus() {
        return null;
    }
}

