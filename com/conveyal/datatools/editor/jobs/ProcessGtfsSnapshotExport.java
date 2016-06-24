package com.conveyal.datatools.editor.jobs;

import com.beust.jcommander.internal.Lists;
import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.model.CalendarDate;
import com.conveyal.gtfs.model.Entity;
import com.conveyal.gtfs.model.Frequency;
import com.conveyal.gtfs.model.Shape;
import com.conveyal.gtfs.model.ShapePoint;
import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Coordinate;
import com.conveyal.datatools.editor.datastore.FeedTx;
import com.conveyal.datatools.editor.datastore.GlobalTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.Snapshot;
import com.conveyal.datatools.editor.models.transit.*;
import java.time.LocalDate;

import org.mapdb.Fun;
import org.mapdb.Fun.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.conveyal.datatools.editor.utils.GeoUtils;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public class ProcessGtfsSnapshotExport implements Runnable {
    public static final Logger LOG = LoggerFactory.getLogger(ProcessGtfsSnapshotExport.class);
    private Collection<Tuple2<String, Integer>> snapshots;
    private File output;
    private LocalDate startDate;
    private LocalDate endDate;

    /** Export the named snapshots to GTFS */
    public ProcessGtfsSnapshotExport(Collection<Tuple2<String, Integer>> snapshots, File output, LocalDate startDate, LocalDate endDate) {
        this.snapshots = snapshots;
        this.output = output;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    /**
     * Export the master branch of the named feeds to GTFS. The boolean variable can be either true or false, it is only to make this
     * method have a different erasure from the other
     */
    public ProcessGtfsSnapshotExport(Collection<String> agencies, File output, LocalDate startDate, LocalDate endDate, boolean isagency) {
        this.snapshots = Lists.newArrayList(agencies.size());

        for (String agency : agencies) {
            // leaving version null will cause master to be used
            this.snapshots.add(new Tuple2<String, Integer>(agency, null));
        }

        this.output = output;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    /**
     * Export this snapshot to GTFS, using the validity range in the snapshot.
     */
    public ProcessGtfsSnapshotExport (Snapshot snapshot, File output) {
        this(Arrays.asList(new Tuple2[] { snapshot.id }), output, snapshot.validFrom, snapshot.validTo);
    }

    @Override
    public void run() {
        GTFSFeed feed = new GTFSFeed();

        GlobalTx gtx = VersionedDataStore.getGlobalTx();
        FeedTx atx = null;

        try {
            for (Tuple2<String, Integer> ssid : snapshots) {
                String feedId = ssid.a;
                final FeedTx tx = VersionedDataStore.getFeedTx(feedId);
                Collection<Agency> agencies = tx.agencies.values();
                for (Agency agency : agencies) {
                    String agencyId = agency.gtfsAgencyId;
//                    Agency agency = gtx.feeds.get(agencyId);
                    com.conveyal.gtfs.model.Agency gtfsAgency = agency.toGtfs();
                    LOG.info("Exporting agency %s", gtfsAgency);

                    if (ssid.b == null) {
                        atx = VersionedDataStore.getFeedTx(agencyId);
                    }
                    else {
                        atx = VersionedDataStore.getFeedTx(agencyId, ssid.b);
                    }

                    // write the feeds.txt entry
                    feed.agency.put(agencyId, agency.toGtfs());

                    // write all of the calendars and calendar dates
                    if(atx.calendars != null) {
                        for (ServiceCalendar cal : atx.calendars.values()) {
                            com.conveyal.gtfs.model.Service gtfsService = cal.toGtfs(toGtfsDate(startDate), toGtfsDate(endDate));
                            // note: not using user-specified IDs

                            // add calendar dates
                            if (atx.exceptions != null) {
                                for (ScheduleException ex : atx.exceptions.values()) {
                                    for (LocalDate date : ex.dates) {
                                        if (date.isBefore(startDate) || date.isAfter(endDate))
                                            // no need to write dates that do not apply
                                            continue;

                                        CalendarDate cd = new CalendarDate();
                                        cd.date = date;
                                        cd.service_id = gtfsService.service_id;
                                        cd.exception_type = ex.serviceRunsOn(cal) ? 1 : 2;

                                        if (gtfsService.calendar_dates.containsKey(date))
                                            throw new IllegalArgumentException("Duplicate schedule exceptions on " + date.toString());

                                        gtfsService.calendar_dates.put(date, cd);
                                    }
                                }
                            }

                            feed.services.put(gtfsService.service_id, gtfsService);
                        }
                    }

                    Map<String, com.conveyal.gtfs.model.Route> gtfsRoutes = Maps.newHashMap();

                    // write the routes
                    if(atx.routes != null) {
                        for (Route route : atx.routes.values()) {
                            // only export approved routes
                            if (route.status == StatusType.APPROVED) {
                                com.conveyal.gtfs.model.Route gtfsRoute = route.toGtfs(gtfsAgency, gtx);

                                feed.routes.put(route.getGtfsId(), gtfsRoute);

                                gtfsRoutes.put(route.id, gtfsRoute);
                            }
                        }
                    }

                    // write the trips on those routes
                    // we can't use the trips-by-route index because we may be exporting a snapshot database without indices
                    if(atx.trips != null) {
                        for (Trip trip : atx.trips.values()) {
                            if (!gtfsRoutes.containsKey(trip.routeId)) {
                                LOG.warn("Trip {} has not matching route", trip);
                                continue;
                            }

                            com.conveyal.gtfs.model.Route gtfsRoute = gtfsRoutes.get(trip.routeId);
                            Route route = atx.routes.get(trip.routeId);

                            com.conveyal.gtfs.model.Trip gtfsTrip = new com.conveyal.gtfs.model.Trip();

                            gtfsTrip.block_id = trip.blockId;
                            gtfsTrip.route_id = gtfsRoute.route_id;
                            gtfsTrip.trip_id = trip.getGtfsId();
                            // not using custom ids for calendars
                            gtfsTrip.service_id = feed.services.get(trip.calendarId).service_id;
                            gtfsTrip.trip_headsign = trip.tripHeadsign;
                            gtfsTrip.trip_short_name = trip.tripShortName;
                            gtfsTrip.direction_id = trip.tripDirection == TripDirection.A ? 0 : 1;

                            TripPattern pattern = atx.tripPatterns.get(trip.patternId);

                            Tuple2<String, Integer> nextKey = feed.shape_points.ceilingKey(new Tuple2(pattern.id, null));
                            if ((nextKey == null || !pattern.id.equals(nextKey.a)) && pattern.shape != null && !pattern.useStraightLineDistances) {
                                // this shape has not yet been saved
                                double[] coordDistances = GeoUtils.getCoordDistances(pattern.shape);

                                for (int i = 0; i < coordDistances.length; i++) {
                                    Coordinate coord = pattern.shape.getCoordinateN(i);
                                    ShapePoint shape = new ShapePoint(pattern.id, coord.y, coord.x, i + 1, coordDistances[i]);
                                    feed.shape_points.put(new Tuple2(pattern.id, shape.shape_pt_sequence), shape);
                                }
                            }

                            if (pattern.shape != null && !pattern.useStraightLineDistances)
                                gtfsTrip.shape_id = pattern.id;

                            if (trip.wheelchairBoarding != null) {
                                if (trip.wheelchairBoarding.equals(AttributeAvailabilityType.AVAILABLE))
                                    gtfsTrip.wheelchair_accessible = 1;

                                else if (trip.wheelchairBoarding.equals(AttributeAvailabilityType.UNAVAILABLE))
                                    gtfsTrip.wheelchair_accessible = 2;

                                else
                                    gtfsTrip.wheelchair_accessible = 0;

                            } else if (route.wheelchairBoarding != null) {
                                if (route.wheelchairBoarding.equals(AttributeAvailabilityType.AVAILABLE))
                                    gtfsTrip.wheelchair_accessible = 1;

                                else if (route.wheelchairBoarding.equals(AttributeAvailabilityType.UNAVAILABLE))
                                    gtfsTrip.wheelchair_accessible = 2;

                                else
                                    gtfsTrip.wheelchair_accessible = 0;

                            }

                            feed.trips.put(gtfsTrip.trip_id, gtfsTrip);

                            TripPattern patt = atx.tripPatterns.get(trip.patternId);

                            Iterator<TripPatternStop> psi = patt.patternStops.iterator();

                            int stopSequence = 1;

                            // write the stop times
                            for (StopTime st : trip.stopTimes) {
                                TripPatternStop ps = psi.next();
                                if (st == null)
                                    continue;

                                Stop stop = atx.stops.get(st.stopId);

                                if (!st.stopId.equals(ps.stopId)) {
                                    throw new IllegalStateException("Trip " + trip.id + " does not match its pattern!");
                                }

                                com.conveyal.gtfs.model.StopTime gst = new com.conveyal.gtfs.model.StopTime();
                                gst.arrival_time = st.arrivalTime != null ? st.arrivalTime : Entity.INT_MISSING;
                                gst.departure_time = st.departureTime != null ? st.departureTime : Entity.INT_MISSING;

                                if (st.dropOffType != null)
                                    gst.drop_off_type = st.dropOffType.toGtfsValue();
                                else if (stop.dropOffType != null)
                                    gst.drop_off_type = stop.dropOffType.toGtfsValue();

                                if (st.pickupType != null)
                                    gst.pickup_type = st.pickupType.toGtfsValue();
                                else if (stop.dropOffType != null)
                                    gst.drop_off_type = stop.dropOffType.toGtfsValue();

                                gst.shape_dist_traveled = ps.shapeDistTraveled;
                                gst.stop_headsign = st.stopHeadsign;
                                gst.stop_id = stop.getGtfsId();

                                // write the stop as needed
                                if (!feed.stops.containsKey(gst.stop_id)) {
                                    feed.stops.put(gst.stop_id, stop.toGtfs());
                                }

                                gst.stop_sequence = stopSequence++;

                                if (ps.timepoint != null)
                                    gst.timepoint = ps.timepoint ? 1 : 0;
                                else
                                    gst.timepoint = Entity.INT_MISSING;

                                gst.trip_id = gtfsTrip.trip_id;

                                feed.stop_times.put(new Tuple2(gtfsTrip.trip_id, gst.stop_sequence), gst);
                            }

                            // create frequencies as needed
                            if (trip.useFrequency != null && trip.useFrequency) {
                                Frequency f = new Frequency();
                                f.trip_id = gtfsTrip.trip_id;
                                f.start_time = trip.startTime;
                                f.end_time = trip.endTime;
                                f.exact_times = 0;
                                f.headway_secs = trip.headway;
                                feed.frequencies.add(Fun.t2(gtfsTrip.trip_id, f));
                            }
                        }
                    }
                }

            }


            feed.toFile(output.getAbsolutePath());
        } finally {
            gtx.rollbackIfOpen();
            if (atx != null) atx.rollbackIfOpen();
        }
    }

    public static int toGtfsDate (LocalDate date) {
        return date.getYear() * 10000 + date.getMonthValue() * 100 + date.getDayOfMonth();
    }
}

