package com.conveyal.datatools.editor.datastore;

import com.conveyal.datatools.editor.models.transit.*;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import java.time.LocalDate;
import org.mapdb.Atomic;
import org.mapdb.BTreeMap;
import org.mapdb.Bind;
import org.mapdb.DB;
import org.mapdb.Fun;
import org.mapdb.Fun.Tuple2;
import com.conveyal.datatools.editor.utils.BindUtils;

import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/** a transaction in an agency database */
public class FeedTx extends DatabaseTx {
    // primary com.conveyal.datatools.editor.datastores
    // if you add another, you MUST update SnapshotTx.java
    // if you don't, not only will your new data not be backed up, IT WILL BE THROWN AWAY WHEN YOU RESTORE!
    // AND ALSO the duplicate() function below
    public BTreeMap<String, TripPattern> tripPatterns;
    public BTreeMap<String, Route> routes;
    public BTreeMap<String, Trip> trips;
    public BTreeMap<String, ServiceCalendar> calendars;
    public BTreeMap<String, ScheduleException> exceptions;
    public BTreeMap<String, Stop> stops;
    public BTreeMap<String, Agency> agencies;
    public BTreeMap<String, Fare> fares;
    // if you add anything here, see warning above!

    // secondary indices

    /** Set containing tuples <Route ID, Trip ID> */
    public NavigableSet<Tuple2<String, String>> tripsByRoute;

    /** <route ID, trip pattern ID> */
    public NavigableSet<Tuple2<String, String>> tripPatternsByRoute;

    /** <trip pattern ID, trip ID> */
    public NavigableSet<Tuple2<String, String>> tripsByTripPattern;

    /** <calendar ID, trip ID> */
    public NavigableSet<Tuple2<String, String>> tripsByCalendar;

    /** <calendar id, schedule exception id> */
    public NavigableSet<Tuple2<String, String>> exceptionsByCalendar;

    /** <<patternId, calendarId>, trip id> */
    public NavigableSet<Tuple2<Tuple2<String, String>, String>> tripsByPatternAndCalendar;

    /** major stops for this agency */
    public NavigableSet<String> majorStops;

    /** <Stop ID, Pattern ID> </Stop>trip patterns using each stop */
    public NavigableSet<Tuple2<String, String>> tripPatternsByStop;

    /** number of schedule exceptions on each date - this will always be null, 0, or 1, as we prevent save of others */
    public ConcurrentMap<LocalDate, Long> scheduleExceptionCountByDate;

    /** number of trips on each tuple2<patternId, calendar id> */
    public ConcurrentMap<Tuple2<String, String>, Long> tripCountByPatternAndCalendar;

    /** number of trips on each calendar */
    public ConcurrentMap<String, Long> tripCountByCalendar;

    /**
     * Spatial index of stops. Set<Tuple2<Tuple2<Lon, Lat>, stop ID>>
     * This is not a true spatial index, but should be sufficiently efficient for our purposes.
     * Jan Kotek describes this approach here, albeit in Czech: https://groups.google.com/forum/#!msg/mapdb/ADgSgnXzkk8/Q8J9rWAWXyMJ
     */
    public NavigableSet<Tuple2<Tuple2<Double, Double>, String>> stopsGix;

    /** snapshot versions. we use an atomic value so that they are (roughly) sequential, instead of using unordered UUIDs */
    private Atomic.Integer snapshotVersion;

//    public Atomic.Boolean editedSinceSnapshot;
    /**
     * Create a feed tx.
     */
    public FeedTx(DB tx) {
        this(tx, true);
    }

    /** Create a feed tx, optionally without secondary indices */
    public FeedTx(DB tx, boolean buildSecondaryIndices) {
        super(tx);

        tripPatterns = getMap("tripPatterns");
        routes = getMap("routes");
        trips = getMap("trips");
        calendars = getMap("calendars");
        exceptions = getMap("exceptions");
        snapshotVersion = tx.getAtomicInteger("snapshotVersion");
        stops = getMap("stops");
        agencies = getMap("agencies");
        fares = getMap("fares");

        if (buildSecondaryIndices)
            buildSecondaryIndices();

//        editedSinceSnapshot = tx.getAtomicBoolean("editedSinceSnapshot") == null ? tx.createAtomicBoolean("editedSinceSnapshot", false) : editedSinceSnapshot;
    }

    public void commit () {
        try {
//            editedSinceSnapshot.set(true);
            tx.commit();
        } catch (UnsupportedOperationException e) {
            // probably read only, but warn
            LOG.warn("Rollback failed; if this is a read-only database this is not unexpected");
        }
        closed = true;
    }

    public void buildSecondaryIndices () {
        // build secondary indices
        // we store indices in the mapdb not because we care about persistence, but because then they
        // will be managed within the context of MapDB transactions
        tripsByRoute = getSet("tripsByRoute");

        // bind the trips to the routes
        Bind.secondaryKeys(trips, tripsByRoute, (tripId, trip) -> new String[] { trip.routeId });

        tripPatternsByRoute = getSet("tripPatternsByRoute");
        Bind.secondaryKeys(tripPatterns, tripPatternsByRoute, (tripId, trip) -> new String[] { trip.routeId });

        tripsByTripPattern = getSet("tripsByTripPattern");
        Bind.secondaryKeys(trips, tripsByTripPattern, (tripId, trip) -> new String[] { trip.patternId });

        tripsByCalendar = getSet("tripsByCalendar");
        Bind.secondaryKeys(trips, tripsByCalendar, (tripId, trip) -> new String[] { trip.calendarId });

        exceptionsByCalendar = getSet("exceptionsByCalendar");
        Bind.secondaryKeys(exceptions, exceptionsByCalendar, (key, ex) -> {
            if (ex.customSchedule == null) return new String[0];

            return ex.customSchedule.toArray(new String[ex.customSchedule.size()]);
        });

        tripsByPatternAndCalendar = getSet("tripsByPatternAndCalendar");
        Bind.secondaryKeys(trips, tripsByPatternAndCalendar, (key, trip) -> new Tuple2[] { new Tuple2(trip.patternId, trip.calendarId) });

        majorStops = getSet("majorStops");
        BindUtils.subsetIndex(stops, majorStops, (key, val) -> val.majorStop != null && val.majorStop);

        tripPatternsByStop = getSet("tripPatternsByStop");
        Bind.secondaryKeys(tripPatterns, tripPatternsByStop, (key, tp) -> {
            String[] stops1 = new String[tp.patternStops.size()];

            for (int i = 0; i < stops1.length; i++) {
                stops1[i] = tp.patternStops.get(i).stopId;
            }

            return stops1;
        });

        tripCountByPatternAndCalendar = getMap("tripCountByPatternAndCalendar");
        Bind.histogram(trips, tripCountByPatternAndCalendar, (tripId, trip) -> new Tuple2(trip.patternId, trip.calendarId));

        // getting schedule exception map appears to be causing issues for some feeds
        // The names of the code writers have been changed to protect the innocent.
        try {
            scheduleExceptionCountByDate = getMap("scheduleExceptionCountByDate");
        } catch (RuntimeException e1) {
            LOG.error("Error getting scheduleExceptionCountByDate map. Getting a new one.");
            int count = 0;
            final int NEW_MAP_LIMIT = 100;
            while (true) {
                try {
                    scheduleExceptionCountByDate = getMap("scheduleExceptionCountByDateMapDBIsTheWORST" + count);
                } catch (RuntimeException e2) {
                    LOG.error("Error getting {} scheduleExceptionCountByDateMapDBIsTheWORST map. Getting a new one.", count);
                    count++;
                    if (count > NEW_MAP_LIMIT) {
                        LOG.error("Cannot create new map. Reached limit of {}", NEW_MAP_LIMIT);
                        throw e2;
                    }
                    continue;
                }
                break;
            }
        }
        BindUtils.multiHistogram(exceptions, scheduleExceptionCountByDate, (id, ex) -> ex.dates.toArray(new LocalDate[ex.dates.size()]));

        tripCountByCalendar = getMap("tripCountByCalendar");
        BindUtils.multiHistogram(trips, tripCountByCalendar, (key, trip) -> {
            if (trip.calendarId == null)
                return new String[] {};
            else
                return new String[] { trip.calendarId };
        });

        // "spatial index"
        stopsGix = getSet("stopsGix");
        Bind.secondaryKeys(stops, stopsGix, (stopId, stop) -> new Tuple2[] { new Tuple2(stop.location.getX(), stop.location.getY()) });
    }

    public Collection<Trip> getTripsByPattern(String patternId) {
        Set<Tuple2<String, String>> matchedKeys = tripsByTripPattern.subSet(new Tuple2(patternId, null), new Tuple2(patternId, Fun.HI));

        return matchedKeys.stream()
                .map(input -> trips.get(input.b))
                .collect(Collectors.toList());
    }

    public Collection<Trip> getTripsByRoute(String routeId) {
        Set<Tuple2<String, String>> matchedKeys = tripsByRoute.subSet(new Tuple2(routeId, null), new Tuple2(routeId, Fun.HI));

        return matchedKeys.stream().map(input -> trips.get(input.b)).collect(Collectors.toList());
    }

    public Collection<Trip> getTripsByCalendar(String calendarId) {
        Set<Tuple2<String, String>> matchedKeys = tripsByCalendar.subSet(new Tuple2(calendarId, null), new Tuple2(calendarId, Fun.HI));

        return matchedKeys.stream().map(input -> trips.get(input.b)).collect(Collectors.toList());
    }

    public Collection<ScheduleException> getExceptionsByCalendar(String calendarId) {
        Set<Tuple2<String, String>> matchedKeys = exceptionsByCalendar.subSet(new Tuple2(calendarId, null), new Tuple2(calendarId, Fun.HI));

        return matchedKeys.stream().map(input -> exceptions.get(input.b)).collect(Collectors.toList());
    }

    public Collection<Trip> getTripsByPatternAndCalendar(String patternId, String calendarId) {
        Set<Tuple2<Tuple2<String, String>, String>> matchedKeys =
                tripsByPatternAndCalendar.subSet(new Tuple2(new Tuple2(patternId, calendarId), null), new Tuple2(new Tuple2(patternId, calendarId), Fun.HI));

        return matchedKeys.stream().map(input -> trips.get(input.b)).collect(Collectors.toList());
    }

    public Collection<Stop> getStopsWithinBoundingBox (double north, double east, double south, double west) {
        // find all the stops in this bounding box
        // avert your gaze please as I write these generic types
        Tuple2<Double, Double> min = new Tuple2<Double, Double>(west, south);
        Tuple2<Double, Double> max = new Tuple2<Double, Double>(east, north);

        Set<Tuple2<Tuple2<Double, Double>, String>> matchedKeys =
                stopsGix.subSet(new Tuple2(min, null), new Tuple2(max, Fun.HI));

        Collection<Stop> matchedStops = matchedKeys.stream().map(input -> stops.get(input.b)).collect(Collectors.toList());

        return matchedStops;
    }

    public Collection<TripPattern> getTripPatternsByStop (String id) {
        Collection<Tuple2<String, String>> matchedPatterns = tripPatternsByStop.subSet(new Tuple2(id, null), new Tuple2(id, Fun.HI));
        return matchedPatterns.stream()
                .map(input -> tripPatterns.get(input.b))
                .collect(Collectors.toList());
    }

    /** return the version number of the next snapshot */
    public int getNextSnapshotId () {
        return snapshotVersion.incrementAndGet();
    }

    /** duplicate an EditorFeed in its entirety. Return the new feed ID */
    public static String duplicate (String feedId) {
        final String newId = UUID.randomUUID().toString();

        FeedTx feedTx = VersionedDataStore.getFeedTx(feedId);

        DB newDb = VersionedDataStore.getRawFeedTx(newId);

        copy(feedTx, newDb, newId);

        // rebuild indices
        FeedTx newTx = new FeedTx(newDb);
        newTx.commit();

        feedTx.rollback();

        GlobalTx gtx = VersionedDataStore.getGlobalTx();
        EditorFeed feedCopy;

        try {
            feedCopy = gtx.feeds.get(feedId).clone();
        } catch (CloneNotSupportedException e) {
            // not likely
            e.printStackTrace();
            gtx.rollback();
            return null;
        }

        feedCopy.id = newId;
//        a2.name = Messages.get("agency.copy-of", a2.name);

        gtx.feeds.put(feedCopy.id, feedCopy);

        gtx.commit();

        return newId;
    }

    /** copy a feed database */
    static void copy (FeedTx feedTx, DB newDb, final String newFeedId) {
        // copy everything
        try {
            Iterator<Tuple2<String, Stop>> stopSource = Iterators.transform(
                    FeedTx.<String, Stop>pumpSourceForMap(feedTx.stops),
                    (Function<Tuple2<String, Stop>, Tuple2<String, Stop>>) input -> {
                        Stop st;
                        try {
                            st = input.b.clone();
                        } catch (CloneNotSupportedException e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                        st.feedId = newFeedId;
                        return new Tuple2(input.a, st);
                    });
            pump(newDb, "stops", stopSource);

            Iterator<Tuple2<String, Trip>> tripSource = Iterators.transform(
                    FeedTx.<String, Trip>pumpSourceForMap(feedTx.trips),
                    (Function<Tuple2<String, Trip>, Tuple2<String, Trip>>) input -> {
                        Trip st;
                        try {
                            st = input.b.clone();
                        } catch (CloneNotSupportedException e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                        st.feedId = newFeedId;
                        return new Tuple2(input.a, st);
                    });
            pump(newDb, "trips", tripSource);

            Iterator<Tuple2<String, TripPattern>> pattSource = Iterators.transform(
                    FeedTx.<String, TripPattern>pumpSourceForMap(feedTx.tripPatterns),
                    (Function<Tuple2<String, TripPattern>, Tuple2<String, TripPattern>>) input -> {
                        TripPattern st;
                        try {
                            st = input.b.clone();
                        } catch (CloneNotSupportedException e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                        st.feedId = newFeedId;
                        return new Tuple2(input.a, st);
                    });
            pump(newDb, "tripPatterns", pattSource);

            Iterator<Tuple2<String, Route>> routeSource = Iterators.transform(
                    FeedTx.<String, Route>pumpSourceForMap(feedTx.routes),
                    (Function<Tuple2<String, Route>, Tuple2<String, Route>>) input -> {
                        Route st;
                        try {
                            st = input.b.clone();
                        } catch (CloneNotSupportedException e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                        st.feedId = newFeedId;
                        return new Tuple2(input.a, st);
                    });
            pump(newDb, "routes", routeSource);

            Iterator<Tuple2<String, ServiceCalendar>> calSource = Iterators.transform(
                    FeedTx.<String, ServiceCalendar>pumpSourceForMap(feedTx.calendars),
                    (Function<Tuple2<String, ServiceCalendar>, Tuple2<String, ServiceCalendar>>) input -> {
                        ServiceCalendar st;
                        try {
                            st = input.b.clone();
                        } catch (CloneNotSupportedException e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                        st.feedId = newFeedId;
                        return new Tuple2(input.a, st);
                    });
            pump(newDb, "calendars", calSource);

            Iterator<Tuple2<String, ScheduleException>> exSource = Iterators.transform(
                    FeedTx.<String, ScheduleException>pumpSourceForMap(feedTx.exceptions),
                    (Function<Tuple2<String, ScheduleException>, Tuple2<String, ScheduleException>>) input -> {
                        ScheduleException st;
                        try {
                            st = input.b.clone();
                        } catch (CloneNotSupportedException e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                        st.feedId = newFeedId;
                        return new Tuple2(input.a, st);
                    });
            pump(newDb, "exceptions", exSource);

            Iterator<Tuple2<String, Agency>> agencySource = Iterators.transform(
                    FeedTx.pumpSourceForMap(feedTx.agencies),
                    new Function<Tuple2<String, Agency>, Tuple2<String, Agency>>() {
                        @Override
                        public Tuple2<String, Agency> apply(Tuple2<String, Agency> input) {
                            Agency agency;
                            try {
                                agency = input.b.clone();
                            } catch (CloneNotSupportedException e) {
                                e.printStackTrace();
                                throw new RuntimeException(e);
                            }
                            agency.feedId = newFeedId;
                            return new Tuple2(input.a, agency);
                        }
                    });
            pump(newDb, "agencies", agencySource);

            Iterator<Tuple2<String, Fare>> fareSource = Iterators.transform(
                    FeedTx.pumpSourceForMap(feedTx.agencies),
                    new Function<Tuple2<String, Fare>, Tuple2<String, Fare>>() {
                        @Override
                        public Tuple2<String, Fare> apply(Tuple2<String, Fare> input) {
                            Fare fare;
                            try {
                                fare = input.b.clone();
                            } catch (CloneNotSupportedException e) {
                                e.printStackTrace();
                                throw new RuntimeException(e);
                            }
                            fare.feedId = newFeedId;
                            return new Tuple2(input.a, fare);
                        }
                    });
            pump(newDb, "fares", fareSource);

            // copy histograms
            pump(newDb, "tripCountByCalendar", (BTreeMap) feedTx.tripCountByCalendar);
            pump(newDb, "scheduleExceptionCountByDate", (BTreeMap) feedTx.scheduleExceptionCountByDate);
            pump(newDb, "tripCountByPatternAndCalendar", (BTreeMap) feedTx.tripCountByPatternAndCalendar);

        }
        catch (Exception e) {
            newDb.rollback();
            feedTx.rollback();
            throw new RuntimeException(e);
        }
    }
}