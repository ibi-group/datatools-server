package com.conveyal.datatools.editor.models.transit;


import com.conveyal.datatools.editor.datastore.FeedTx;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.linearref.LinearLocation;
import com.conveyal.datatools.editor.models.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TripPattern extends Model implements Cloneable, Serializable {
    public static final long serialVersionUID = 1;
    public static final Logger LOG = LoggerFactory.getLogger(TripPattern.class);
    public String name;
    public String headsign;

    public LineString shape;
    
    // if true, use straight-line rather than shape-based distances
    public boolean useStraightLineDistances;
    
    public boolean useFrequency;
    
    public String routeId;

    public String feedId;

    public TripDirection patternDirection;

    public List<TripPatternStop> patternStops = new ArrayList<>();

    // give the UI a little information about the content of this trip pattern
    public transient int numberOfTrips;
    public transient Map<String, Long> tripCountByCalendar;


    @JsonProperty("numberOfTrips")
    public int jsonGetNumberOfTrips () {
        return numberOfTrips;
    }

    @JsonProperty("tripCountByCalendar")
    Map<String, Long> jsonGetTripCountByCalendar () { return tripCountByCalendar; }

    // do-nothing setters
    @JsonProperty("numberOfTrips")
    public void jsonSetNumberOfTrips(int numberOfTrips) { }

    @JsonProperty("tripCountByCalendar")
    public void jsonSetTripCountByCalendar(Map<String, Long> tripCountByCalendar) { }

    /** add transient info for UI with number of routes, number of trips */
    public void addDerivedInfo(final FeedTx tx) {
        Collection<Trip> trips = tx.getTripsByPattern(this.id);
        numberOfTrips = trips.size();
        tripCountByCalendar = trips.stream()
                .filter(t -> t != null && t.calendarId != null)
                .collect(Collectors.groupingBy(t -> t.calendarId, Collectors.counting()));
    }

//    /**
//     * Lines showing how stops are being snapped to the shape.
//     * @return array of LineStrings showing how stops connect to shape
//     */
//    @JsonProperty("stopConnections")
//    public LineString[] jsonGetStopConnections () {
//        if (useStraightLineDistances || shape == null)
//            return null;
//
//        final FeedTx tx = VersionedDataStore.getFeedTx(this.feedId);
//
//        try {
//            LineString[] ret = new LineString[patternStops.size()];
//
//            double[] coordDistances = getCoordDistances(shape);
//            LocationIndexedLine shapeIdx = new LocationIndexedLine(shape);
//
//            for (int i = 0; i < ret.length; i++) {
//                TripPatternStop ps = patternStops.retrieveById(i);
//
//                if (ps.shapeDistTraveled == null) {
//                    return null;
//                }
//
//                Coordinate snapped = shapeIdx.extractPoint(getLoc(coordDistances, ps.shapeDistTraveled));
//                // offset it slightly so that line creation does not fail if the stop is coincident
//                snapped.x = snapped.x - 0.00000001;
//                Stop st = tx.stops.retrieveById(ps.stopId);
//                Coordinate stop = st.location.getCoordinate();
//                ret[i] = GeoUtils.geometyFactory.createLineString(new Coordinate[] {stop, snapped});
//            }
//
//            return ret;
//        } finally {
//            tx.rollback();
//        }
//
//    }

    public TripPattern() {}

    public TripPattern(String name, String headsign, LineString shape, Route route) {
        this.name = name;
        this.headsign = headsign;
        this.shape = shape;
        this.routeId = route.id;
    }
    
    public TripPattern clone() throws CloneNotSupportedException {
        TripPattern ret = (TripPattern) super.clone();

        if (this.shape != null)
            ret.shape = (LineString) this.shape.clone();
        else
            ret.shape = null;

        ret.patternStops = new ArrayList<>();

        for (TripPatternStop ps : this.patternStops) {
            ret.patternStops.add(ps.clone());
        }

        return ret;
    }
    
    
    
    /**
     * update the trip pattern stops and the associated stop times
     * see extensive discussion in ticket #102
     * basically, we assume only one stop has changed---either it's been removed, added or moved
     * this is consistent with the use of Backbone.save in the UI, and
     * also with the principle of least magic possible
     * of course, we check to ensure that that is the case and fail if it's not
     * this lets us easily detect what has happened simply by length
     */
    public static void reconcilePatternStops(TripPattern originalTripPattern, TripPattern newTripPattern, FeedTx tx) {
        // convenience
        List<TripPatternStop> originalStops = originalTripPattern.patternStops;
        List<TripPatternStop> newStops = newTripPattern.patternStops;

        // no need to do anything
        // see #174
        if (originalStops.size() == 0)
            return;
        
        // ADDITIONS (IF DIFF == 1)
        if (originalStops.size() == newStops.size() - 1) {
            // we have an addition; find it

            int differenceLocation = -1;
            for (int i = 0; i < newStops.size(); i++) {
                if (differenceLocation != -1) {
                    // we've already found the addition
                    if (i < originalStops.size() && !originalStops.get(i).stopId.equals(newStops.get(i + 1).stopId)) {
                        // there's another difference, which we weren't expecting
                        throw new IllegalStateException("Multiple differences found when trying to detect stop addition");
                    }
                }

                // if we've reached where one trip has an extra stop, or if the stops at this position differ
                else if (i == newStops.size() - 1 || !originalStops.get(i).stopId.equals(newStops.get(i).stopId)) {
                    // we have found the difference
                    differenceLocation = i;
                }
            }

            // insert a skipped stop at the difference location
            
            for (Trip trip : tx.getTripsByPattern(originalTripPattern.id)) {
                trip.stopTimes.add(differenceLocation, null);
                // TODO: safe?
                tx.trips.put(trip.id, trip);
            }            
        }
        
        // DELETIONS
        else if (originalStops.size() == newStops.size() + 1) {
            // we have an deletion; find it
            int differenceLocation = -1;
            for (int i = 0; i < originalStops.size(); i++) {
                if (differenceLocation != -1) {
                    if (!originalStops.get(i).stopId.equals(newStops.get(i - 1).stopId)) {
                        // there is another difference, which we were not expecting
                        throw new IllegalStateException("Multiple differences found when trying to detect stop removal");
                    }
                }
                
                // we've reached the end and the only difference is length (so the last stop is the different one)
                // or we've found the difference
                else if (i == originalStops.size() - 1 || !originalStops.get(i).stopId.equals(newStops.get(i).stopId)) {
                    differenceLocation = i;
                }
            }
            
            // remove stop times for removed pattern stop
            String removedStopId = originalStops.get(differenceLocation).stopId;
            
            for (Trip trip : tx.getTripsByPattern(originalTripPattern.id)) {
                StopTime removed = trip.stopTimes.remove(differenceLocation);

                // the removed stop can be null if it was skipped. trip.stopTimes.remove will throw an exception
                // rather than returning null if we try to do a remove out of bounds.
                if (removed != null && !removed.stopId.equals(removedStopId)) {
                    throw new IllegalStateException("Attempted to remove wrong stop!");
                }

                // TODO: safe?
                tx.trips.put(trip.id, trip);
            }
        }
        
        // TRANSPOSITIONS
        else if (originalStops.size() == newStops.size()) {
            // Imagine the trip patterns pictured below (where . is a stop, and lines indicate the same stop)
            // the original trip pattern is on top, the new below
            // . . . . . . . .
            // | |  \ \ \  | |
            // * * * * * * * *
            // also imagine that the two that are unmarked are the same
            // (the limitations of ascii art, this is prettier on my whiteboard)
            // There are three regions: the beginning and end, where stopSequences are the same, and the middle, where they are not
            // The same is true of trips where stops were moved backwards
            
            // find the left bound of the changed region
            int firstDifferentIndex = 0;
            while (originalStops.get(firstDifferentIndex).stopId.equals(newStops.get(firstDifferentIndex).stopId)) {
                firstDifferentIndex++;
                
                if (firstDifferentIndex == originalStops.size())
                    // trip patterns do not differ at all, nothing to do
                    return;
            }
            
            // find the right bound of the changed region
            int lastDifferentIndex = originalStops.size() - 1;
            while (originalStops.get(lastDifferentIndex).stopId.equals(newStops.get(lastDifferentIndex).stopId)) {
                lastDifferentIndex--;
            }
            
            // TODO: write a unit test for this
            if (firstDifferentIndex == lastDifferentIndex) {
                throw new IllegalStateException("stop substitutions are not supported, region of difference must have length > 1");
            }
            
            // figure out whether a stop was moved left or right
            // note that if the stop was only moved one position, it's impossible to tell, and also doesn't matter,
            // because the requisite operations are equivalent
            int from, to;
            
            // TODO: ensure that this is all that happened (i.e. verify stop ID map inside changed region)
            if (originalStops.get(firstDifferentIndex).stopId.equals(newStops.get(lastDifferentIndex).stopId)) {
                // stop was moved right
                from = firstDifferentIndex;
                to = lastDifferentIndex;
            }

            else if (newStops.get(firstDifferentIndex).stopId.equals(originalStops.get(lastDifferentIndex).stopId)) {
                // stop was moved left
                from = lastDifferentIndex;
                to = firstDifferentIndex;
            }
            
            else {
                throw new IllegalStateException("not a simple, single move!");
            }
            
            for (Trip trip : tx.getTripsByPattern(originalTripPattern.id)) {
                StopTime moved = trip.stopTimes.remove(from);
                trip.stopTimes.add(to, moved);
                trip.invalid = true;

                // TODO: safe?
                tx.trips.put(trip.id, trip);
            }
        }
        // CHECK IF SET OF STOPS ADDED TO END OF LIST
        else if (originalStops.size() < newStops.size()) {
            // find the left bound of the changed region to check that no stops have changed in between
            int firstDifferentIndex = 0;
            while (firstDifferentIndex < originalStops.size() && originalStops.get(firstDifferentIndex).stopId.equals(newStops.get(firstDifferentIndex).stopId)) {
                firstDifferentIndex++;
            }
            if (firstDifferentIndex != originalStops.size())
                throw new IllegalStateException("When adding multiple stops to patterns, new stops must all be at the end");

            for (Trip trip : tx.getTripsByPattern(originalTripPattern.id)) {

                // insert a skipped stop for each new element in newStops
                for (int i = firstDifferentIndex; i < newStops.size(); i++) {
                    trip.stopTimes.add(i, null);
                }
                // TODO: safe?
                tx.trips.put(trip.id, trip);
            }
        }
        // OTHER STUFF IS NOT SUPPORTED
        else {
            throw new IllegalStateException("Changes to trip pattern stops must be made one at a time");
        }
    }

    /**
     * From an array of distances at coordinates and a distance, retrieveById a linear location for that distance.
     */
    private static LinearLocation getLoc(double[] distances, double distTraveled) {
        if (distTraveled < 0)
            return null;

        // this can happen due to rounding errors
        else if (distTraveled >= distances[distances.length - 1]) {
            LOG.warn("Shape dist traveled past end of shape, was {}, expected max {}, clamping", distTraveled, distances[distances.length - 1]);
            return new LinearLocation(distances.length - 1, 0);
        }

        for (int i = 1; i < distances.length; i++) {
            if (distTraveled <= distances[i]) {
                // we have found the appropriate segment
                double frac = (distTraveled - distances[i - 1]) / (distances[i] - distances[i - 1]);
                return new LinearLocation(i - 1, frac);
            }
        }

        return null;
    }

    /**
     * From an array of distances at coordinates and linear locs, retrieveById a distance for that location.
     */
    private static double getDist(double[] distances, LinearLocation loc) {
        if (loc.getSegmentIndex() == distances.length - 1)
            return distances[distances.length - 1];

        return distances[loc.getSegmentIndex()] + (distances[loc.getSegmentIndex() + 1] - distances[loc.getSegmentIndex()]) * loc.getSegmentFraction();
    }
}
