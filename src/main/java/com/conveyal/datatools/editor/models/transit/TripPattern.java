package com.conveyal.datatools.editor.models.transit;


import com.conveyal.datatools.editor.datastore.FeedTx;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.linearref.LinearLocation;
import com.vividsolutions.jts.linearref.LocationIndexedLine;
import com.conveyal.datatools.editor.models.Model;
import org.geotools.referencing.GeodeticCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.conveyal.datatools.editor.utils.GeoUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.conveyal.datatools.editor.utils.GeoUtils.getCoordDistances;

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

    // cast generic Geometry object to LineString because jackson2-geojson library only returns generic Geometry objects
    @JsonProperty
    public void setShape (Geometry g) {
        this.shape = (LineString) g;
    }

//    public void calcShapeDistTraveled () {
//        FeedTx tx = VersionedDataStore.getFeedTx(feedId);
//        calcShapeDistTraveled(tx);
//        tx.rollback();
//    }
    
    /**
     * Calculate the shape dist traveled along the current shape. Do this by snapping points but constraining order.
     * 
     * To make this a bit more formal, here is the algorithm:
     * 
     * 1. We snap each stop to the nearest point on the shape, sliced by the shape_dist_traveled of the previous stop to ensure monotonicity.
     * 2. then compute the distance from stop to snapped point
     * 3. multiply by 2, create a buffer of that radius around the stop, and intersect with the shape.
     * 4. if it intersects in 1 or 2 places, assume that you have found the correct location for that stop and
     *    "fix" it into that position.
     * 5. otherwise, mark it to be returned to on the second pass
     * 6. on the second pass, just snap to the closest point on the subsection of the shape defined by the previous and next stop positions.
     */
    public void calcShapeDistTraveled(final FeedTx tx) {
        if (patternStops.size() == 0)
            return;

        // we don't actually store shape_dist_traveled, but rather the distance from the previous point along the shape
        // however, for the algorithm it's more useful to have the cumulative dist traveled
        double[] shapeDistTraveled = new double[patternStops.size()];

        useStraightLineDistances = false;

        if (shape == null) {
            calcShapeDistTraveledStraightLine(tx);
            return;
        }

        // compute the shape dist traveled of each coordinate of the shape
        double[] shapeDist = getCoordDistances(shape);

        double[] coordDist = shapeDist;

        for (int i = 0; i < shapeDistTraveled.length; i++) {
            shapeDistTraveled[i] = -1;
        }

        // location along the entire shape
        LocationIndexedLine shapeIdx = new LocationIndexedLine(shape);
        // location along the subline currently being considered
        LocationIndexedLine subIdx = shapeIdx;

        LineString subShape;

        double lastShapeDistTraveled = 0;

        int fixed = 0;

        GeodeticCalculator gc = new GeodeticCalculator();

        // detect backwards shapes
        int backwards = 0;

        double lastPos = -1;
        for (TripPatternStop tps : patternStops) {
            Stop stop = tx.stops.get(tps.stopId);
            double pos = getDist(shapeDist, shapeIdx.project(stop.location.getCoordinate()));

            if (lastPos > 0) {
                if (pos > lastPos)
                    backwards--;
                else if (pos > lastPos)
                    backwards++;
            }

            lastPos = pos;
        }

        if (backwards > 0) {
            LOG.warn("Detected likely backwards shape for trip pattern {} ({}) on route {}, reversing", id, name, routeId);
            this.shape = (LineString) this.shape.reverse();
            calcShapeDistTraveled(tx);
            return;
        }
        else if (backwards == 0) {
            LOG.warn("Unable to tell if shape is backwards for trip pattern {} ({}) on route {}, assuming it is correct", id, name, routeId);
        }

        // first pass: fix the obvious stops
        for (int i = 0; i < shapeDistTraveled.length; i++) {
            TripPatternStop tps = patternStops.get(i);
            Stop stop = tx.stops.get(tps.stopId);
            LinearLocation candidateLoc = subIdx.project(stop.location.getCoordinate());
            Coordinate candidatePt = subIdx.extractPoint(candidateLoc);

            // step 2: compute distance
            gc.setStartingGeographicPoint(stop.location.getX(), stop.location.getY());
            gc.setDestinationGeographicPoint(candidatePt.x, candidatePt.y);
            double dist = gc.getOrthodromicDistance();

            // don't snap stops more than 1km
            if (dist > 1000) {
                LOG.warn("Stop is more than 1km from its shape, using straight-line distances");
                this.calcShapeDistTraveledStraightLine(tx);
                return;
            }

            // step 3: compute buffer
            // add 5m to the buffer so that if the stop sits exactly atop two lines we don't just pick one
            Polygon buffer = GeoUtils.bufferGeographicPoint(stop.location.getCoordinate(), dist * 2 + 5, 20);

            Geometry intersection = buffer.intersection(shape);
            if (intersection.getNumGeometries() == 1) {
                // good, only one intersection
                shapeDistTraveled[i] = lastShapeDistTraveled + getDist(coordDist, candidateLoc);
                lastShapeDistTraveled = shapeDistTraveled[i];

                // recalculate shape dist traveled and idx
                subShape = (LineString) subIdx.extractLine(candidateLoc, subIdx.getEndIndex());
                subIdx = new LocationIndexedLine(subShape);

                coordDist = getCoordDistances(subShape);

                fixed++;
            }
        }

        LOG.info("Fixed {} / {} stops after first round for trip pattern {} ({}) on route {}", fixed, shapeDistTraveled.length, id, name, routeId);

        // pass 2: fix the rest of the stops
        lastShapeDistTraveled = 0;
        for (int i = 0; i < shapeDistTraveled.length; i++) {
            TripPatternStop tps = patternStops.get(i);
            Stop stop = tx.stops.get(tps.stopId);

            if (shapeDistTraveled[i] >= 0) {
                lastShapeDistTraveled = shapeDistTraveled[i];
                continue;
            }

            // find the next shape dist traveled
            double nextShapeDistTraveled = shapeDist[shapeDist.length - 1];
            for (int j = i; j < shapeDistTraveled.length; j++) {
                if (shapeDistTraveled[j] >= 0) {
                    nextShapeDistTraveled = shapeDistTraveled[j];
                    break;
                }
            }

            // create and index the subshape
            // recalculate shape dist traveled and idx
            subShape = (LineString) shapeIdx.extractLine(getLoc(shapeDist, lastShapeDistTraveled), getLoc(shapeDist, nextShapeDistTraveled));

            if (subShape.getLength() < 0.00000001) {
                LOG.warn("Two stops on trip pattern {} map to same point on shape", id);
                shapeDistTraveled[i] = lastShapeDistTraveled;
                continue;
            }

            subIdx = new LocationIndexedLine(subShape);

            coordDist = getCoordDistances(subShape);

            LinearLocation loc = subIdx.project(stop.location.getCoordinate());
            shapeDistTraveled[i] = lastShapeDistTraveled + getDist(coordDist, loc);
            lastShapeDistTraveled = shapeDistTraveled[i];
        }

        // assign default distances
        for (int i = 0; i < shapeDistTraveled.length; i++) {
            patternStops.get(i).shapeDistTraveled = shapeDistTraveled[i];
        }
    }

    /** Calculate distances using straight line geometries */
    public void calcShapeDistTraveledStraightLine(FeedTx tx) {
        useStraightLineDistances = true;
        GeodeticCalculator gc = new GeodeticCalculator();
        Stop prev = tx.stops.get(patternStops.get(0).stopId);
        patternStops.get(0).shapeDistTraveled = 0D;
        double previousDistance = 0;
        for (int i = 1; i < patternStops.size(); i++) {
            TripPatternStop ps = patternStops.get(i);
            Stop stop = tx.stops.get(ps.stopId);
            gc.setStartingGeographicPoint(prev.location.getX(), prev.location.getY());
            gc.setDestinationGeographicPoint(stop.location.getX(), stop.location.getY());
            previousDistance = ps.shapeDistTraveled = previousDistance + gc.getOrthodromicDistance();
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
