package com.conveyal.datatools.editor.models.transit;


import com.beust.jcommander.internal.Sets;
import com.conveyal.gtfs.model.Calendar;
import com.conveyal.gtfs.model.Service;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.conveyal.datatools.editor.datastore.FeedTx;
import com.conveyal.datatools.editor.models.Model;
import java.time.LocalDate;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ServiceCalendar extends Model implements Cloneable, Serializable {
    public static final long serialVersionUID = 1;

    public String feedId;
    public String gtfsServiceId;
    public String description;
    public Boolean monday;
    public Boolean tuesday;
    public Boolean wednesday;
    public Boolean thursday;
    public Boolean friday;
    public Boolean saturday;
    public Boolean sunday;
    public LocalDate startDate;
    public LocalDate endDate;
    
    public ServiceCalendar() {}
    
    public ServiceCalendar(Calendar calendar, EditorFeed feed) {
        this.gtfsServiceId = calendar.service_id;
        this.monday = calendar.monday == 1;
        this.tuesday = calendar.tuesday == 1;
        this.wednesday = calendar.wednesday == 1;
        this.thursday = calendar.thursday == 1;
        this.friday = calendar.friday == 1;
        this.saturday = calendar.saturday == 1;
        this.sunday = calendar.sunday == 1;
        this.startDate = calendar.start_date;
        this.endDate = calendar.end_date;
        inferName();
        this.feedId = feed.id;
    }
    
    public ServiceCalendar clone () throws CloneNotSupportedException {
        return (ServiceCalendar) super.clone();
    }

    // TODO: time zones
    private static LocalDate fromGtfs(int date) {
        int day = date % 100;
        date -= day;
        int month = (date % 10000) / 100;
        date -= month * 100;
        int year = date / 10000;

        return LocalDate.of(year, month, day);
    }

    // give the UI a little information about the content of this calendar
    public transient Long numberOfTrips;

    @JsonProperty("numberOfTrips")
    public Long jsonGetNumberOfTrips () {
        return numberOfTrips;
    }

    public transient Map<String, Long> routes;

    @JsonProperty("routes")
    public Map<String, Long> jsonGetRoutes () {
        return routes;
    }

    // do-nothing setters
    @JsonProperty("numberOfTrips")
    public void jsonSetNumberOfTrips(Long numberOfTrips) { }

    @JsonProperty("routes")
    public void jsonSetRoutes(Collection<String> routes) { }

    /**
     * Infer the name of this calendar
     */
    public void inferName () {
        StringBuilder sb = new StringBuilder(14);

        if (monday)
            sb.append("Mo");
        
        if (tuesday)
            sb.append("Tu");
        
        if (wednesday)
            sb.append("We");
        
        if (thursday)
            sb.append("Th");
        
        if (friday)
            sb.append("Fr");
        
        if (saturday)
            sb.append("Sa");
        
        if (sunday)
            sb.append("Su");
        
        this.description = sb.toString();
        
        if (this.description.equals("") && this.gtfsServiceId != null)
            this.description = gtfsServiceId;
    }

    public String toString() {

        String str = "";

        if(this.monday)
            str += "Mo";

        if(this.tuesday)
            str += "Tu";

        if(this.wednesday)
            str += "We";

        if(this.thursday)
            str += "Th";

        if(this.friday)
            str += "Fr";

        if(this.saturday)
            str += "Sa";

        if(this.sunday)
            str += "Su";

        return str;
    }

    /**
     * Convert this service to a GTFS service calendar.
     * @param startDate int, in GTFS format: YYYYMMDD
     * @param endDate int, again in GTFS format
     */
    public Service toGtfs(int startDate, int endDate) {
        Service ret = new Service(id);
        ret.calendar = new Calendar();
        ret.calendar.service_id = ret.service_id;
        ret.calendar.start_date = fromGtfs(startDate);
        ret.calendar.end_date = fromGtfs(endDate);
        ret.calendar.sunday     = sunday    ? 1 : 0;
        ret.calendar.monday     = monday    ? 1 : 0;
        ret.calendar.tuesday    = tuesday   ? 1 : 0;
        ret.calendar.wednesday  = wednesday ? 1 : 0;
        ret.calendar.thursday   = thursday  ? 1 : 0;
        ret.calendar.friday     = friday    ? 1 : 0;
        ret.calendar.saturday   = saturday  ? 1 : 0;

        // TODO: calendar dates
        return ret;
    }

    // equals and hashcode use DB ID; they are used to put service calendar dates into a HashMultimap in ProcessGtfsSnapshotExport
    public int hashCode () {
        return id.hashCode();
    }

    public boolean equals(Object o) {
        if (o instanceof ServiceCalendar) {
            ServiceCalendar c = (ServiceCalendar) o;

            return id.equals(c.id);
        }

        return false;
    }

    /**
     * Used to represent a service calendar and its service on a particular route.
     */
    public static class ServiceCalendarForPattern {
        public String description;
        public String id;
        public long routeTrips;

        public ServiceCalendarForPattern(ServiceCalendar cal, TripPattern patt, long routeTrips    ) {
            this.description = cal.description;
            this.id = cal.id;
            this.routeTrips = routeTrips;
        }
    }

    /** add transient info for UI with number of routes, number of trips */
    public void addDerivedInfo(final FeedTx tx) {
        this.numberOfTrips = tx.tripCountByCalendar.get(this.id);

        if (this.numberOfTrips == null)
            this.numberOfTrips = 0L;

        // note that this is not ideal as we are fetching all of the trips. however, it's not really very possible
        // with MapDB to have an index involving three tables.
        Map<String, Long> tripsForRoutes = new HashMap<>();
        for (Trip trip : tx.getTripsByCalendar(this.id)) {
            if (trip == null) continue;
            Long count = 0L;

            /**
             * if for some reason, routeId ever was set to null (or never properly initialized),
             * take care of that here so we don't run into null map errors.
             */
            if (trip.routeId == null) {
                trip.routeId = tx.tripPatterns.get(trip.patternId).routeId;
            }
            if (tripsForRoutes.containsKey(trip.routeId)) {
                count = tripsForRoutes.get(trip.routeId);
            }
            if (trip.routeId != null) {
                tripsForRoutes.put(trip.routeId, count + 1);
            }
        }
        this.routes = tripsForRoutes;
    }
}
