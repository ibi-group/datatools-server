package com.conveyal.datatools.manager.models;

import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.loader.TableReader;
import com.conveyal.gtfs.model.Agency;
import com.conveyal.gtfs.model.Stop;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Iterators;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.io.Serializable;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by landon on 5/10/16.
 */
public class FeedValidationResult implements Serializable {
    private static final long serialVersionUID = 1L;
    @JsonProperty
    public LoadStatus loadStatus;
    public String loadFailureReason;
    public Collection<String> agencies;
    public int agencyCount;
    public int routeCount;
    public int tripCount;
    public int stopTimesCount;
    public int errorCount;
    public LocalDate startDate;
    public LocalDate endDate;
    public Rectangle2D bounds;
    public long avgDailyRevenueTime;

    // legacy fields included for backwards compatibility (not currently used)
    public String feedFileName;
//    public ValidationResult routes;
//    public ValidationResult stops;
//    public ValidationResult trips;
//    public ValidationResult shapes;

    // constructor for data dump load
    public FeedValidationResult() {}

    // constructor for bad feed load
    public FeedValidationResult(LoadStatus loadStatus, String loadFailureReason) {
        this.loadStatus = loadStatus;
        this.loadFailureReason = loadFailureReason;
    }

    // TODO: construct FeedValidationResult from sql-loader Feed (or FeedInfo)

    public FeedValidationResult(Feed feed) {
        ArrayList<String> agencyIds = new ArrayList<>();
        TableReader<Agency> agencyReader = feed.agencies;
        agencyReader.forEach(agency -> agencyIds.add(agency.agency_id));
        this.agencies = agencyIds;
        this.agencyCount = calcaluateCount(agencyReader);
        this.routeCount = calcaluateCount(feed.routes);

        // FIXME: add back in.
        this.bounds = calculateBounds(feed.stops);
//        LocalDate calDateStart = stats.getCalendarDateStart();
//        LocalDate calSvcStart = stats.getCalendarServiceRangeStart();
//
//        LocalDate calDateEnd = stats.getCalendarDateEnd();
//        LocalDate calSvcEnd = stats.getCalendarServiceRangeEnd();
//
//        if (calDateStart == null && calSvcStart == null)
//            // no service . . . this is bad
//            this.startDate = null;
//        else if (calDateStart == null)
//            this.startDate = calSvcStart;
//        else if (calSvcStart == null)
//            this.startDate = calDateStart;
//        else
//            this.startDate = calDateStart.isBefore(calSvcStart) ? calDateStart : calSvcStart;
//
//        if (calDateEnd == null && calSvcEnd == null)
//            // no service . . . this is bad
//            this.endDate = null;
//        else if (calDateEnd == null)
//            this.endDate = calSvcEnd;
//        else if (calSvcEnd == null)
//            this.endDate = calDateEnd;
//        else
//            this.endDate = calDateEnd.isAfter(calSvcEnd) ? calDateEnd : calSvcEnd;

        // FIXME add back in.
//        try {
//            // retrieve revenue time in seconds for Tuesdays in feed
//            this.avgDailyRevenueTime = stats.getAverageDailyRevenueTime(2);
//        } catch (Exception e) {
//            // temporarily catch errors in calculating this stat
//            this.avgDailyRevenueTime = -1L;
//        }

        this.loadStatus = LoadStatus.SUCCESS;
        this.tripCount = calcaluateCount(feed.trips);
        this.stopTimesCount = calcaluateCount(feed.stopTimes);
        // FIXME this is not the right way to grab errors
        this.errorCount = feed.errors.size();
    }

    private int calcaluateCount(TableReader tableReader) {
        int count = Iterators.size(tableReader.iterator());
        return count;
    }

    private Rectangle2D calculateBounds (TableReader<Stop> stops) {
        Rectangle2D bounds = null;
        for (Stop stop : stops) {
            // FIXME add back in
//            // skip over stops that don't have any stop times
//            if (!feed.stopCountByStopTime.containsKey(stop.stop_id)) {
//                continue;
//            }
            if (bounds == null) {
                bounds = new Rectangle2D.Double(stop.stop_lon, stop.stop_lat, 0, 0);
            }
            else {
                bounds.add(new Point2D.Double(stop.stop_lon, stop.stop_lat));
            }
        }
        return bounds;
    }
}