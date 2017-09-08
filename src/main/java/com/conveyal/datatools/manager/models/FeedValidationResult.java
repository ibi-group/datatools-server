package com.conveyal.datatools.manager.models;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.stats.FeedStats;
import com.conveyal.gtfs.validator.json.LoadStatus;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.awt.geom.Rectangle2D;
import java.io.Serializable;
import java.time.LocalDate;
import java.util.Collection;
import java.util.stream.Collectors;

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

    public FeedValidationResult(GTFSFeed feed, FeedStats stats) {
        this.agencies = stats.getAllAgencies().stream().map(agency -> agency.agency_id).collect(Collectors.toList());
        this.agencyCount = stats.getAgencyCount();
        this.routeCount = stats.getRouteCount();
        this.bounds = stats.getBounds();
        LocalDate calDateStart = stats.getCalendarDateStart();
        LocalDate calSvcStart = stats.getCalendarServiceRangeStart();

        LocalDate calDateEnd = stats.getCalendarDateEnd();
        LocalDate calSvcEnd = stats.getCalendarServiceRangeEnd();

        if (calDateStart == null && calSvcStart == null)
            // no service . . . this is bad
            this.startDate = null;
        else if (calDateStart == null)
            this.startDate = calSvcStart;
        else if (calSvcStart == null)
            this.startDate = calDateStart;
        else
            this.startDate = calDateStart.isBefore(calSvcStart) ? calDateStart : calSvcStart;

        if (calDateEnd == null && calSvcEnd == null)
            // no service . . . this is bad
            this.endDate = null;
        else if (calDateEnd == null)
            this.endDate = calSvcEnd;
        else if (calSvcEnd == null)
            this.endDate = calDateEnd;
        else
            this.endDate = calDateEnd.isAfter(calSvcEnd) ? calDateEnd : calSvcEnd;

        try {
            // retrieve revenue time in seconds for Tuesdays in feed
            this.avgDailyRevenueTime = stats.getAverageDailyRevenueTime(2);
        } catch (Exception e) {
            // temporarily catch errors in calculating this stat
            this.avgDailyRevenueTime = -1L;
        }

        this.loadStatus = LoadStatus.SUCCESS;
        this.tripCount = stats.getTripCount();
        this.stopTimesCount = stats.getStopTimesCount();
        this.errorCount = feed.errors.size();
    }

    /** Why a GTFS feed failed to load */
//    public enum LoadStatus {
//        SUCCESS, INVALID_ZIP_FILE, OTHER_FAILURE, MISSING_REQUIRED_FIELD, INCORRECT_FIELD_COUNT_IMPROPER_QUOTING;
//    }
}