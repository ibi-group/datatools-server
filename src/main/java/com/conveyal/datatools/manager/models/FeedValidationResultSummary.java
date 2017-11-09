package com.conveyal.datatools.manager.models;

import java.awt.geom.Rectangle2D;
import java.io.Serializable;
import java.time.LocalDate;
import java.util.Collection;

import com.conveyal.gtfs.loader.FeedLoadResult;
import com.conveyal.gtfs.validator.ValidationResult;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

/**
 * Represents a subset of a feed validation result, just enough for display, without overwhelming the browser
 * or sending unnecessary amounts of data over the wire
 */
public class FeedValidationResultSummary implements Serializable {
    private static final long serialVersionUID = 1L;

    public LoadStatus loadStatus;

    @JsonInclude(Include.ALWAYS)
    public String loadFailureReason;
    public Collection<String> agencies;

    public int errorCount;

    // statistics
    public int agencyCount;
    public int routeCount;
    public int tripCount;
    public int stopTimesCount;
    public long avgDailyRevenueTime;

    /** The first date the feed has service, either in calendar.txt or calendar_dates.txt */
    @JsonInclude(Include.ALWAYS)
    public LocalDate startDate;

    /** The last date the feed has service, either in calendar.txt or calendar_dates.txt */
    @JsonInclude(Include.ALWAYS)
    public LocalDate endDate;

    @JsonInclude(Include.ALWAYS)
    public Bounds bounds;

    /**
     * Construct a summarized version of the given FeedValidationResult.
     * @param validationResult
     */
    public FeedValidationResultSummary (ValidationResult validationResult, FeedLoadResult feedLoadResult) {
        if (validationResult != null) {
            this.loadStatus = validationResult.fatalException == null
                    ? LoadStatus.SUCCESS
                    : LoadStatus.OTHER_FAILURE;
            this.loadFailureReason = validationResult.fatalException;
            if (loadStatus == LoadStatus.SUCCESS) {
                this.errorCount = validationResult.errorCount;
                this.agencyCount = feedLoadResult.agency.rowCount;
                this.routeCount = feedLoadResult.routes.rowCount;
                this.tripCount = feedLoadResult.trips.rowCount;
                this.stopTimesCount = feedLoadResult.stopTimes.rowCount;
                this.startDate = validationResult.firstCalendarDate;
                this.endDate = validationResult.lastCalendarDate;
                this.bounds = boundsFromValidationResult(validationResult);
                // FIXME: compute avg revenue time
//                this.avgDailyRevenueTime = validationResult.avgDailyRevenueTime;
            }
        }

    }

    private static Bounds boundsFromValidationResult (ValidationResult result) {
        Bounds bounds = new Bounds();
        bounds.north = result.fullBounds.maxLat;
        bounds.south = result.fullBounds.minLat;
        bounds.east = result.fullBounds.maxLon;
        bounds.west = result.fullBounds.minLon;
        return bounds;
    }
}