package com.conveyal.datatools.manager.models;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.Collection;

import com.conveyal.datatools.editor.utils.JacksonSerializers;
import com.conveyal.gtfs.loader.FeedLoadResult;
import com.conveyal.gtfs.validator.ValidationResult;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Represents a subset of a feed validation result, just enough for display, without overwhelming the browser
 * or sending unnecessary amounts of data over the wire
 */
public class FeedValidationResultSummary implements Serializable {
    private static final long serialVersionUID = 1L;
    // Include feed ID and namespace here so the client can trace back to the full feed version if this is nested under
    // a feed source.
    public String feedVersionId;
    public String namespace;
    public LoadStatus loadStatus;

    @JsonInclude(Include.ALWAYS)
    public String loadFailureReason;
    public Collection<String> agencies;

    public int errorCount;

    // statistics
    public int agencyCount;
    public int routeCount;
    public int stopCount;
    public int stopTimesCount;
    public int tripCount;
    public long avgDailyRevenueTime;

    /** The first date the feed has service, either in calendar.txt or calendar_dates.txt */
    @JsonInclude(Include.ALWAYS)
    @JsonSerialize(using = JacksonSerializers.LocalDateIsoSerializer.class)
    @JsonDeserialize(using = JacksonSerializers.LocalDateIsoDeserializer.class)
    public LocalDate startDate;

    /** The last date the feed has service, either in calendar.txt or calendar_dates.txt */
    @JsonInclude(Include.ALWAYS)
    @JsonSerialize(using = JacksonSerializers.LocalDateIsoSerializer.class)
    @JsonDeserialize(using = JacksonSerializers.LocalDateIsoDeserializer.class)
    public LocalDate endDate;

    @JsonInclude(Include.ALWAYS)
    public Bounds bounds;

    /** No-arg constructor for de-/serialization. */
    public FeedValidationResultSummary () {}

    /**
     * Construct a summarized version of the given FeedValidationResult.
     */
    public FeedValidationResultSummary (FeedVersion version) {
        this.feedVersionId = version.id;
        this.namespace = version.namespace;
        // If feed load failed (and is null), construct an empty result to avoid NPEs.
        if (version.feedLoadResult == null) {
            version.feedLoadResult = new FeedLoadResult(true);
        }
        if (version.validationResult != null) {
            this.loadStatus = version.validationResult.fatalException == null
                    ? LoadStatus.SUCCESS
                    : LoadStatus.OTHER_FAILURE;
            this.loadFailureReason = version.validationResult.fatalException;
            if (loadStatus == LoadStatus.SUCCESS) {
                this.errorCount = version.validationResult.errorCount;
                this.agencyCount = version.feedLoadResult.agency.rowCount;
                this.routeCount = version.feedLoadResult.routes.rowCount;
                this.stopCount = version.feedLoadResult.stops.rowCount;
                this.tripCount = version.feedLoadResult.trips.rowCount;
                this.stopTimesCount = version.feedLoadResult.stopTimes.rowCount;
                this.startDate = version.validationResult.firstCalendarDate;
                this.endDate = version.validationResult.lastCalendarDate;
                this.bounds = boundsFromValidationResult(version.validationResult);
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