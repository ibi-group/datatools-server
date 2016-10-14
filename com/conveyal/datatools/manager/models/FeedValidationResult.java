package com.conveyal.datatools.manager.models;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.model.ValidationResult;
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
    @JsonProperty
    public LoadStatus loadStatus;
    public String loadFailureReason;
    public String feedFileName;
    public Collection<String> agencies;
    public ValidationResult routes;
    public ValidationResult stops;
    public ValidationResult trips;
    public ValidationResult shapes;
    public int agencyCount;
    public int routeCount;
    public int tripCount;
    public int stopTimesCount;
    public int errorCount;
    public LocalDate startDate;
    public LocalDate endDate;
    public Rectangle2D bounds;

    public FeedValidationResult() {
//        this.agencies = stats.getAllAgencies().stream().map(agency -> agency.agency_id).collect(Collectors.toList());
//        this.agencyCount = stats.getAgencyCount();
//        this.routeCount = stats.getRouteCount();
//        this.bounds = stats.getBounds();
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
//        this.loadStatus = LoadStatus.SUCCESS;
//        this.tripCount = stats.getTripCount();
//        this.stopTimesCount = stats.getStopTimesCount();
//        this.errorCount = gtfsFeed.errors.size();
    }
}