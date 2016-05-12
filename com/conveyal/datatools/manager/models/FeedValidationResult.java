package com.conveyal.datatools.manager.models;

import com.conveyal.gtfs.model.ValidationResult;
import com.conveyal.gtfs.validator.json.LoadStatus;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.awt.geom.Rectangle2D;
import java.io.Serializable;
import java.time.LocalDate;
import java.util.Collection;

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
    }
}