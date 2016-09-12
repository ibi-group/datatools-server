package com.conveyal.datatools.editor.models.transit;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/** A stop on a trip pattern. This is not a model, as it is stored in a list within trippattern */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TripPatternStop implements Cloneable, Serializable {
    public static final long serialVersionUID = 1;

    public String stopId;

    public int defaultTravelTime;
    public int defaultDwellTime;

    /**
     * Is this stop a timepoint?
     *
     * If null, no timepoint information will be exported for this stop.
     */
    public Boolean timepoint;

    public Double shapeDistTraveled;

    public TripPatternStop()
    {

    }

    public TripPatternStop(Stop stop, Integer defaultTravelTime)
    {
        this.stopId = stop.id;
        this.defaultTravelTime = defaultTravelTime;
    }

    public TripPatternStop clone () throws CloneNotSupportedException {
        return (TripPatternStop) super.clone();
    }
}


