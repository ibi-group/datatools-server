package com.conveyal.datatools.editor.models.transit;

import com.conveyal.datatools.editor.models.Model;

import java.io.Serializable;
import java.net.URL;
import java.time.LocalDate;

/**
 * Created by demory on 6/8/16.
 */
public class EditorFeed extends Model implements Cloneable, Serializable {
    private static final long serialVersionUID = 1L;

    // GTFS Editor defaults
    public String color;
    public Double defaultLat;
    public Double defaultLon;
    public GtfsRouteType defaultRouteType;

    // feed-info.txt fields
    public String feedPublisherName;
    public URL feedPublisherUrl;
    public String feedLang;
    public String feedVersion;
    public LocalDate feedStartDate;
    public LocalDate feedEndDate;

//    public transient int numberOfRoutes, numberOfStops;
//    @JsonProperty("numberOfRoutes")
//    public int jsonGetNumberOfRoutes() { return numberOfRoutes; }
//
//    @JsonProperty("numberOfStops")
//    public int jsonGetNumberOfStops() { return numberOfStops; }
//
//    // Add information about the days of week this route is active
//    public void addDerivedInfo(final FeedTx tx) {
//        numberOfRoutes = tx.routes.size();
//        numberOfStops = tx.stops.size();
//    }

    public EditorFeed() {}

    public EditorFeed(String id) {
        this.id = id;
    }

    public EditorFeed clone () throws CloneNotSupportedException {
        return (EditorFeed) super.clone();
    }

}
