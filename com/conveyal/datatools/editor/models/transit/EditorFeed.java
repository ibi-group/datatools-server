package com.conveyal.datatools.editor.models.transit;

import com.conveyal.datatools.editor.datastore.FeedTx;
import com.conveyal.datatools.editor.datastore.GlobalTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.Model;
import com.conveyal.datatools.manager.models.JsonViews;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;

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

//    @JsonProperty
//    public Integer getRouteCount() {
//        FeedTx tx = VersionedDataStore.getFeedTx(id);
//        return tx.routes.size();
//    }
//
//    @JsonProperty
//    public Integer getStopCount() {
//        FeedTx tx = VersionedDataStore.getFeedTx(id);
//        return tx.stops.size();
//    }
//    @JsonInclude(JsonInclude.Include.NON_NULL)
//    @JsonView(JsonViews.UserInterface.class)
//    public boolean getEditedSinceSnapshot() {
//        FeedTx tx = VersionedDataStore.getFeedTx(id);
////        return tx.editedSinceSnapshot.get();
//        return false;
//    }
    // the associated FeedSource in the data manager DB
    //public String feedSourceId;


    public EditorFeed() {}

    public EditorFeed clone () throws CloneNotSupportedException {
        return (EditorFeed) super.clone();
    }

}
