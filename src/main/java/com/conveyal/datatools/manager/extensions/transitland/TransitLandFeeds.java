package com.conveyal.datatools.manager.extensions.transitland;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by demory on 3/31/16.
 */

public class TransitLandFeeds {
    @JsonProperty
    List<TransitLandFeed> feeds = new ArrayList<TransitLandFeed>();

//        @JsonProperty
//        Map

//        @JsonProperty
//        List<TransitLandFeed> feeds;

    @JsonProperty
    Map<String, String> meta;
}