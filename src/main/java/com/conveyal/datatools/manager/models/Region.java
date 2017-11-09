package com.conveyal.datatools.manager.models;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Created by landon on 4/15/16.
 */
public class Region extends Model {
    private static final long serialVersionUID = 1L;

    /** The name of this region, e.g. Atlanta. */
    public String name;

    // Polygon geometry of region as GeoJSON string
    @JsonIgnore
    public String geometry;
    public Double lat, lon;
    // hierarchical order of region: country, 1st order admin, or region
    public String order;

    public Boolean isPublic;
    public String defaultLanguage;
    public String defaultTimeZone;

    //@JsonView
    public Collection<FeedSource> feedSources;

    public Double north, south, east, west;

    public Region() {

    }

}
