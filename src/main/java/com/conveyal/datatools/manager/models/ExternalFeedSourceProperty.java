package com.conveyal.datatools.manager.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


/**
 * Created by demory on 3/30/16.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExternalFeedSourceProperty extends Model {
    private static final long serialVersionUID = 1L;

    // constructor for data dump load
    public ExternalFeedSourceProperty() {}

    public ExternalFeedSourceProperty(String feedSourceId, String resourceType, String name, String value) {
        this.feedSourceId = feedSourceId;
        this.resourceType = resourceType;
        this.name = name;
        this.value = value;
    }

    public ExternalFeedSourceProperty(FeedSource feedSource, String resourceType, String name, String value) {
        this(feedSource.id, resourceType, name, value);
        this.id = constructId(feedSource, resourceType, name);
    }

    public static String constructId(FeedSource feedSource, String resourceType, String name) {
        return feedSource.id + "_" + resourceType + "_" + name;
    }

    public String resourceType;

    public String feedSourceId;

    public String name;

    public String value;
}
