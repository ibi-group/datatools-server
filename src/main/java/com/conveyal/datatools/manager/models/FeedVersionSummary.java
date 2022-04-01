package com.conveyal.datatools.manager.models;

import java.io.Serializable;
import java.util.Date;

/**
 * Includes summary data (a subset of fields) for a feed version.
 */
public class FeedVersionSummary implements Serializable {
    private static final long serialVersionUID = 1L;

    public String id;
    public Date lastUpdated;
    public Date dateCreated;
    public FeedRetrievalMethod retrievalMethod;
    public int version;
    public String name;
    public String namespace;
    public Long fileSize;

    /** Empty constructor for serialization */
    public FeedVersionSummary() {
        // Do nothing
    }

    public FeedVersionSummary(FeedVersion feedVersion) {
        id = feedVersion.id;
        dateCreated = feedVersion.dateCreated;
        lastUpdated = feedVersion.lastUpdated;
        retrievalMethod = feedVersion.retrievalMethod;
        version = feedVersion.version;
        name = feedVersion.name;
        namespace = feedVersion.namespace;
        fileSize = feedVersion.fileSize;
    }
}
