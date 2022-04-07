package com.conveyal.datatools.manager.models;

import java.io.Serializable;
import java.util.Date;

/**
 * Includes summary data (a subset of fields) for a feed version.
 */
public class FeedVersionSummary extends Model implements Serializable {
    private static final long serialVersionUID = 1L;

    public FeedRetrievalMethod retrievalMethod;
    public int version;
    public String name;
    public String namespace;
    public String originNamespace;
    public Long fileSize;
    public Date updated;

    /** Empty constructor for serialization */
    public FeedVersionSummary() {
        // Do nothing
    }
}
