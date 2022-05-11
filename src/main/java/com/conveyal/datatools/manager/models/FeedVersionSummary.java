package com.conveyal.datatools.manager.models;

import com.conveyal.gtfs.validator.ValidationResult;

import java.io.Serializable;
import java.util.Date;

/**
 * Includes summary data (a subset of fields) for a feed version.
 */
public class FeedVersionSummary extends Model implements Serializable {
    private static final long serialVersionUID = 1L;

    public FeedRetrievalMethod retrievalMethod;
    public int version;
    public String feedSourceId;
    public String name;
    public String namespace;
    public String originNamespace;
    public Long fileSize;
    public Date updated;
    public ValidationResult validationResult;

    /** Empty constructor for serialization */
    public FeedVersionSummary() {
        // Do nothing
    }
}
