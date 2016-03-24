package com.conveyal.datatools.manager.models;

import java.io.Serializable;
import java.util.Date;

/**
 * A summary of a FeedVersion, leaving out all of the individual validation errors.
 */
public class SummarizedFeedVersion implements Serializable {
    public FeedValidationResultSummary validationResult;
    public FeedSource feedSource;
    public String id;
    public Date updated;
    public String previousVersionId;
    public String nextVersionId;
    public int version;

    public SummarizedFeedVersion (FeedVersion version) {
        this.validationResult = new FeedValidationResultSummary(version.validationResult);
        this.feedSource = version.getFeedSource();
        this.updated = version.updated;
        this.id = version.id;
        this.nextVersionId = version.getNextVersionId();
        this.previousVersionId = version.getPreviousVersionId();
        this.version = version.version;
    }
}