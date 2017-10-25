package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by demory on 6/16/16.
 */
public class ValidateFeedJob extends MonitorableJob {
    public static final Logger LOG = LoggerFactory.getLogger(ValidateFeedJob.class);

    private FeedVersion feedVersion;

    public ValidateFeedJob(FeedVersion version, String owner) {
        super(owner, "Validating Feed", JobType.VALIDATE_FEED);
        feedVersion = version;
        status.update(false, "Waiting to begin validation...", 0);
    }

    @Override
    public void jobLogic () {
        LOG.info("Running ValidateFeedJob for {}", feedVersion.id);
        feedVersion.validate(status);
    }

    @Override
    public void jobFinished () {
        if (!status.error) {
            if (parentJobId != null && JobType.PROCESS_FEED.equals(parentJobType)) {
                // Validate stage is happening as part of an overall process feed job.
                // At this point all GTFS data has been loaded and validated, so we record
                // the FeedVersion into mongo.
                // This happens here because otherwise we would have to wait for other jobs,
                // such as BuildTransportNetwork, to finish. If those subsequent jobs fail,
                // the version won't get loaded into the database (even though it exists in postgres).
                Persistence.feedVersions.create(feedVersion);
            }
            // TODO: If ValidateFeedJob is called without a parent job (e.g., to "re-validate" a feed), we should handle
            // storing the updated ValidationResult in Mongo.

            status.update(false, "Validation finished!", 100, true);
        }
    }

    /**
     * Getter that allows a client to know the ID of the feed version that will be created as soon as the upload is
     * initiated; however, we will not store the FeedVersion in the mongo application database until the upload and
     * processing is completed. This prevents clients from manipulating GTFS data before it is entirely imported.
     */
    @JsonProperty
    public String getFeedVersionId () {
        return feedVersion.id;
    }

    @JsonProperty
    public String getFeedSourceId () {
        return feedVersion.parentFeedSource().id;
    }

}
