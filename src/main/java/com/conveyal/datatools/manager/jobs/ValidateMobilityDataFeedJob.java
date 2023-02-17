package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.FeedVersionJob;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This job handles the MobilityData validation of a given feed version. If the version is not new, it will simply
 * replace the existing version with the version object that has updated validation info.
 */
public class ValidateMobilityDataFeedJob extends FeedVersionJob {
    public static final Logger LOG = LoggerFactory.getLogger(ValidateMobilityDataFeedJob.class);

    private FeedVersion feedVersion;

    public ValidateMobilityDataFeedJob(FeedVersion version, Auth0UserProfile owner) {
        super(owner, "Validating Feed using MobilityData", JobType.VALIDATE_FEED);
        feedVersion = version;
        status.update("Waiting to begin MobilityData validation...", 0);
    }

    @Override
    public void jobLogic () {
        LOG.info("Running ValidateMobilityDataFeedJob for {}", feedVersion.id);
        feedVersion.validateMobility(status);
    }

    @Override
    public void jobFinished () {
        status.completeSuccessfully("MobilityData validation finished!");
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
