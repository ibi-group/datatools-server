package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.FeedVersionJob;
import com.conveyal.datatools.common.utils.Scheduler;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.validator.ValidationResult;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This job handles the validation of a given feed version. If the version is not new, it will simply replace the
 * existing version with the version object that has updated validation info.
 */
public class ValidateFeedJob extends FeedVersionJob {
    public static final Logger LOG = LoggerFactory.getLogger(ValidateFeedJob.class);

    private FeedVersion feedVersion;
    private final boolean isNewVersion;

    public ValidateFeedJob(FeedVersion version, Auth0UserProfile owner, boolean isNewVersion) {
        super(owner, "Validating Feed", JobType.VALIDATE_FEED);
        feedVersion = version;
        this.isNewVersion = isNewVersion;
        status.update("Waiting to begin validation...", 0);
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
                // the version won't get loaded into MongoDB (even though it exists in postgres).
                feedVersion.storeUser(owner);
                feedVersion.persistFeedVersionAfterValidation(isNewVersion);
                // Schedule expiration notification jobs.
                Scheduler.scheduleExpirationNotifications(feedVersion.parentFeedSource());
            }
            // TODO: If ValidateFeedJob is called without a parent job (e.g., to "re-validate" a feed), we should handle
            //  storing the updated ValidationResult in Mongo.
            status.completeSuccessfully("Validation finished!");
        } else {
            // If the version was not stored successfully, call FeedVersion#delete to reset things to before the version
            // was uploaded/fetched. Note: delete calls made to MongoDB on the version ID will not succeed, but that is
            // expected.
            feedVersion.delete();
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

    /**
     * Getter that returns the validationResult so that once the job finishes, the client can optionally provide
     * directions to users based on the success of the validation or other validation data (e.g., "The feed you have
     * loaded is only valid for future dates.").
     */
    @JsonProperty
    public ValidationResult getValidationResult () {
        return feedVersion.validationResult;
    }

}
