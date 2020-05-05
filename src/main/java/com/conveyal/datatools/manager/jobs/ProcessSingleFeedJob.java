package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.editor.jobs.CreateSnapshotJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedTransformation;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.models.Snapshot;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Process/validate a single GTFS feed. This job is called once a GTFS file had been uploaded or fetched and is ready to
 * be loaded into the GTFS database. This job acts as a parent job that chains together multiple server jobs. Loading
 * the feed and validating the feed are a part of this chain unconditionally. However, depending on which modules are
 * enabled, other jobs may be included here if desired.
 * @author mattwigway
 *
 */
public class ProcessSingleFeedJob extends MonitorableJob {
    private FeedVersion feedVersion;
    private final boolean isNewVersion;
    private static final Logger LOG = LoggerFactory.getLogger(ProcessSingleFeedJob.class);

    /**
     * Create a job for the given feed version.
     */
    public ProcessSingleFeedJob (FeedVersion feedVersion, Auth0UserProfile owner, boolean isNewVersion) {
        super(owner, "Processing GTFS for " + (feedVersion.parentFeedSource() != null ? feedVersion.parentFeedSource().name : "unknown feed source"), JobType.PROCESS_FEED);
        this.feedVersion = feedVersion;
        this.isNewVersion = isNewVersion;
        status.update("Waiting...", 0);
        status.uploading = true;
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

    @Override
    public void jobLogic () {
        LOG.info("Processing feed for {}", feedVersion.id);
        FeedSource feedSource = feedVersion.parentFeedSource();

        // Collect any transformations that should happen pre- or post-load.
        List<MonitorableJob> postValidationTransforms = new ArrayList<>();
        // Apply any transformations to target feed version that need to be applied pre-load.
        for (FeedTransformation transformation : feedSource.transformations) {
            ArbitraryFeedTransformJob transformJob = new ArbitraryFeedTransformJob(owner, feedVersion, transformation);
            if (transformation.isAppliedBeforeLoad()) {
                // Run transform job in line so we can monitor the error status before load/validate begins.
                transformJob.run();
                // Short circuit the feed load/validate if a pre-load transform fails.
                if (transformJob.status.error) {
                    return;
                }
            } else {
                postValidationTransforms.add(transformJob);
            }
        }

        // First, load the feed into database. During this stage, the GTFS file will be uploaded to S3 (and deleted locally).
        addNextJob(new LoadFeedJob(feedVersion, owner, isNewVersion));

        // Next, validate the feed.
        addNextJob(new ValidateFeedJob(feedVersion, owner, isNewVersion));

        // Next, handle any transformations that need to be applied following processing.
        // TODO: If not transforming in place, do the following:
        //  1. snapshot feed version,
        //  2. apply transformations,
        //  3. create feed version from snapshot
        if (!feedSource.transformInPlace) {
            Snapshot snapshot = new Snapshot("Transform of " + feedVersion.name, feedVersion.namespace);
            addNextJob(
                new CreateSnapshotJob(
                    owner,
                    snapshot,
                    false,
                    false,
                    false
                )
            );
        }

        // Apply post-load transformations to target feed version.
        addNextJob(postValidationTransforms);

        // FIXME: Should we overwrite the input GTFS dataset if transforming in place?

        // TODO: Any other activities that need to be run (e.g., module-specific activities).
        if (DataManager.isModuleEnabled("deployment")) {
//            Project project = feedSource.retrieveProject();
//            if (project.autoDeployId)
            // TODO: Get deployment, update feed version for feed source, and kick off deployment to server that
            //  deployment is currently pointed at.
        }
    }

    @Override
    public void jobFinished () {
        if (!status.error) {
            // Note: storing a new feed version in database is handled at completion of the ValidateFeedJob subtask.
            status.completeSuccessfully("New version saved.");
        } else {
            // Processing did not complete. Depending on which sub-task this occurred in,
            // there may or may not have been a successful load/validation of the feed.
            String errorReason = status.exceptionType != null ? String.format("error due to %s", status.exceptionType) : "unknown error";
            LOG.warn("Error processing version {} because of {}.", feedVersion.id, errorReason);
        }
    }

}
