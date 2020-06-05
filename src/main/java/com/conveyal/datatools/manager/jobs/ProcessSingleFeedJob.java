package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.editor.jobs.CreateSnapshotJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.transform.DbTransformation;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Snapshot;
import com.conveyal.datatools.manager.models.transform.FeedTransformRules;
import com.conveyal.datatools.manager.models.transform.ZipTransformation;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static com.conveyal.datatools.manager.models.FeedRetrievalMethod.VERSION_CLONE;

/**
 * Process/validate a single GTFS feed. This job is called once a GTFS file had been uploaded or fetched and is ready to
 * be loaded into the GTFS database. This job acts as a parent job that chains together multiple server jobs. Loading
 * the feed and validating the feed are a part of this chain unconditionally. However, depending on which modules are
 * enabled, other jobs may be included here if desired.
 * @author mattwigway
 *
 */
public class ProcessSingleFeedJob extends MonitorableJob {
    private final FeedVersion feedVersion;
    private final boolean isNewVersion;
    private static final Logger LOG = LoggerFactory.getLogger(ProcessSingleFeedJob.class);
    private final boolean shouldClone;
    private final FeedSource feedSource;

    /**
     * Create a job for the given feed version.
     */
    public ProcessSingleFeedJob (FeedVersion feedVersion, Auth0UserProfile owner, boolean isNewVersion) {
        super(owner, "Processing GTFS for " + (feedVersion.parentFeedSource() != null ? feedVersion.parentFeedSource().name : "unknown feed source"), JobType.PROCESS_FEED);
        this.feedVersion = feedVersion;
        this.feedSource = feedVersion.parentFeedSource();
        this.isNewVersion = isNewVersion;
        // If there are any feed transformations that apply to the VERSION_CLONE retrieval method, we need to add a
        // stage to jobFinished (perhaps) to clone the feed version (as long as this feed version has not already been
        // cloned). Once that feed version is processed, the appropriate transformations will apply themselves.
        shouldClone = !feedVersion.retrievalMethod.equals(VERSION_CLONE) &&
            feedSource.hasRulesForRetrievalMethod(VERSION_CLONE);
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
        return feedSource.id;
    }

    @Override
    public void jobLogic () {
        LOG.info("Processing feed for {}", feedVersion.id);
        FeedTransformRules rules = feedSource.getRulesForRetrievalMethod(feedVersion.retrievalMethod);
        boolean shouldTransform = rules != null;
        if (shouldTransform) {
            // Run zip transformations before load to handle any operations that must be applied directly to the zip file.
            List<ZipTransformation> zipTransformations = rules.getActiveTransformations(feedVersion, ZipTransformation.class);
            for (ZipTransformation transformation : zipTransformations) {
                ArbitraryTransformJob zipTransform = new ArbitraryTransformJob(owner, feedVersion.retrieveGtfsFile(), transformation);
                // Run transform job in line so we can monitor the error status before load/validate begins.
                zipTransform.run();
                // Short circuit the feed load/validate if a pre-load transform fails.
                if (zipTransform.status.error) return;
            }
        }

        // First, load the feed into database. During this stage, the GTFS file will be uploaded to S3 (and deleted locally).
        addNextJob(new LoadFeedJob(feedVersion, owner, isNewVersion));

        // Next, validate the feed.
        addNextJob(new ValidateFeedJob(feedVersion, owner, isNewVersion));

        // We only need to snapshot the feed if there are transformations at the database level. In the case that there
        // are, the snapshot namespace will be the target of these modifications. If we were to apply the modifications
        // to the original feed version's namespace, the namespace and the zip file would become out of sync, which
        // violates a core expectation for this application: that each zip file represents exactly its database
        // representation.
        boolean shouldSnapshot = shouldTransform && rules.hasTransformationsOfType(feedVersion, DbTransformation.class);
        if (shouldSnapshot) {
            Snapshot snapshot = new Snapshot("Transform of " + feedVersion.name, feedVersion);
            addNextJob(new CreateSnapshotJob(owner, snapshot));
            // Apply post-load transformations to snapshotted feed version. Post-load transformations will modify only the
            // snapshot (not the original feed version's namespace), so the snapshot must be published (or loaded into the
            // editor) in order to see the results.
            // FIXME need to add snapshot namespace...
            for (DbTransformation transformation : rules.getActiveTransformations(feedVersion, DbTransformation.class)) {
                addNextJob(new ArbitraryTransformJob(owner, snapshot.id, transformation));
            }
            // If the user has selected to create a new version from the resulting snapshot, do so here.
            if (rules.createNewVersion) {
                addNextJob(new CreateFeedVersionFromSnapshotJob(feedSource, snapshot, owner));
            }
        }

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
            // OK, let's map out the different transformation possibilities:
            // 1. We only have zip transformations. These can either apply directly to the imported zip (dangerous)
            // or they can apply to a cloned version of the import. In that case, the retrieval method should be set to
            // VERSION_CLONE (otherwise, can be set to any other combination of retrieval methods).
            // 2. We only have db transformations. These will always apply to a cloned snapshot of a feed version to
            // avoid the primary db namespace having different contents from the zip file. Possibilities here are that
            // the snapshot can end up hanging (i.e., not published) or once all of the transformations have been
            // applied, we can create a new feed version from the snapshot (retrieval method being produced in house??).
            // 3. Let's say we have zip transformations and db transformations.
            // 3a: Zip transformations apply to imported version, db transformations apply to snapshot or get published (no big deal)
            // 3b: Zip transformations apply to cloned version, db transformations apply to snapshot or get published.
            //  ---- This case is a little trickier, because we could end up with two cloned_versions. Let's say we want
            //  a SQL query to run on the original data, create a new GTFS file, and then replace a GTFS+ file from some
            //  other version. FTR1 = clone
            if (shouldClone) {
                try {
                    // Create a new version for the clone.
                    FeedVersion newFeedVersion = new FeedVersion(feedSource, VERSION_CLONE);
                    // Get new path for GTFS file.
                    File newGtfsFile = FeedVersion.feedStore.getFeedFile(newFeedVersion.id);
                    // Copy previous version to the new GTFS path.
                    Files.copy(feedVersion.retrieveGtfsFile().toPath(), newGtfsFile.toPath());
                    // Handle hashing file.
                    newFeedVersion.assignGtfsFileAttributes(newGtfsFile);
                    // Kick off job in new thread. Transformations that apply to clone will be picked up in the next
                    // processing stage. FIXME should this happen in the same thread?
                    ProcessSingleFeedJob processSingleFeedJob = new ProcessSingleFeedJob(newFeedVersion, owner, true);
                    DataManager.heavyExecutor.execute(processSingleFeedJob);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        } else {
            // Processing did not complete. Depending on which sub-task this occurred in,
            // there may or may not have been a successful load/validation of the feed.
            String errorReason = status.exceptionType != null ? String.format("error due to %s", status.exceptionType) : "unknown error";
            LOG.warn("Error processing version {} because of {}.", feedVersion.id, errorReason);
        }
    }

}
