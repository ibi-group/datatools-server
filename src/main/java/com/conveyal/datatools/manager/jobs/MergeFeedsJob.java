package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.FeedSourceJob;
import com.conveyal.datatools.common.utils.aws.CheckedAWSException;
import com.conveyal.datatools.common.utils.aws.S3Utils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.gtfsplus.tables.GtfsPlusTable;
import com.conveyal.datatools.manager.jobs.feedmerge.FeedMergeContext;
import com.conveyal.datatools.manager.jobs.feedmerge.FeedToMerge;
import com.conveyal.datatools.manager.jobs.feedmerge.MergeFeedsResult;
import com.conveyal.datatools.manager.jobs.feedmerge.MergeFeedsType;
import com.conveyal.datatools.manager.jobs.feedmerge.MergeLineContext;
import com.conveyal.datatools.manager.jobs.feedmerge.MergeStrategy;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.ErrorUtils;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.StopTime;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipOutputStream;

import static com.conveyal.datatools.manager.jobs.feedmerge.MergeFeedsType.SERVICE_PERIOD;
import static com.conveyal.datatools.manager.jobs.feedmerge.MergeFeedsType.REGIONAL;
import static com.conveyal.datatools.manager.jobs.feedmerge.MergeStrategy.CHECK_STOP_TIMES;
import static com.conveyal.datatools.manager.jobs.feedmerge.MergeStrategy.EXTEND_FUTURE;
import static com.conveyal.datatools.manager.models.FeedRetrievalMethod.REGIONAL_MERGE;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.*;

/**
 * This job handles merging two or more feed versions according to logic specific to the specified merge type.
 * The merge types handled here are:
 * - {@link MergeFeedsType#REGIONAL}: this is essentially a "dumb" merge. For each feed version, each primary key is
 * scoped so that there is no possibility that it will conflict with other IDs
 * found in any other feed version. Note: There is absolutely no attempt to merge
 * entities based on either expected shared IDs or entity location (e.g., stop
 * coordinates).
 * - {@link MergeFeedsType#SERVICE_PERIOD}:
 * this strategy is defined in detail at https://github.com/conveyal/datatools-server/issues/185,
 * but in essence, this strategy attempts to merge an active and future feed into
 * a combined file. For certain entities (specifically stops and routes) it uses
 * alternate fields as primary keys (stop_code and route_short_name) if they are
 * available. There is some complexity related to this in {@link #constructMergedTable(Table, List, ZipOutputStream)}.
 * Another defining characteristic is to prefer entities defined in the "future"
 * file if there are matching entities in the active file.
 * Future merge strategies could be added here. For example, some potential customers have mentioned a desire to
 * prefer entities from the active version, so that entities edited in Data Tools would override the values found
 * in the "future" file, which may have limited data attributes due to being exported from scheduling software with
 * limited GTFS support.
 */
public class MergeFeedsJob extends FeedSourceJob {

    private static final Logger LOG = LoggerFactory.getLogger(MergeFeedsJob.class);
    public static final ObjectMapper mapper = new ObjectMapper();
    private final Set<FeedVersion> feedVersions;
    private final FeedSource feedSource;
    public final MergeFeedsResult mergeFeedsResult;
    private final String filename;
    public final String projectId;
    public final MergeFeedsType mergeType;
    private File mergedTempFile = null;
    final FeedVersion mergedVersion;
    @JsonIgnore @BsonIgnore
    public Set<String> tripIdsToModifyForActiveFeed = new HashSet<>();
    @JsonIgnore @BsonIgnore
    public Set<String> tripIdsToSkipForActiveFeed = new HashSet<>();
    @JsonIgnore @BsonIgnore
    public Set<String> serviceIdsToExtend = new HashSet<>();
    @JsonIgnore @BsonIgnore
    public Set<String> serviceIdsToCloneAndRename = new HashSet<>();
    // Variables used for a service period merge.
    private FeedMergeContext feedMergeContext;

    /**
     * @param owner             user ID that initiated job
     * @param feedVersions      set of feed versions to merge
     * @param file              resulting merge filename (without .zip)
     * @param mergeType         the type of merge to perform {@link MergeFeedsType}
     */
    public MergeFeedsJob(Auth0UserProfile owner, Set<FeedVersion> feedVersions, String file, MergeFeedsType mergeType) {
        super(owner, mergeType.equals(REGIONAL) ? "Merging project feeds" : "Merging feed versions",
            JobType.MERGE_FEED_VERSIONS);
        this.feedVersions = feedVersions;
        // Construct full filename with extension
        this.filename = String.format("%s.zip", file);
        // If the merge type is regional, the file string should be equivalent to projectId, which
        // is used by the client to download the merged feed upon job completion.
        this.projectId = mergeType.equals(REGIONAL) ? file : null;
        this.mergeType = mergeType;
        // Assuming job is successful, mergedVersion will contain the resulting feed version.
        Project project = Persistence.projects.getById(projectId);
        // Grab parent feed source depending on merge type.
        FeedSource regionalFeedSource = null;
        // If storing a regional merge as a new version, find the feed source designated by the project.
        if (mergeType.equals(REGIONAL)) {
            regionalFeedSource = Persistence.feedSources.getById(project.regionalFeedSourceId);
            // Create new feed source if this is the first regional merge.
            if (regionalFeedSource == null) {
                regionalFeedSource = new FeedSource("REGIONAL MERGE", project.id, REGIONAL_MERGE);
                // Store new feed source.
                Persistence.feedSources.create(regionalFeedSource);
                // Update regional feed source ID on project.
                project.regionalFeedSourceId = regionalFeedSource.id;
                Persistence.projects.replace(project.id, project);
            }
        }
        // Assign regional feed source or simply the first parent feed source found in the feed version list (these
        // should all belong to the same feed source if the merge is not regional).
        this.feedSource = mergeType.equals(REGIONAL)
            ? regionalFeedSource
            : feedVersions.iterator().next().parentFeedSource();
        // Assuming job is successful, mergedVersion will contain the resulting feed version.
        // Merged version will be null if the new version should not be stored.
        this.mergedVersion = getMergedVersion(this, true);
        this.mergeFeedsResult = new MergeFeedsResult(mergeType);
    }

    @BsonIgnore @JsonIgnore
    public Set<FeedVersion> getFeedVersions() {
        return this.feedVersions;
    }

    /**
     * The final stage handles clean up (deleting temp file) and adding the next job to process the
     * new merged version (assuming the merge did not fail).
     */
    @Override
    public void jobFinished() {
        // Delete temp file to ensure it does not cause storage bloat. Note: merged file has already been stored
        // permanently.
        try {
            Files.delete(mergedTempFile.toPath());
        } catch (IOException e) {
            logAndReportToBugsnag(
                e,
                "Merged feed file {} not deleted. This may contribute to storage space shortages.",
                mergedTempFile.getAbsolutePath()
            );
        }
    }

    /**
     * Primary job logic handles collecting and sorting versions, creating a merged table for all versions, and writing
     * the resulting zip file to storage.
     */
    @Override
    public void jobLogic() {
        // Create temp zip file to add merged feed content to.
        try {
            mergedTempFile = File.createTempFile(filename, null);
        } catch (IOException e) {

            String message = "Error creating temp file for feed merge.";
            logAndReportToBugsnag(e, message);
            status.fail(message, e);
        }

        // Create the zipfile with try with resources so that it is always closed.
        try (ZipOutputStream out = new ZipOutputStream(new FileOutputStream(mergedTempFile))) {
            LOG.info("Created merge file: {}", mergedTempFile.getAbsolutePath());
            feedMergeContext = new FeedMergeContext(feedVersions, owner);

            // Determine which tables to merge (only merge GTFS+ tables for MTC extension).
            final List<Table> tablesToMerge = getTablesToMerge();
            int numberOfTables = tablesToMerge.size();

            // Skip merging process altogether if the failing condition is met.
            FeedMergeContext.TripMismatchedServiceIds serviceIdMismatch;
            if (feedMergeContext.areTripIdsMatchingButNotServiceIds()) {
                failMergeJob("Feed merge failed because the trip_ids are identical in the future and active feeds. A new service requires unique trip_ids for merging.");
                return;
            } else if ((serviceIdMismatch = feedMergeContext.shouldFailJobDueToMatchingTripIds()) != null) {
                // We cannot account for the case where service_ids do not match! It would be a bit too complicated
                // to handle this unique case, so instead just include in the failure reasons and use failure
                // strategy.
                failMergeJob(
                    String.format("Shared trip_id (%s) had mismatched service id between two feeds (active: %s, future: %s)",
                        serviceIdMismatch.tripId,
                        serviceIdMismatch.activeServiceId,
                        serviceIdMismatch.futureServiceId
                    )
                );
                return;
            }

            // Before initiating the merge process, get the merge strategy to use, which runs some pre-processing to
            // check for id conflicts for certain tables (e.g., trips and calendars).
            if (mergeType.equals(SERVICE_PERIOD)) {
                mergeFeedsResult.mergeStrategy = getMergeStrategy();
            }
            // Loop over GTFS tables and merge each feed one table at a time.
            for (int i = 0; i < numberOfTables; i++) {
                Table table = tablesToMerge.get(i);
                if (shouldSkipTable(table.name)) continue;
                double percentComplete = Math.round((double) i / numberOfTables * 10000d) / 100d;
                status.update("Merging " + table.name, percentComplete);
                // Perform the merge.
                LOG.info("Writing {} to merged feed", table.name);
                int mergedLineNumber = constructMergedTable(table, feedMergeContext.feedsToMerge, out);
                if (mergedLineNumber == 0) {
                    LOG.warn("Skipping {} table. No entries found in zip files.", table.name);
                } else if (mergedLineNumber == -1) {
                    LOG.error("Merge {} table failed!", table.name);
                }
            }
        } catch (IOException e) {
            String message = "Error creating output stream for feed merge.";
            logAndReportToBugsnag(e, message);
            status.fail(message, e);
        } finally {
            try {
                feedMergeContext.close();
            } catch (IOException e) {
                logAndReportToBugsnag(e, "Error closing FeedMergeContext object");
            }
        }
        if (!mergeFeedsResult.failed) {
            // Store feed locally and (if applicable) upload regional feed to S3.
            storeMergedFeed();
            status.completeSuccessfully("Merged feed created successfully.");
        }
        LOG.info("Feed merge is complete.");
        if (shouldLoadAsNewFeed()) {
            mergedVersion.inputVersions = feedVersions.stream().map(FeedVersion::retrieveId).collect(Collectors.toSet());
            // Handle the processing of the new version when storing new version (note: s3 upload is handled within this job).
            // We must add this job in jobLogic (rather than jobFinished) because jobFinished is called after this job's
            // subJobs are run.
            addNextJob(new ProcessSingleFeedJob(mergedVersion, owner, true));
        }
    }

    private List<Table> getTablesToMerge() {
        List<Table> tablesToMerge = Arrays.stream(Table.tablesInOrder)
            .filter(Table::isSpecTable)
            .collect(Collectors.toList());
        if (DataManager.isExtensionEnabled("mtc")) {
            // Merge GTFS+ tables only if MTC extension is enabled. We should do this for both
            // regional and MTC merge strategies.
            tablesToMerge.addAll(Arrays.asList(GtfsPlusTable.tables));
        }
        return tablesToMerge;
    }

    /**
     * Check if the table should be skipped in the merged output.
     */
    private boolean shouldSkipTable(String tableName) {
        if (mergeType.equals(REGIONAL) && tableName.equals(Table.FEED_INFO.name)) {
            // It does not make sense to include the feed_info table when performing a
            // regional feed merge because this file is intended to contain data specific to
            // a single agency feed.
            // TODO: Perhaps future work can generate a special feed_info file for the merged
            //  file.
            LOG.warn("Skipping feed_info table for regional merge.");
            return true;
        }
        if (tableName.equals(Table.PATTERNS.name) || tableName.equals(Table.PATTERN_STOP.name)) {
            LOG.warn("Skipping editor-only table {}.", tableName);
            return true;
        }
        return false;
    }

    /**
     * Determines whether {@link ProcessSingleFeedJob} should be run as a follow on task at the completion of the merge.
     * The merge must have no errors and the mergedVersion must not be null (i.e., we need somewhere to put the new
     * version).
     */
    private boolean shouldLoadAsNewFeed() {
        return mergedVersion != null && !status.error && !mergeFeedsResult.failed;
    }

    /**
     * Handle updating {@link MergeFeedsResult} and the overall job status when a failure condition is triggered while
     * merging feeds.
     */
    public void failMergeJob(String failureMessage) {
        LOG.error(failureMessage);
        mergeFeedsResult.failed = true;
        mergeFeedsResult.errorCount++;
        mergeFeedsResult.failureReasons.add(failureMessage);
        // Use generic message for overall job status.
        status.fail("Merging feed versions failed.");
    }

    /**
     * Handles writing the GTFS zip file to disk. For REGIONAL merges, this will end up in a project subdirectory on s3.
     * Otherwise, it will write to a new version.
     */
    private void storeMergedFeed() {
        if (mergedVersion != null) {
            // Store the zip file for the merged feed version.
            try {
                mergedVersion.newGtfsFile(new FileInputStream(mergedTempFile));
            } catch (IOException e) {
                logAndReportToBugsnag(e, "Could not store merged feed for new version");
            }
        }
        // Write the new latest regional merge file to s3://$BUCKET/project/$PROJECT_ID.zip
        if (mergeType.equals(REGIONAL)) {
            status.update("Saving merged feed.", 95);
            // Store the project merged zip locally or on s3
            if (DataManager.useS3) {
                String s3Key = String.join("/", "project", filename);
                try {
                    S3Utils.getDefaultS3Client().putObject(S3Utils.DEFAULT_BUCKET, s3Key, mergedTempFile);
                } catch (CheckedAWSException e) {
                    String message = "Could not upload store merged feed for new version";
                    logAndReportToBugsnag(e, message);
                    status.fail(message, e);
                }
                LOG.info("Storing merged project feed at {}", S3Utils.getDefaultBucketUriForKey(s3Key));
            } else {
                try {
                    FeedVersion.feedStore.newFeed(filename, new FileInputStream(mergedTempFile), null);
                } catch (IOException e) {
                    String message = "Could not store feed for project " + filename;
                    logAndReportToBugsnag(e, message);
                    status.fail(message, e);
                }
            }
        }
    }

    /**
     * Merge the specified table for multiple GTFS feeds.
     *
     * @param table        table to merge
     * @param feedsToMerge map of feedSources to zipFiles from which to extract the .txt tables
     * @param out          output stream to write table into
     * @return number of lines in merged table
     */
    private int constructMergedTable(Table table, List<FeedToMerge> feedsToMerge, ZipOutputStream out) {
        MergeLineContext ctx = null;
        try {
            ctx = MergeLineContext.create(this, table, out);

            // Iterate over each zip file. For service period merge, the first feed is the future GTFS.
            for (int feedIndex = 0; feedIndex < feedsToMerge.size(); feedIndex++) {
                ctx.startNewFeed(feedIndex);
                if (ctx.skipFile) continue;
                LOG.info("Adding {} table for {}{}", table.name, ctx.feedSource.name, ctx.version.version);
                // Iterate over the rows of the table and write them to the merged output table. If an error was
                // encountered, return -1 to fail the merge job immediately.
                if (!ctx.iterateOverRows()) {
                    return -1;
                }
            }
            ctx.flushAndClose();
        } catch (IOException e) {
            List<String> versionNames = feedVersions.stream()
                .map(version -> version.parentFeedSource().name)
                .collect(Collectors.toList());
            String message = "Error merging feed sources: " + versionNames;
            logAndReportToBugsnag(e, message);
            status.fail(message, e);
        }
        if (ctx != null) {
            // Track the number of lines in the merged table and return final number.
            mergeFeedsResult.linesPerTable.put(table.name, ctx.mergedLineNumber);
            return ctx.mergedLineNumber;
        }
        return 0;
    }

    /**
     * Get the merge strategy to use for MTC service period merges by checking the active and future feeds for various
     * combinations of matching trip and service IDs.
     */
    private MergeStrategy getMergeStrategy() {
        if (feedMergeContext.tripIdsMatch) {
            // If trip ids and service ids match, these ids will be extended to the future per MTC requirements.
            // Effectively this exact match condition means that the future feed will be used as is
            // (including stops, routes, etc.), the only modification being service date ranges.
            // (If service ids mismatch, shouldFailJobDueToMatchingTripIds will fail the merge.)
            return feedMergeContext.serviceIdsMatch ? EXTEND_FUTURE : MergeStrategy.DEFAULT;
        }
        if (feedMergeContext.serviceIdsMatch) {
            // If just the service_ids are an exact match, check the that the stop_times having matching signatures
            // between the two feeds (i.e., each stop time in the ordered list is identical between the two feeds).
            Feed futureFeed = feedMergeContext.future.feed;
            Feed activeFeed = feedMergeContext.active.feed;
            for (String tripId : feedMergeContext.sharedTripIds) {
                compareStopTimesAndCollectTripAndServiceIds(tripId, futureFeed, activeFeed);
            }
            // If a trip only in the active feed references a service_id that is set to be extended, that
            // service_id needs to be cloned and renamed to differentiate it from the same service_id in
            // the future feed. (The trip in question will be linked to the cloned service_id.)
            Set<String> tripsOnlyInActiveFeed = Sets.difference(feedMergeContext.active.tripIds, feedMergeContext.future.tripIds);
            tripsOnlyInActiveFeed.stream()
                .map(tripId -> activeFeed.trips.get(tripId).service_id)
                .filter(serviceId -> serviceIdsToExtend.contains(serviceId))
                .forEach(serviceId -> serviceIdsToCloneAndRename.add(serviceId));
            // If a trip only in the future feed references a service_id that is set to be extended, that
            // service_id needs to be cloned and renamed to differentiate it from the same service_id in
            // the future feed. (The trip in question will be linked to the cloned service_id.)
            Set<String> tripsOnlyInFutureFeed = Sets.difference(feedMergeContext.future.tripIds, feedMergeContext.active.tripIds);
            tripsOnlyInFutureFeed.stream()
                .map(tripId -> futureFeed.trips.get(tripId).service_id)
                .filter(serviceId -> serviceIdsToExtend.contains(serviceId))
                .forEach(serviceId -> serviceIdsToCloneAndRename.add(serviceId));

            return CHECK_STOP_TIMES;
        }
        // If neither the trips or services are exact matches, use the default merge strategy.
        return MergeStrategy.DEFAULT;
    }

    /**
     * Compare stop times for the given tripId between the future and active feeds. The comparison will inform whether
     * trip and/or service IDs should be modified in the output merged feed.
     */
    private void compareStopTimesAndCollectTripAndServiceIds(String tripId, Feed futureFeed, Feed activeFeed) {
        // Fetch all ordered stop_times for each shared trip_id and compare the two sets for the
        // future and active feed. If the stop_times are an exact match, include one instance of the trip
        // (ignoring the other identical one). If they do not match, modify the active trip_id and include.
        List<StopTime> futureStopTimes = Lists.newArrayList(futureFeed.stopTimes.getOrdered(tripId));
        List<StopTime> activeStopTimes = Lists.newArrayList(activeFeed.stopTimes.getOrdered(tripId));
        String futureServiceId = futureFeed.trips.get(tripId).service_id;
        if (!stopTimesMatch(futureStopTimes, activeStopTimes)) {
            // If stop_times or services do not match, the trip will be cloned. Also, track the service_id
            // (it will need to be cloned and renamed for both active feeds).
            tripIdsToModifyForActiveFeed.add(tripId);
            serviceIdsToCloneAndRename.add(futureServiceId);
        } else {
            // If the trip's stop_times are an exact match, we can safely include just the
            // future trip and exclude the active one. Also, track the service_id (it will need to be
            // extended to the full time range).
            tripIdsToSkipForActiveFeed.add(tripId);
            serviceIdsToExtend.add(futureServiceId);
        }
    }

    public String getFeedSourceId() {
        return feedSource.id;
    }

    private void logAndReportToBugsnag(Exception e, String message, Object... args) {
        LOG.error(message, args, e);
        ErrorUtils.reportToBugsnag(e, "datatools", message, owner);
    }

    @BsonIgnore @JsonIgnore
    public FeedMergeContext getFeedMergeContext() {
        return feedMergeContext;
    }
}
