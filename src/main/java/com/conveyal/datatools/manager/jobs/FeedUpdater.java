package com.conveyal.datatools.manager.jobs;

import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.conveyal.datatools.manager.DataManager;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.HashUtils;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.conveyal.datatools.manager.extensions.mtc.MtcFeedResource.AGENCY_ID;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

/**
 * This class is used to schedule an {@link UpdateFeedsTask}, which will check the specified S3 bucket (and prefix) for
 * new files. If a new feed is found, the feed will be downloaded and its MD5 hash will be checked against the feed
 * versions for the related feed source. When it finds a match, it will ensure that the {@link FeedSource#publishedVersionId}
 * matches the {@link FeedVersion#namespace} for the version/file found on S3 and will update it if not.
 *
 * This is all done to ensure that the "published" version in MTC's RTD database matches the published version in Data
 * Tools, which is primarily used to ensure that any alerts are built using GTFS stop or route IDs from the active GTFS
 * feed.
 */
public class FeedUpdater {
    private Map<String, String> eTagForFeed;
    private final String feedBucket;
    private final String bucketFolder;
    private static final Logger LOG = LoggerFactory.getLogger(FeedUpdater.class);

    private FeedUpdater(int updateFrequencySeconds, String feedBucket, String bucketFolder) {
        LOG.info("Setting feed update to check every {} seconds", updateFrequencySeconds);
        DataManager.scheduler.scheduleAtFixedRate(new UpdateFeedsTask(), 0, updateFrequencySeconds, TimeUnit.SECONDS);
        this.feedBucket = feedBucket;
        this.bucketFolder = bucketFolder;
    }

    /**
     * Create a {@link FeedUpdater} to poll the provided S3 bucket/prefix at the specified interval (in seconds) for
     * updated files. The updater's task is run using the {@link DataManager#scheduler}.
     * @param updateFrequencySeconds
     * @param s3Bucket
     * @param s3Prefix
     * @return
     */
    public static FeedUpdater schedule(int updateFrequencySeconds, String s3Bucket, String s3Prefix) {
        return new FeedUpdater(updateFrequencySeconds, s3Bucket, s3Prefix);
    }

    class UpdateFeedsTask implements Runnable {
        public void run() {
            Map<String, String> updatedTags;
            try {
                LOG.debug("Checking MTC feeds for newly processed versions");
                updatedTags = checkForUpdatedFeeds();
                eTagForFeed.putAll(updatedTags);
                if (!updatedTags.isEmpty()) LOG.info("New eTag list: {}", eTagForFeed);
                else LOG.debug("No feeds updated (eTags on S3 match current list).");
            } catch (Exception e) {
                LOG.error("Error updating feeds {}", e);
            }
        }
    }

    /**
     * Check for any updated feeds that have been published to the S3 bucket. This tracks eTagForFeed (AWS file hash) of s3
     * objects in order to keep data-tools application in sync with external processes (for example, MTC RTD).
     * @return          map of feedIDs to eTag values
     */
    private Map<String, String> checkForUpdatedFeeds() {
        if (eTagForFeed == null) {
            // If running the check for the first time, instantiate the eTag map.
            LOG.info("Running initial check for feeds on S3.");
            eTagForFeed = new HashMap<>();
        }
        Map<String, String> newTags = new HashMap<>();
        // iterate over feeds in download_prefix folder and register to gtfsApi (MTC project)
        ObjectListing gtfsList = FeedStore.s3Client.listObjects(feedBucket, bucketFolder);
        for (S3ObjectSummary objSummary : gtfsList.getObjectSummaries()) {

            String eTag = objSummary.getETag();
            if (!eTagForFeed.containsValue(eTag)) {
                String keyName = objSummary.getKey();
                // Don't add object if it is a dir
                if (keyName.equals(bucketFolder)) {
                    continue;
                }
                String filename = keyName.split("/")[1];
                String feedId = filename.replace(".zip", "");
                // Skip object if the filename is null
                if ("null".equals(feedId)) continue;
                try {
                    LOG.warn("New version found for at {}/{}. ETag = {}. Downloading from s3", feedBucket, keyName, eTag);
                    S3Object object = FeedStore.s3Client.getObject(feedBucket, keyName);
                    InputStream in = object.getObjectContent();
                    File file = new File(FeedStore.basePath, filename);
                    OutputStream out = new FileOutputStream(file);
                    ByteStreams.copy(in, out);
                    String md5 = HashUtils.hashFile(file);
                    FeedSource feedSource = null;
                    List<ExternalFeedSourceProperty> properties = Persistence.externalFeedSourceProperties.getFiltered(and(eq("value", feedId), eq("name", AGENCY_ID)));
                    if (properties.size() > 1) {
                        StringBuilder b = new StringBuilder();
                        properties.forEach(b::append);
                        LOG.warn("Found multiple feed sources for feedId {}: {}",
                                feedId,
                                properties.stream().map(p -> p.feedSourceId).collect(Collectors.joining(",")));
                    }
                    for (ExternalFeedSourceProperty prop : properties) {
                        // FIXME: What if there are multiple props found for different feed sources. This could happen if
                        // multiple projects have been synced with MTC or if the ExternalFeedSourceProperty for a feed
                        // source is not deleted properly when the feed source is deleted.
                        feedSource = Persistence.feedSources.getById(prop.feedSourceId);
                    }
                    if (feedSource == null) {
                        LOG.error("No feed source found for feed ID {}", feedId);
                        continue;
                    }
                    Collection<FeedVersion> versions = feedSource.retrieveFeedVersions();
                    LOG.info("Searching for md5 {} across {} versions for {} ({})", md5, versions.size(), feedSource.name, feedSource.id);
                    boolean foundMatchingVersion = false;
                    int count = 0;
                    for (FeedVersion feedVersion : versions) {
                        LOG.info("version {} md5: {}", count++, feedVersion.hash);
                        if (feedVersion.hash.equals(md5)) {
                            foundMatchingVersion = true;
                            LOG.info("Found local version that matches latest file on S3  (SQL namespace={})", feedVersion.namespace);
                            if (!feedVersion.namespace.equals(feedSource.publishedVersionId)) {
                                LOG.info("Updating published version for feed {} to latest s3 published feed.", feedId);
                                Persistence.feedSources.updateField(feedSource.id, "publishedVersionId", feedVersion.namespace);
                                Persistence.feedVersions.updateField(feedVersion.id, "processing", false);
                            } else {
                                LOG.info("No need to update published version (published s3 feed already matches feed source's published namespace).");
                            }
                        }
                    }
                    if (!foundMatchingVersion) {
                        LOG.error("Did not find version for feed {} that matched eTag found in s3!!!", feedId);
                    }
                } catch (Exception e) {
                    LOG.warn("Could not load feed " + keyName, e);
                } finally {
                    // Add new tag to map used for tracking updates. NOTE: this is in a finally block because we still
                    // need to track the eTags even for feed sources that were not found. Otherwise, the feeds will be
                    // re-downloaded each time the update task is run, which could cause many unnecessary S3 operations.
                    newTags.put(feedId, eTag);
                }
            }
        }
        return newTags;
    }

}
