package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.utils.MergeFeedUtils;
import com.conveyal.gtfs.loader.Feed;
import com.google.common.collect.Sets;

import java.io.Closeable;
import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.Set;

/**
 * Contains merge information between an active feed and a future feed.
 */
public class FeedMergeContext implements Closeable {
    public final List<FeedToMerge> feedsToMerge;
    public final FeedContext active;
    public final FeedContext future;
    public final boolean serviceIdsMatch;
    public final boolean tripIdsMatch;
    private LocalDate futureFirstCalendarStartDate = LocalDate.MAX;
    public final Set<String> sharedTripIds;

    public FeedMergeContext(Set<FeedVersion> feedVersions, Auth0UserProfile owner) throws IOException {
        feedsToMerge = MergeFeedUtils.collectAndSortFeeds(feedVersions, owner);
        FeedToMerge activeFeedToMerge = feedsToMerge.get(1);
        FeedToMerge futureFeedToMerge = feedsToMerge.get(0);
        active = new FeedContext(activeFeedToMerge);
        future = new FeedContext(futureFeedToMerge);

        // Determine whether service and trip IDs are exact matches.
        serviceIdsMatch = activeFeedToMerge.serviceIds.equals(futureFeedToMerge.serviceIds);
        tripIdsMatch = active.tripIds.equals(future.tripIds);
        sharedTripIds = Sets.intersection(active.tripIds, future.tripIds);
    }

    @Override
    public void close() throws IOException {
        for (FeedToMerge feed : feedsToMerge) {
            feed.close();
        }
    }

    /**
     * Partially handles MTC Requirement to detect matching trip ids linked to different service ids.
     * @return true if trip ids match but not service ids (in such situation, merge should fail).
     */
    public boolean areTripIdsMatchingButNotServiceIds() {
        // If only trip IDs match and not service IDs, do not permit merge to continue.
        return tripIdsMatch && !serviceIdsMatch;
    }

    /**
     * Determines whether there a same trip id is linked to different service ids in the active and future feed
     * (MTC requirement).
     * @return the first {@link TripMismatchedServiceIds} whose trip id is linked to different service ids,
     *         or null if nothing found.
     */
    public TripMismatchedServiceIds shouldFailJobDueToMatchingTripIds() {
        if (serviceIdsMatch) {
            // If just the service_ids are an exact match, check the that the stop_times having matching signatures
            // between the two feeds (i.e., each stop time in the ordered list is identical between the two feeds).
            for (String tripId : sharedTripIds) {
                TripMismatchedServiceIds mismatchInfo = tripIdHasMismatchedServiceIds(tripId, future.feed, active.feed);
                if (mismatchInfo.hasMismatch) {
                    return mismatchInfo;
                }
            }
        }

        return null;
    }

    /**
     * Compare stop times for the given tripId between the future and active feeds. The comparison will inform whether
     * trip and/or service IDs should be modified in the output merged feed.
     * @return A {@link TripMismatchedServiceIds} with info on whether the given tripId is found in
     *         different service ids in the active and future feed.
     */
    private static TripMismatchedServiceIds tripIdHasMismatchedServiceIds(String tripId, Feed futureFeed, Feed activeFeed) {
        String futureServiceId = futureFeed.trips.get(tripId).service_id;
        String activeServiceId = activeFeed.trips.get(tripId).service_id;
        return new TripMismatchedServiceIds(tripId, !futureServiceId.equals(activeServiceId), activeServiceId, futureServiceId);
    }

    public LocalDate getFutureFirstCalendarStartDate() {
        return futureFirstCalendarStartDate;
    }

    public void setFutureFirstCalendarStartDate(LocalDate futureFirstCalendarStartDate) {
        this.futureFirstCalendarStartDate = futureFirstCalendarStartDate;
    }

    /**
     * Holds the status of a trip service id mismatch determination.
     */
    public static class TripMismatchedServiceIds {
        public final String tripId;
        public final String activeServiceId;
        public final String futureServiceId;
        public final boolean hasMismatch;

        TripMismatchedServiceIds(String tripId, boolean hasMismatch, String activeServiceId, String futureServiceId) {
            this.tripId = tripId;
            this.hasMismatch = hasMismatch;
            this.activeServiceId = activeServiceId;
            this.futureServiceId = futureServiceId;
        }
    }
}
