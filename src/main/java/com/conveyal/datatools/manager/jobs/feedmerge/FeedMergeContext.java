package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.utils.MergeFeedUtils;
import com.conveyal.gtfs.model.Calendar;
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
    public final LocalDate futureFirstCalendarStartDate;
    public final Set<String> sharedTripIds;

    public FeedMergeContext(Set<FeedVersion> feedVersions, Auth0UserProfile owner) throws IOException {
        feedsToMerge = MergeFeedUtils.collectAndSortFeeds(feedVersions, owner);
        FeedToMerge activeFeedToMerge = feedsToMerge.get(1);
        FeedToMerge futureFeedToMerge = feedsToMerge.get(0);
        active = new FeedContext(activeFeedToMerge);
        future = new FeedContext(futureFeedToMerge);

        // Determine whether service and trip IDs are exact matches.
        serviceIdsMatch = activeFeedToMerge.serviceIdsInUse.equals(futureFeedToMerge.serviceIdsInUse);
        tripIdsMatch = active.tripIds.equals(future.tripIds);
        sharedTripIds = Sets.intersection(active.tripIds, future.tripIds);

        // Initialize, before processing any rows, the first calendar start dates from the future feed.
        LocalDate futureFirstCalStartDate = LocalDate.MAX;
        for (Calendar c : future.feed.calendars.getAll()) {
            if (futureFirstCalStartDate.isAfter(c.start_date)) {
                futureFirstCalStartDate = c.start_date;
            }
        }
        this.futureFirstCalendarStartDate = futureFirstCalStartDate;
    }

    public void collectServiceIdsToRemove() {
        active.setServiceIdsToRemoveUsingOtherFeed(getActiveTripIdsNotInFutureFeed());
        future.setServiceIdsToRemoveUsingOtherFeed(getFutureTripIdsNotInActiveFeed());
    }

    @Override
    public void close() throws IOException {
        for (FeedToMerge feed : feedsToMerge) {
            feed.close();
        }
    }

    /**
     * Partially handles the Revised MTC Feed Merge Requirement
     * to detect disjoint trip ids between the active/future feeds.
     * @return true if no trip ids from the active feed is found in the future feed, and vice-versa.
     */
    public boolean areActiveAndFutureTripIdsDisjoint() {
        return sharedTripIds.isEmpty();
    }

    /**
     * Obtains the trip ids found in the active feed, but not in the future feed.
     */
    public Sets.SetView<String> getActiveTripIdsNotInFutureFeed() {
        return Sets.difference(active.tripIds, future.tripIds);
    }

    /**
     * Obtains the trip ids found in the future feed, but not in the active feed.
     */
    public Sets.SetView<String> getFutureTripIdsNotInActiveFeed() {
        return Sets.difference(future.tripIds, active.tripIds);
    }
}
