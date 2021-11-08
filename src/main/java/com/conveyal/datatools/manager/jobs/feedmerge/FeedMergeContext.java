package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.utils.MergeFeedUtils;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.loader.Table;
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
    public final FeedToMerge activeFeedToMerge;
    public final FeedToMerge futureFeedToMerge;
    public final Set<String> activeTripIds;
    public final Set<String> futureTripIds;
    public final boolean serviceIdsMatch;
    public final boolean tripIdsMatch;
    public final Feed futureFeed;
    public final Feed activeFeed;
    public final LocalDate activeFeedFirstDate;
    private LocalDate futureFeedFirstDate;
    private LocalDate futureFirstCalendarStartDate = LocalDate.MAX;
    /** Trip ids shared between the active and future feed. */
    public final Set<String> sharedTripIds;


    public FeedMergeContext(Set<FeedVersion> feedVersions, Auth0UserProfile owner) throws IOException {
        feedsToMerge = MergeFeedUtils.collectAndSortFeeds(feedVersions, owner);
        activeFeedToMerge = feedsToMerge.get(1);
        futureFeedToMerge = feedsToMerge.get(0);
        futureFeedToMerge.collectTripAndServiceIds();
        activeFeedToMerge.collectTripAndServiceIds();
        activeTripIds = activeFeedToMerge.idsForTable.get(Table.TRIPS);
        futureTripIds = futureFeedToMerge.idsForTable.get(Table.TRIPS);

        // Determine whether service and trip IDs are exact matches.
        serviceIdsMatch = activeFeedToMerge.serviceIds.equals(futureFeedToMerge.serviceIds);
        tripIdsMatch = activeTripIds.equals(futureTripIds);

        futureFeed = new Feed(DataManager.GTFS_DATA_SOURCE, futureFeedToMerge.version.namespace);
        activeFeed = new Feed(DataManager.GTFS_DATA_SOURCE, activeFeedToMerge.version.namespace);
        sharedTripIds = Sets.intersection(activeTripIds, futureTripIds);

        // Initialize future and active feed's first date to the first calendar date from validation result.
        // This is equivalent to either the earliest date of service defined for a calendar_date record or the
        // earliest start_date value for a calendars.txt record. For MTC, however, they require that GTFS
        // providers use calendars.txt entries and prefer that this value (which is used to determine cutoff
        // dates for the active feed when merging with the future) be strictly assigned the earliest
        // calendar#start_date (unless that table for some reason does not exist).
        activeFeedFirstDate = activeFeedToMerge.version.validationResult.firstCalendarDate;
        futureFeedFirstDate = futureFeedToMerge.version.validationResult.firstCalendarDate;
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
                TripMismatchedServiceIds mismatchInfo = tripIdHasMismatchedServiceIds(tripId, futureFeed, activeFeed);
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
    public static TripMismatchedServiceIds tripIdHasMismatchedServiceIds(String tripId, Feed futureFeed, Feed activeFeed) {
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

    public LocalDate getFutureFeedFirstDate() {
        return futureFeedFirstDate;
    }

    public void setFutureFeedFirstDate(LocalDate futureFeedFirstDate) {
        this.futureFeedFirstDate = futureFeedFirstDate;
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
