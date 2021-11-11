package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.utils.MergeFeedUtils;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.loader.Table;
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
    /**
     * Trip ids shared between the active and future feed.
     */
    public final Set<String> sharedTripIds;
    /**
     * Holds the auto-generated agency id to be updated if none was provided.
     */
    private String newAgencyId;

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

        // Initialize, before processing rows, the calendar start dates from the future feed.
        for (Calendar c : futureFeed.calendars.getAll()) {
            if (futureFirstCalendarStartDate.isAfter(c.start_date)) {
                futureFirstCalendarStartDate = c.start_date;
            }
        }
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

    public LocalDate getFutureFirstCalendarStartDate() {
        return futureFirstCalendarStartDate;
    }

    public LocalDate getFutureFeedFirstDate() {
        return futureFeedFirstDate;
    }

    public void setFutureFeedFirstDate(LocalDate futureFeedFirstDate) {
        this.futureFeedFirstDate = futureFeedFirstDate;
    }

    public String getNewAgencyId() {
        return newAgencyId;
    }

    public void setNewAgencyId(String newAgencyId) {
        this.newAgencyId = newAgencyId;
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
