package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.loader.Table;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Set;

/**
 * Contains information related to a feed to merge.
 */
public class FeedContext {
    public final FeedToMerge feedToMerge;
    public final Set<String> tripIds;
    public final Feed feed;
    private LocalDate feedFirstDate;
    /**
     * Holds the auto-generated agency id to be updated for each feed if none was provided.
     */
    private String newAgencyId;

    public FeedContext(FeedToMerge givenFeedToMerge) throws IOException {
        feedToMerge = givenFeedToMerge;
        feedToMerge.collectTripAndServiceIds();
        tripIds = feedToMerge.idsForTable.get(Table.TRIPS);
        feed = new Feed(DataManager.GTFS_DATA_SOURCE, feedToMerge.version.namespace);

        // Initialize future and active feed's first date to the first calendar date from validation result.
        // This is equivalent to either the earliest date of service defined for a calendar_date record or the
        // earliest start_date value for a calendars.txt record. For MTC, however, they require that GTFS
        // providers use calendars.txt entries and prefer that this value (which is used to determine cutoff
        // dates for the active feed when merging with the future) be strictly assigned the earliest
        // calendar#start_date (unless that table for some reason does not exist).
        feedFirstDate = feedToMerge.version.validationResult.firstCalendarDate;
    }

    public LocalDate getFeedFirstDate() { return feedFirstDate; }

    public void setFeedFirstDate(LocalDate firstDate) { feedFirstDate = firstDate; }

    public String getNewAgencyId() {
        return newAgencyId;
    }

    public void setNewAgencyId(String agencyId) {
        newAgencyId = agencyId;
    }
}
