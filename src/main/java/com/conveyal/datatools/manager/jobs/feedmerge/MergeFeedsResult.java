package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.datatools.manager.jobs.MergeFeedsJob;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Contains the result of {@link MergeFeedsJob}.
 */
public class MergeFeedsResult implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Type of merge operation performed */
    public MergeFeedsType type;
    public MergeStrategy mergeStrategy = MergeStrategy.DEFAULT;
    /** Contains the set of IDs for records that were excluded in the merged feed */
    public Set<String> skippedIds = new HashSet<>();
    /**
     * Track the set of service IDs to end up in the merged feed in order to determine which calendar, calendar_dates and
     * trip records should be retained in the merged result.
     */
    public Set<String> serviceIds  = new HashSet<>();

    /**
     * Track the set of service IDs obtained from calendar records.
     */
    public Set<String> calendarServiceIds = new HashSet<>();

    /**
     * Track the set of service IDs obtained from calendar date records.
     */
    public Set<String> calendarDatesServiceIds = new HashSet<>();

    /**
     * Track various table ids for resolving foreign references.
     */
    public Set<String> stopIds = new HashSet<>();
    public Set<String> locationGroupStopIds = new HashSet<>();

    /**
     * Track the set of route IDs to end up in the merged feed in order to determine which route_attributes
     * records should be retained in the merged result.
     */
    public Set<String> routeIds  = new HashSet<>();
    /** Contains the set of IDs that had their values remapped during the merge */
    public Map<String, String> remappedIds = new HashMap<>();
    /** Mapping of table name to line count in merged file */
    public Map<String, Integer> linesPerTable = new HashMap<>();
    public int remappedReferences;
    public int recordsSkipCount;
    public Date startTime;
    public boolean failed;
    public int errorCount;
    /** Set of reasons explaining why merge operation failed */
    public Set<String> failureReasons = new HashSet<>();
    public Set<String> tripIdsToCheck = new HashSet<>();

    public MergeFeedsResult (MergeFeedsType type) {
        this.type = type;
        this.startTime = new Date();
    }
}
