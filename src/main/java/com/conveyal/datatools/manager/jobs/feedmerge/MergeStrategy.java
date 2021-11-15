package com.conveyal.datatools.manager.jobs.feedmerge;

/**
 * This enum defines the different strategies for merging, which is currently dependent on whether trip_ids and/or
 * service_ids between the two feeds are exactly matching.
 */
public enum MergeStrategy {
    /**
     * If service_ids and trip_ids between active and future feed are all unique, all IDs shall be included
     * in merged feed. If a service_id from the active calendar has end_date in the future, the end_date shall be
     * set to one day prior to the earliest start_date in future dataset before appending the calendar record to
     * the merged file. It shall be ensured that trip_ids between active and future datasets must not match.
     */
    DEFAULT,
    /**
     * If service_ids in active and future feed exactly match but only some of the trip_ids match then the merge
     * strategy shall handle the following three cases:
     * - *trip_id in both feeds*: The service shall start from the data merge date and end at the future feed’s service
     *   end date.
     *   Note: The merge process shall validate records in stop_times.txt file for same trip signature (same set of
     *   stops with same sequence). Trips with matching stop_times will be included as is (but not duplicated of course).
     *   Trips that do not match on stop_times will be handled with the below approaches.
     *   Note: Same service IDs shall be used (but extended to account for the full range of dates from active to future).
     * - *trip_id in active feed*: A new service shall be created starting from the merge date and expiring at the end
     *   of active service period.
     *   Note: a new service_id will be generated for these active trips in the merged feed (rather than using the
     *   service_id with extended range).
     * - *trip_id in future feed*: A new service shall be created for these trips with service period defined in future
     *   feed
     *   Note: a new service_id will be generated for these future trips in the merged feed (rather than using the
     *   service_id with extended range).
     */
    CHECK_STOP_TIMES
}