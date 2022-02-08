package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.datatools.manager.jobs.MergeFeedsJob;

/**
 * This enum contains the types of merge feeds that {@link MergeFeedsJob} can currently perform.
 */
public enum MergeFeedsType {
    /** Merge feed versions that represent different transit agencies (usually operating in the same geographic region */
    REGIONAL,
    /**
     * Merge two feed versions from the same transit agency that contain service for different time periods (usually to
     * combine two overlapping time periods, for example, the current schedules and the future schedules).
     */
    SERVICE_PERIOD
}
