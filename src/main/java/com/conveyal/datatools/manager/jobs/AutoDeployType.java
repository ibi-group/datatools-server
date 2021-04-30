package com.conveyal.datatools.manager.jobs;

/**
 * This enum contains the auto deploy types which define when an {@link AutoDeployJob} should be started.
 */
public enum AutoDeployType {
    /**
     * Auto-deployment should occur whenever a new feed version is created as a result of fetching a feed (either
     * manually or automatically).
     */
    ON_FEED_FETCH,
    /**
     * Auto-deployment should occur whenever a new feed version is created for any reason.
     */
    ON_PROCESS_FEED,
}
