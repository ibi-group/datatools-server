package com.conveyal.datatools.manager.jobs;

/**
 * This enum contains the auto deploy types which define when an {@link AutoDeployJob} should be started.
 */
public enum AutoDeployType {
    ON_FEED_FETCH,
    DAILY,
    ON_PROCESS_FEED,
}
