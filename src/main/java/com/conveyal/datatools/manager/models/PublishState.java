package com.conveyal.datatools.manager.models;

/**
 * Enumeration of possible publish states for a FeedVersion.
 */
public enum PublishState {
    PUBLISHED,
    PUBLISHING,
    PUBLISH_BLOCKED,
    FEED_LOADING,
    READY_TO_PUBLISH
}
