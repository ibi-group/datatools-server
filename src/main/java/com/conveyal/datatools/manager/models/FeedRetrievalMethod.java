package com.conveyal.datatools.manager.models;

/**
 * Represents ways feeds can be retrieved. Note: this enum was originally developed for feed sources, which were
 * limited to a single retrieval method per source; however, use of this software has evolved in such a way that
 * supports GTFS data for a single feed source to be retrieved in a multitude of ways, including: fetching via URL,
 * uploading manually, creating with the editor, or transforming in some way (e.g., merging multiple versions or
 * post-processing a single version).
 */
public enum FeedRetrievalMethod {
    /** Feed automatically retrieved over HTTP on some regular basis. */
    FETCHED_AUTOMATICALLY,
    /** Feed manually uploaded by someone, perhaps the agency, or perhaps an internal user. */
    MANUALLY_UPLOADED,
    /** Feed produced in-house in a GTFS Editor instance (i.e., from a database snapshot). */
    PRODUCED_IN_HOUSE,
    /** Feed produced in-house in the GTFS+ Editor. */
    PRODUCED_IN_HOUSE_GTFS_PLUS,
    /** Feed produced from a regional merge (e.g., merging all feeds from a project). */
    REGIONAL_MERGE,
    /** Feed produced by merging two versions that span different service periods. */
    SERVICE_PERIOD_MERGE,
    /** Feed produced by cloning an existing version. */
    VERSION_CLONE
}