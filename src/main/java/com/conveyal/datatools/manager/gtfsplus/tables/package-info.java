/**
 * This package contains classes that correspond to those found for GTFS entity types in
 * {@link com.conveyal.gtfs.model}, but for GTFS+ entity types. It also contains
 * {@link com.conveyal.datatools.manager.gtfsplus.tables.GtfsPlusTable}, which extends the
 * {@link com.conveyal.gtfs.loader.Table} in order to define a table specification for this set of
 * extension tables.
 *
 * Note: these classes are primarily used for the MTC merge type in
 * {@link com.conveyal.datatools.manager.jobs.MergeFeedsJob}. There may be an opportunity to also use
 * these classes in the GTFS+ validation code path found in
 * {@link com.conveyal.datatools.manager.controllers.api.GtfsPlusController}; however,
 * TODO a way to define an enum set for string field values would need to first be added to support
 *  fields such as {@link com.conveyal.datatools.manager.gtfsplus.tables.StopAttribute#cardinal_direction}.
 */
package com.conveyal.datatools.manager.gtfsplus.tables;

