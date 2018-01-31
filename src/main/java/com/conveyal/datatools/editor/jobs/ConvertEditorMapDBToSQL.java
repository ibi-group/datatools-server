package com.conveyal.datatools.editor.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.editor.datastore.FeedTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.Snapshot;
import com.conveyal.datatools.editor.models.transit.ScheduleException;
import com.conveyal.datatools.editor.models.transit.ServiceCalendar;
import com.conveyal.datatools.editor.models.transit.Trip;
import com.conveyal.datatools.editor.models.transit.TripPattern;
import com.conveyal.datatools.editor.models.transit.TripPatternStop;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.loader.FeedLoadResult;
import com.conveyal.gtfs.loader.JdbcGTFSFeedConverter;
import com.conveyal.gtfs.loader.JdbcGtfsLoader;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.CalendarDate;
import org.apache.commons.dbutils.DbUtils;
import org.mapdb.Fun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collection;

import static com.conveyal.datatools.editor.jobs.ProcessGtfsSnapshotExport.toGtfsDate;
import static com.conveyal.gtfs.GTFS.createDataSource;

public class ConvertEditorMapDBToSQL extends MonitorableJob {
    private Collection<Fun.Tuple2<String, Integer>> snapshots;
    private static final Logger LOG = LoggerFactory.getLogger(ConvertEditorMapDBToSQL.class);
    private Connection connection;

    public ConvertEditorMapDBToSQL(Collection<Fun.Tuple2<String, Integer>> snapshots) {
        // FIXME owner and job name
        super("owner", "Create snapshot from legacy editor", JobType.CONVERT_EDITOR_MAPDB_TO_SQL);
        this.snapshots = snapshots;
    }

    /**
     * Import this snapshot to GTFS, using the validity range in the snapshot.
     */
    public ConvertEditorMapDBToSQL(Snapshot snapshot) {
        this(Arrays.asList(new Fun.Tuple2[]{snapshot.id}));
    }

    @Override
    public void jobLogic() {
        GTFSFeed feed;
        FeedTx feedTx;
        try {
            for (Fun.Tuple2<String, Integer> ssid : snapshots) {
                // FIXME: This needs to share a connection with the snapshotter.
                // Create connection for each snapshot
                // FIXME: use GTFS_DATA_SOURCE
                DataSource dataSource = createDataSource("jdbc:postgresql://localhost/dmtest", null, null);
                connection = dataSource.getConnection(); // DataManager.GTFS_DATA_SOURCE.getConnection();
                String feedId = ssid.a;

                // retrieveById present feed database if no snapshot version provided
                if (ssid.b == null) {
                    feedTx = VersionedDataStore.getFeedTx(feedId);
                }
                // else retrieveById snapshot version data
                else {
                    feedTx = VersionedDataStore.getFeedTx(feedId, ssid.b);
                }

                feed = feedTx.toGTFSFeed(true);

                // STEP 1: Write GTFSFeed into SQL database. There are some gaps remaining after this process wraps up:
                // - Routes doesn't have publicly_visible and status fields
                // - Patterns do not exist
                // - Pattern stops table does not exist, so it needs to be created and populated.
                // - FIXME No schedule exceptions.... ugh...
                // - Trips need pattern ID

                // FIXME Does FeedLoadResult need to be populated with more info about the load? (Currently it's just
                // namespace and load time.
                FeedLoadResult feedLoadResult = feed.toSQL(dataSource);

                String namespace = feedLoadResult.uniqueIdentifier;

                // FIXME: This needs to be done in the same transaction as the above operation.
                // Iterate over routes and update
                int batchSize = 0;
                String tableName = String.join(".", namespace, Table.ROUTES.name);
                String updateSql = String.format("update %s set status=?, publicly_visible=? where route_id = ?", tableName);
                PreparedStatement updateRouteStatment = connection.prepareStatement(updateSql);
                LOG.info("Updating status, publicly_visible for {} routes", feedTx.routes.size());
                for (com.conveyal.datatools.editor.models.transit.Route route : feedTx.routes.values()) {
                    // FIXME: Maybe it's risky to update on gtfs route ID (which may not be unique for some feeds).
                    // Could we alternatively update on ID field (not sure what the value for each route will be after
                    // insertion)?
                    updateRouteStatment.setInt(1, route.status == null ? 0 :route.status.toInt());
                    int publiclyVisible = route.publiclyVisible == null ? 0 : route.publiclyVisible ? 1 : 0;
                    updateRouteStatment.setInt(2, publiclyVisible);
                    updateRouteStatment.setString(3, route.gtfsRouteId);
                    // FIXME: Do something with the return value?  E.g., rollback if it hits more than one route.
                    // FIXME: Do this in batches?
                    updateRouteStatment.addBatch();
                    batchSize += 1;
                    batchSize = handleBatchExecution(batchSize, updateRouteStatment);
                }
                // Handle any remaining updates.
                updateRouteStatment.executeBatch();

                // Annoyingly, a number of fields on the Editor Trip class differ from the gtfs-lib Trip class (e.g.,
                // patternId and calendarId refer to the editor Model#ID field not the GTFS key field). So we first
                // convert the trips to gtfs trips and then insert them into the database. And while we're at it, we do
                // this with stop times, too.
                // OLD COMMENT: we can't use the trips-by-route index because we may be exporting a snapshot database without indices
                if (feedTx.trips != null) {
                    batchSize = 0;
                    // Update pattern_id for trips.
                    String tripsTableName = String.join(".", namespace, Table.TRIPS.name);
                    LOG.info("Updating pattern_id for {} trips", feedTx.trips.size());
                    String updateTripsSql = String.format("update %s set pattern_id=? where trip_id=?", tripsTableName);
                    PreparedStatement updateTripsStatement = connection.prepareStatement(updateTripsSql);
                    for (Trip trip : feedTx.trips.values()) {
                        TripPattern pattern = feedTx.tripPatterns.get(trip.patternId);
                        // FIXME: Should we exclude patterns from the original insert (GTFSFeed.toSQL)? These pattern IDs
                        // will not match those found in the GTFSFeed patterns. However, FeedTx.toGTFSFeed doesn't
                        // actually create patterns, so there are no patterns loaded to begin with.
                        updateTripsStatement.setString(1, pattern.id);
                        updateTripsStatement.setString(2, trip.gtfsTripId);
                        // FIXME: Do something with the return value?  E.g., rollback if it hits more than one trip.
                        // FIXME: Do this in batches?
                        updateTripsStatement.addBatch();
                        batchSize += 1;
                        // If we've accumulated a lot of prepared statement calls, pass them on to the database backend.
                        batchSize = handleBatchExecution(batchSize, updateTripsStatement);
                        // FIXME Need to cherry-pick frequency fixes made for Izmir/WRI
                    }
                    // Handle remaining updates.
                    updateTripsStatement.executeBatch();
                }

                // Pattern stops table has not yet been created because pattern stops do not exist in
                // GTFSFeed. NOte, we want this table to be created regardless of whether patterns exist or not.
                Table.PATTERN_STOP.createSqlTable(connection, namespace, true);

                // Insert all trip patterns and pattern stops into database (tables have already been created FIXME pattern_stops has not yet been created).
                if (feedTx.tripPatterns != null) {
                    batchSize = 0;
                    // Handle inserting patterns
                    PreparedStatement insertPatternStatement = connection.prepareStatement(
                            Table.PATTERNS.generateInsertSql(namespace, true));
                    // Handle inserting pattern stops
                    PreparedStatement insertPatternStopStatement = connection.prepareStatement(
                            Table.PATTERN_STOP.generateInsertSql(namespace, true));
                    LOG.info("Inserting {} patterns", feedTx.tripPatterns.size());
                    for (TripPattern pattern : feedTx.tripPatterns.values()) {
                        insertPatternStatement.setString(1, pattern.id);
                        insertPatternStatement.setString(2, pattern.routeId);
                        insertPatternStatement.setString(3, pattern.name);
                        if (pattern.patternDirection != null) {
                            insertPatternStatement.setInt(4, pattern.patternDirection.toGtfs());
                        } else {
                            insertPatternStatement.setNull(4, JDBCType.INTEGER.getVendorTypeNumber());
                        }
                        insertPatternStatement.setInt(5, pattern.useFrequency ? 1 : 0);
                        // Shape ID will match the pattern id for pattern geometries that have been converted to shapes.
                        // This process happens in FeedTx.toGTFSFeed.
                        insertPatternStatement.setString(6, pattern.id);
                        insertPatternStatement.addBatch();
                        batchSize += 1;

                        int stopSequence = 1;
                        LOG.info("Inserting {} pattern stops for pattern {}", pattern.patternStops.size(), pattern.id);
                        for (TripPatternStop tripPatternStop : pattern.patternStops) {
                            // TripPatternStop's stop ID needs to be mapped to GTFS stop ID.
                            // FIXME Possible NPE?
                            String stopId = feedTx.stops.get(tripPatternStop.stopId).gtfsStopId;
                            insertPatternStopStatement.setString(1, pattern.id);
                            insertPatternStopStatement.setInt(2, stopSequence);
                            insertPatternStopStatement.setString(3, stopId);
                            insertPatternStopStatement.setInt(4, tripPatternStop.defaultTravelTime);
                            insertPatternStopStatement.setInt(5, tripPatternStop.defaultDwellTime);
                            insertPatternStopStatement.setInt(6, 0);
                            insertPatternStopStatement.setInt(7, 0);
                            // FIXME: shapeDistTraveled could be null
                            insertPatternStopStatement.setDouble(8, tripPatternStop.shapeDistTraveled);
                            insertPatternStopStatement.setInt(9, tripPatternStop.timepoint ? 1 : 0);
                            insertPatternStopStatement.addBatch();
                            batchSize += 1;
                            // If we've accumulated a lot of prepared statement calls, pass them on to the database backend.
                            batchSize = handleBatchExecution(batchSize, insertPatternStatement, insertPatternStopStatement);
                        }
                        // Handle remaining updates.
                        insertPatternStatement.executeBatch();
                        insertPatternStopStatement.executeBatch();
                    }
                }


                LOG.warn("Schedule exceptions need to be imported!!!!");
                // FIXME: Handle calendars/service exceptions....
//                if (feedTx.calendars != null) {
//                    for (ServiceCalendar cal : feedTx.calendars.values()) {
//
//                        int start = toGtfsDate(cal.startDate);
//                        int end = toGtfsDate(cal.endDate);
//                        com.conveyal.gtfs.model.Service gtfsService = cal.toGtfs(start, end);
//                        // note: not using user-specified IDs
//
//                        // add calendar dates
//                        if (feedTx.exceptions != null) {
//                            for (ScheduleException ex : feedTx.exceptions.values()) {
//                                if (ex.equals(ScheduleException.ExemplarServiceDescriptor.SWAP) && !ex.addedService.contains(cal.id) && !ex.removedService.contains(cal.id))
//                                    // skip swap exception if cal is not referenced by added or removed service
//                                    // this is not technically necessary, but the output is cleaner/more intelligible
//                                    continue;
//
//                                for (LocalDate date : ex.dates) {
//                                    if (date.isBefore(cal.startDate) || date.isAfter(cal.endDate))
//                                        // no need to write dates that do not apply
//                                        continue;
//
//                                    CalendarDate cd = new CalendarDate();
//                                    cd.date = date;
//                                    cd.service_id = gtfsService.service_id;
//                                    cd.exception_type = ex.serviceRunsOn(cal) ? 1 : 2;
//
//                                    if (gtfsService.calendar_dates.containsKey(date))
//                                        throw new IllegalArgumentException("Duplicate schedule exceptions on " + date.toString());
//
//                                    gtfsService.calendar_dates.put(date, cd);
//                                }
//                            }
//                        }
//                    }
//                }


            }
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                connection.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }

    private int handleBatchExecution(int batchSize, PreparedStatement ... preparedStatements) throws SQLException {
        if (batchSize > JdbcGtfsLoader.INSERT_BATCH_SIZE) {
            for (PreparedStatement statement : preparedStatements) {
                statement.executeBatch();
            }
            return 0;
        } else {
            return batchSize;
        }
    }
}