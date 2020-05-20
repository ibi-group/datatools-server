package com.conveyal.datatools.editor.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.editor.datastore.FeedTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.transit.Route;
import com.conveyal.datatools.editor.models.transit.ScheduleException;
import com.conveyal.datatools.editor.models.transit.ServiceCalendar;
import com.conveyal.datatools.editor.models.transit.Trip;
import com.conveyal.datatools.editor.models.transit.TripPattern;
import com.conveyal.datatools.editor.models.transit.TripPatternStop;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.Snapshot;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.loader.FeedLoadResult;
import com.conveyal.gtfs.loader.JdbcGtfsLoader;
import com.conveyal.gtfs.loader.Table;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Array;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import static com.conveyal.gtfs.loader.DateField.GTFS_DATE_FORMATTER;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

public class ConvertEditorMapDBToSQL extends MonitorableJob {
    private final String feedId;
    private final Integer versionNumber;
    private static final Logger LOG = LoggerFactory.getLogger(ConvertEditorMapDBToSQL.class);
    private Connection connection;
    private DataSource dataSource;

    public ConvertEditorMapDBToSQL(String feedId, Integer versionNumber) {
        // FIXME owner and job name
        super(Auth0UserProfile.createSystemUser(), "Create snapshot from legacy editor", JobType.CONVERT_EDITOR_MAPDB_TO_SQL);
        this.feedId = feedId;
        this.versionNumber = versionNumber;
    }

    @Override
    public void jobLogic() {
        try {
            // Iterate over the provided snapshots and convert each one. Note: this will skip snapshots for feed IDs that
            // don't exist as feed sources in MongoDB.
            FeedSource feedSource = Persistence.feedSources.getById(feedId);
            if (feedSource == null) {
                LOG.warn("Not converting snapshot. Feed source Id {} does not exist in application data", feedId);
                return;
            }
            Snapshot matchingSnapshot = Persistence.snapshots.getOneFiltered(
                and(
                    eq("version", versionNumber),
                    eq(Snapshot.FEED_SOURCE_REF, feedId)
                )
            );
            boolean snapshotExists = true;
            if (matchingSnapshot == null) {
                snapshotExists = false;
                matchingSnapshot = new Snapshot("Imported", feedId, "mapdb_editor");
            }
            FeedTx feedTx;
            // FIXME: This needs to share a connection with the snapshotter.
            // Create connection for each snapshot
            // FIXME: use GTFS_DATA_SOURCE
            dataSource = DataManager.GTFS_DATA_SOURCE;
            connection = dataSource.getConnection(); // DataManager.GTFS_DATA_SOURCE.getConnection();

            // retrieveById present feed database if no snapshot version provided
            boolean setEditorBuffer = false;
            if (versionNumber == null) {
                setEditorBuffer = true;
                feedTx = VersionedDataStore.getFeedTx(feedId);
            }
            // else retrieveById snapshot version data
            else {
                feedTx = VersionedDataStore.getFeedTx(feedId, versionNumber);
            }

            LOG.info("Converting {}.{} to SQL", feedId, versionNumber);
            // Convert mapdb to SQL
            FeedLoadResult convertFeedResult = convertFeed(feedId, versionNumber, feedTx);
            // Update manager snapshot with result details.
            matchingSnapshot.snapshotOf = "mapdb_editor";
            matchingSnapshot.namespace = convertFeedResult.uniqueIdentifier;
            matchingSnapshot.feedLoadResult = convertFeedResult;
            LOG.info("Storing snapshot {}", matchingSnapshot.id);
            if (snapshotExists) Persistence.snapshots.replace(matchingSnapshot.id, matchingSnapshot);
            else Persistence.snapshots.create(matchingSnapshot);
            if (setEditorBuffer) {
                // If there is no version, that indicates that this was from the editor buffer for that feedId.
                // Make this snapshot the editor namespace buffer.
                LOG.info("Updating active snapshot to {}", matchingSnapshot.id);
                FeedSource updatedFeedSource = Persistence.feedSources.updateField(
                        feedSource.id, "editorNamespace", matchingSnapshot.namespace);
                LOG.info("Editor namespace: {}", updatedFeedSource.editorNamespace);
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

    /**
     * Convert a single MapDB Editor feed (snapshot or no) to a SQL-backed snapshot.
     */
    private FeedLoadResult convertFeed(String feedId, Integer version, FeedTx feedTx) throws SQLException {
        GTFSFeed feed;

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
        if (feedLoadResult.fatalException != null) {
            throw new SQLException(String.format("Fatal exception converting %s.%d to SQL", feedId, version));
        }
        String namespace = feedLoadResult.uniqueIdentifier;

        // FIXME: This needs to be done in the same transaction as the above operation.
        // Iterate over routes and update
        int batchSize = 0;
        String tableName = String.join(".", namespace, Table.ROUTES.name);
        String updateSql = String.format("update %s set status=?, publicly_visible=? where route_id = ?", tableName);
        PreparedStatement updateRouteStatement = connection.prepareStatement(updateSql);
        if (feedTx.routes != null) {
            LOG.info("Updating status, publicly_visible for {} routes", feedTx.routes.size()); // FIXME NPE if (feedTx.routes != null)
            for (com.conveyal.datatools.editor.models.transit.Route route : feedTx.routes.values()) {
                // FIXME: Maybe it's risky to update on gtfs route ID (which may not be unique for some feeds).
                // Could we alternatively update on ID field (not sure what the value for each route will be after
                // insertion)?
                updateRouteStatement.setInt(1, route.status == null ? 0 : route.status.toInt());
                int publiclyVisible = route.publiclyVisible == null ? 0 : route.publiclyVisible ? 1 : 0;
                updateRouteStatement.setInt(2, publiclyVisible);
                updateRouteStatement.setString(3, route.gtfsRouteId);
                // FIXME: Do something with the return value?  E.g., rollback if it hits more than one route.
                // FIXME: Do this in batches?
                updateRouteStatement.addBatch();
                batchSize += 1;
                batchSize = handleBatchExecution(batchSize, updateRouteStatement);
            }
            // Handle any remaining updates.
            updateRouteStatement.executeBatch();
        } else {
            LOG.warn("Skipping routes conversion (feedTx.routes is null)");
        }

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
        // GTFSFeed. Note, we want this table to be created regardless of whether patterns exist or not
        // (which is why it is outside of the check for null pattern map).
        Table.PATTERN_STOP.createSqlTable(connection, namespace, true);

        // Insert all trip patterns and pattern stops into database (tables have already been created).
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
                Route route = feedTx.routes.get(pattern.routeId);
                insertPatternStatement.setString(1, pattern.id);
                insertPatternStatement.setString(2, route.gtfsRouteId);
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
                // stop_sequence must be zero-based and incrementing to match stop_times values.
                int stopSequence = 0;
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
                    if (tripPatternStop.shapeDistTraveled == null) {
                        insertPatternStopStatement.setNull(8, JDBCType.DOUBLE.getVendorTypeNumber());
                    } else {
                        insertPatternStopStatement.setDouble(8, tripPatternStop.shapeDistTraveled);
                    }
                    if (tripPatternStop.timepoint == null) {
                        insertPatternStopStatement.setNull(9, JDBCType.INTEGER.getVendorTypeNumber());
                    } else {
                        insertPatternStopStatement.setInt(9, tripPatternStop.timepoint ? 1 : 0);
                    }
                    insertPatternStopStatement.addBatch();
                    batchSize += 1;
                    stopSequence += 1;
                    // If we've accumulated a lot of prepared statement calls, pass them on to the database backend.
                    batchSize = handleBatchExecution(batchSize, insertPatternStatement, insertPatternStopStatement);
                }
                // Handle remaining updates.
                insertPatternStatement.executeBatch();
                insertPatternStopStatement.executeBatch();
            }
        }


        // FIXME: Handle calendars/service exceptions....
        // Add service calendars FIXME: delete calendars already in the table?
        if (feedTx.calendars != null) {
            // Handle inserting pattern stops
            PreparedStatement insertCalendar = connection.prepareStatement(
                    Table.CALENDAR.generateInsertSql(namespace, true));
            batchSize = 0;
            LOG.info("Inserting {} calendars", feedTx.calendars.size());
            for (ServiceCalendar cal : feedTx.calendars.values()) {
                insertCalendar.setString(1, cal.gtfsServiceId);
                insertCalendar.setInt(2, cal.monday ? 1 : 0);
                insertCalendar.setInt(3, cal.tuesday ? 1 : 0);
                insertCalendar.setInt(4, cal.wednesday ? 1 : 0);
                insertCalendar.setInt(5, cal.thursday ? 1 : 0);
                insertCalendar.setInt(6, cal.friday ? 1 : 0);
                insertCalendar.setInt(7, cal.saturday ? 1 : 0);
                insertCalendar.setInt(8, cal.sunday ? 1 : 0);
                insertCalendar.setString(9, cal.startDate != null ? cal.startDate.format(GTFS_DATE_FORMATTER) : null);
                insertCalendar.setString(10, cal.endDate != null ? cal.endDate.format(GTFS_DATE_FORMATTER) : null);
                insertCalendar.setString(11, cal.description);

                insertCalendar.addBatch();
                batchSize += 1;
                // If we've accumulated a lot of prepared statement calls, pass them on to the database backend.
                batchSize = handleBatchExecution(batchSize, insertCalendar);
            }
            // Handle remaining updates.
            insertCalendar.executeBatch();
        }

        // Create schedule exceptions table.
        Table.SCHEDULE_EXCEPTIONS.createSqlTable(connection, namespace, true);

        // Add schedule exceptions (Note: calendar dates may be carried over from GTFSFeed.toSql, but these will
        // ultimately be overwritten by schedule exceptions during Editor feed export.
        if (feedTx.exceptions != null) {
            batchSize = 0;
            PreparedStatement insertException = connection.prepareStatement(Table.SCHEDULE_EXCEPTIONS.generateInsertSql(namespace, true));
            LOG.info("Inserting {} schedule exceptions", feedTx.exceptions.size());
            for (ScheduleException ex : feedTx.exceptions.values()) {
                String[] dates = ex.dates != null
                        ? ex.dates.stream()
                        .map(localDate -> localDate.format(GTFS_DATE_FORMATTER))
                        .toArray(String[]::new)
                        : new String[]{};
                Array datesArray = connection.createArrayOf("text", dates);
                Array customArray = connection.createArrayOf("text", ex.customSchedule != null
                        ? ex.customSchedule.toArray(new String[0])
                        : new String[]{});
                Array addedArray = connection.createArrayOf("text", ex.addedService != null
                        ? ex.addedService.toArray(new String[0])
                        : new String[]{});
                Array removedArray = connection.createArrayOf("text", ex.removedService != null
                        ? ex.removedService.toArray(new String[0])
                        : new String[]{});
                insertException.setString(1, ex.name);
                insertException.setArray(2, datesArray);
                insertException.setInt(3, ex.exemplar.toInt());
                insertException.setArray(4, customArray);
                insertException.setArray(5, addedArray);
                insertException.setArray(6, removedArray);

                insertException.addBatch();
                batchSize += 1;
                // If we've accumulated a lot of prepared statement calls, pass them on to the database backend.
                batchSize = handleBatchExecution(batchSize, insertException);
            }

            // Handle remaining updates.
            insertException.executeBatch();
        }
        return feedLoadResult;
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
