package com.conveyal.datatools.manager.jobs.validation;

import com.conveyal.datatools.manager.models.Project;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.validator.FeedValidator;
import com.csvreader.CsvReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;


public class SharedStopsValidator extends FeedValidator {
    private static final Logger LOG = LoggerFactory.getLogger(SharedStopsValidator.class);

    Feed feed;
    String feedId;
    Project project;

    public SharedStopsValidator(Project project, String feedId) {
        super(null, null);
        this.project = project;
        this.feedId = feedId;
    }

    // This method can only be run on a SharedStopsValidator that has been set up with a project only
    public SharedStopsValidator buildSharedStopsValidator(Feed feed, SQLErrorStorage errorStorage) {
        if (this.project == null) {
            throw new RuntimeException("Shared stops validator can not be called because no project has been set!");
        }
        if (this.feedId == null) {
            throw new RuntimeException("Shared stops validator can not be called because no feed ID has been set!");
        }
        return new SharedStopsValidator(feed, errorStorage, this.project, this.feedId);
    }
    public SharedStopsValidator(Feed feed, SQLErrorStorage errorStorage, Project project, String feedId) {
        super(feed, errorStorage);
        this.feed = feed;
        this.project = project;
        this.feedId = feedId;
    }

    @Override
    public void validate() {
        String config = project.sharedStopsConfig;
        if (config == null) {
            return;
        }

        CsvReader configReader = CsvReader.parse(config);

        int STOP_GROUP_ID_INDEX = -1;
        int STOP_ID_INDEX = -1;
        int IS_PRIMARY_INDEX = -1;
        int FEED_ID_INDEX = -1;

        try {
            configReader.setHeaders(new String[]{"stop_group_id", "feed_id", "stop_id", "is_primary"});
            String[] headers = configReader.getHeaders();
            for (int i = 0; i < headers.length; i++) {
                String header = headers[i];
                switch(header) {
                    case "stop_group_id":
                        STOP_GROUP_ID_INDEX = i;
                        break;
                    case "feed_id":
                        FEED_ID_INDEX = i;
                        break;
                    case "stop_id":
                        STOP_ID_INDEX = i;
                        break;
                    case "is_primary":
                        IS_PRIMARY_INDEX = i;
                        break;
                    default:
                        throw new RuntimeException("shared_stops.csv contained invalid headers!");
                }
            }

            if (STOP_GROUP_ID_INDEX == -1 || FEED_ID_INDEX == -1 || STOP_ID_INDEX == -1 || IS_PRIMARY_INDEX == -1) {
                throw new RuntimeException("shared_stops.csv is missing headers!");
            }
        } catch (IOException e) {
            throw new RuntimeException("shared_stops.csv was invalid! " + e.toString());
        }

        // Build list of stop Ids.
        List<String> stopIds = new ArrayList<>();
        List<Stop> stops = new ArrayList<>();

        for (Stop stop : feed.stops) {
            stops.add(stop);
            stopIds.add(stop.stop_id);
        }

        // Initialize hashmaps to hold duplication info
        HashSet<String> seenStopIds = new HashSet<>();
        HashSet<String> stopGroupsWithPrimaryStops = new HashSet<>();

        try {
            while (configReader.readRecord()) {
                String stopGroupId = configReader.get(STOP_GROUP_ID_INDEX);
                String stopId = configReader.get(STOP_ID_INDEX);
                String sharedStopFeedId = configReader.get(FEED_ID_INDEX);

                if (stopId.equals("stop_id")) {
                    // Swallow header row
                    continue;
                }

                // Check for SS_01 (stop id appearing in multiple stop groups)
                // Make sure this error is only returned if we are inside the feed that is being checked
                if (seenStopIds.contains(stopId)) {
                    if (this.feedId.equals(sharedStopFeedId)) {
                        registerError(stops
                                .stream()
                                .filter(stop -> stop.stop_id.equals(stopId))
                                .findFirst()
                                .orElse(new Stop()), NewGTFSErrorType.MULTIPLE_SHARED_STOPS_GROUPS
                        );
                    }
                } else {
                    seenStopIds.add(stopId);
                }

                // Check for SS_02 (multiple primary stops per stop group)
                if (configReader.get(IS_PRIMARY_INDEX).equals("1") || configReader.get(IS_PRIMARY_INDEX).equals("true")) {
                    if (stopGroupsWithPrimaryStops.contains(stopGroupId)) {
                        registerError(NewGTFSError.forFeed(NewGTFSErrorType.SHARED_STOP_GROUP_MUTLIPLE_PRIMARY_STOPS, stopGroupId));
                    } else {
                        stopGroupsWithPrimaryStops.add(stopGroupId);
                    }
                }

                // Check for SS_03 (stop_id referenced doesn't exist)
                // Make sure this error is only returned if we are inside the feed that is being checked
                if (feedId.equals(sharedStopFeedId) && !stopIds.contains(stopId)) {
                    registerError(NewGTFSError.forFeed(NewGTFSErrorType.SHARED_STOP_GROUP_ENTITY_DOES_NOT_EXIST, stopId));
                }
            }
        } catch (IOException e) { LOG.error(e.toString()); }
        finally {
            configReader.close();
        }


    }
}
