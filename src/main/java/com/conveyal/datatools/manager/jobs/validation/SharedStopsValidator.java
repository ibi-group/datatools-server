package com.conveyal.datatools.manager.jobs.validation;

import com.conveyal.datatools.manager.models.Project;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.validator.FeedValidator;
import com.csvreader.CsvReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class SharedStopsValidator extends FeedValidator {
    Feed feed;
    Project project;

    public SharedStopsValidator(Project project) {
        super(null, null);
        this.project = project;
    }

    // This method can only be run on a SharedStopsValidator that has been set up with a project only
    public SharedStopsValidator buildSharedStopsValidator(Feed feed, SQLErrorStorage errorStorage) {
        if (this.project == null) {
            throw new RuntimeException("buildSharedStopsValidator can only be called with a project already set!");
        }
        return new SharedStopsValidator(feed, errorStorage, this.project);
    }
    public SharedStopsValidator(Feed feed, SQLErrorStorage errorStorage, Project project) {
        super(feed, errorStorage);
        this.feed = feed;
        this.project = project;
    }

    @Override
    public void validate() {
        String config = project.sharedStopsConfig;
        if (config == null) {
            return;
        }

        CsvReader configReader = CsvReader.parse(config);

        int STOP_GROUP_ID_INDEX = 0;
        int STOP_ID_INDEX = 2;
        int IS_PRIMARY_INDEX = 3;

        // Build list of stop Ids.
        List<String> stopIds = new ArrayList<>();
        List<Stop> stops = new ArrayList<>();

        for (Stop stop : feed.stops) {
            stops.add(stop);
            stopIds.add(stop.stop_id);
        }

        // Initialize hashmaps to hold duplication info
        HashMap<String, Set<String>> stopGroupStops = new HashMap<>();
        HashSet<String> stopGroupsWithPrimaryStops = new HashSet<>();

        try {
            while (configReader.readRecord()) {
                String stopGroupId = configReader.get(STOP_GROUP_ID_INDEX);
                String stopId = configReader.get(STOP_ID_INDEX);

                if (stopId.equals("stop_id")) {
                    // Swallow header row
                    continue;
                }

                if (!stopGroupStops.containsKey(stopGroupId)) {
                    stopGroupStops.put(stopGroupId, new HashSet<>());
                }
                Set<String> seenStopIds = stopGroupStops.get(stopGroupId);

                // Check for SS_01 (stop id appearing in multiple stop groups)
                if (seenStopIds.contains(stopId)) {
                    registerError(stops.stream().filter(stop -> stop.stop_id.equals(stopId)).findFirst().orElse(stops.get(0)), NewGTFSErrorType.MULTIPLE_SHARED_STOPS_GROUPS);
                } else {
                    seenStopIds.add(stopId);
                }

                // Check for SS_02 (multiple primary stops per stop group)
                if (stopGroupsWithPrimaryStops.contains(stopGroupId)) {
                    registerError(NewGTFSError.forFeed(NewGTFSErrorType.SHARED_STOP_GROUP_MUTLIPLE_PRIMARY_STOPS, stopGroupId));
                } else if (configReader.get(IS_PRIMARY_INDEX).equals("true")) {
                    stopGroupsWithPrimaryStops.add(stopGroupId);
                }

                // Check for SS_03 (stop_id referenced doesn't exist)
                // TODO: CHECK FEED ID
                if (!stopIds.contains(stopId)) {
                    registerError(NewGTFSError.forFeed(NewGTFSErrorType.SHARED_STOP_GROUP_ENTITY_DOES_NOT_EXIST, stopId));
                }
            }
        } catch (IOException e) {
            // TODO Fail gracefully
            throw new RuntimeException(e);
        }


    }
}
