package com.conveyal.datatools.manager.jobs.validation;

import com.conveyal.datatools.manager.models.Project;
import com.conveyal.gtfs.error.GTFSError;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.Entity;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.validator.FeedValidator;


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


        for (Stop stop : feed.stops) {
            registerError(stop, NewGTFSErrorType.MULTIPLE_SHARED_STOPS_GROUPS);
        }
        if (config == null) {
            registerError(NewGTFSError.forFeed(NewGTFSErrorType.SHARED_STOP_GROUP_ENTITY_DOES_NOT_EXIST, "uh oh"));
            registerError(NewGTFSError.forFeed(NewGTFSErrorType.SHARED_STOP_GROUP_MUTLIPLE_PRIMARY_STOPS, "good god"));

        }

    }
}
