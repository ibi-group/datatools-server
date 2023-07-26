package com.conveyal.datatools.manager.jobs.validation;

import com.conveyal.datatools.manager.models.Project;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Route;
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

        for (Route route : feed.routes) {
            registerError(route, NewGTFSErrorType.MULTIPLE_SHARED_STOPS_GROUPS);
        }

    }
}
