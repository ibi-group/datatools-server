package com.conveyal.datatools.manager.extensions;

import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;

/**
 * Created by demory on 3/30/16.
 */
public interface ExternalFeedResource {

    public String getResourceType();

    public void importFeedsForProject(Project project);

    public void propertyUpdated(ExternalFeedSourceProperty property);

    public void feedVersionUpdated(FeedVersion feedVersion);
}
