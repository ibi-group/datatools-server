package com.conveyal.datatools.manager.extensions;

import com.conveyal.datatools.common.utils.aws.CheckedAWSException;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;

import java.io.IOException;

/**
 * Created by demory on 3/30/16.
 */
public interface ExternalFeedResource {

    public String getResourceType();

    public void importFeedsForProject(Project project, String authHeader) throws Exception;

    public void feedSourceCreated(FeedSource source, String authHeader) throws Exception;

    public void propertyUpdated(ExternalFeedSourceProperty property, String previousValue, String authHeader) throws IOException;

    public void feedVersionCreated(FeedVersion feedVersion, String authHeader) throws CheckedAWSException;
}
