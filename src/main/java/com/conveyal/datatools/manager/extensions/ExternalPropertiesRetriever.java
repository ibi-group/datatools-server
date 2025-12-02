package com.conveyal.datatools.manager.extensions;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.persistence.Persistence;

import java.util.HashMap;
import java.util.Map;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

/**
 * Class for retrieving external properties for a feed source.
 */
public class ExternalPropertiesRetriever {
    public Map<String, Map<String, String>> retrieveFeedSourceExternalProperties(String feedSourceId) {

        Map<String, Map<String, String>> resourceTable = new HashMap<>();

        for(String resourceType : DataManager.feedResources.keySet()) {
            Map<String, String> propTable = new HashMap<>();

            // Get all external properties for the feed source/resource type and fill prop table.
            Persistence.externalFeedSourceProperties
                .getFiltered(and(eq("feedSourceId", feedSourceId), eq("resourceType", resourceType)))
                .forEach(prop -> propTable.put(prop.name, prop.value));

            resourceTable.put(resourceType, propTable);
        }
        return resourceTable;
    }
}
