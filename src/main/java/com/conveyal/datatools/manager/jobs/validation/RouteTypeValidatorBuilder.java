package com.conveyal.datatools.manager.jobs.validation;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.validator.RouteTypeValidator;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Validates GTFS standard and extended route types that are defined in gtfs.yml.
 */
public class RouteTypeValidatorBuilder {
    private static final List<Integer> CONFIGURED_ROUTE_TYPES = getConfiguredRouteTypes();

    public static RouteTypeValidator buildRouteValidator(Feed feed, SQLErrorStorage errorStorage) {
        return new RouteTypeValidator(feed, errorStorage, CONFIGURED_ROUTE_TYPES);
    }

    /** No public contructor */
    private RouteTypeValidatorBuilder() { }

    /**
     * Builds a list of configured route types from gtfs.yaml.
     */
    private static List<Integer> getConfiguredRouteTypes() {
        List<Integer> options = new ArrayList<>();

        // Find the routes.txt node
        for (JsonNode entry : DataManager.gtfsConfig) {
            JsonNode idNode = entry.get("id");
            if (idNode != null && idNode.asText().equals("route")) {
                // Find the route types node
                JsonNode fieldsNode = entry.get("fields");
                if (fieldsNode != null) {
                    for (JsonNode fileField : fieldsNode) {
                        JsonNode nameNode = fileField.get("name");
                        if (nameNode != null && nameNode.asText().equals("route_type")) {
                            // Collect all options
                            JsonNode optionsNode = fileField.get("options");
                            if (optionsNode != null) {
                                for (JsonNode option : optionsNode) {
                                    JsonNode valueNode = option.get("value");
                                    if (valueNode != null) {
                                        options.add(valueNode.intValue());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return options;
    }
}
