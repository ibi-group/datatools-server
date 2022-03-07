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

    /** No public constructor. */
    private RouteTypeValidatorBuilder() { }

    /**
     * Builds a list of configured route types from gtfs.yaml.
     */
    static List<Integer> getConfiguredRouteTypes() {
        List<Integer> options = new ArrayList<>();

        // Find the node that describes the route fields.
        JsonNode routeFieldsNode = getNode(DataManager.gtfsConfig, "id", "route", "fields");
        if (routeFieldsNode != null) {
            // Find the node that describes the route type options.
            JsonNode routeTypeOptionsNode = getNode(routeFieldsNode, "name", "route_type", "options");
            if (routeTypeOptionsNode != null) {
                // Collect the values in the child nodes.
                for (JsonNode option : routeTypeOptionsNode) {
                    JsonNode valueNode = option.get("value");
                    if (valueNode != null) {
                        options.add(valueNode.intValue());
                    }
                }
            }
        }
        return options;
    }

    /**
     * Helper method to get a node with the given key and value.
     */
    private static JsonNode getNode(JsonNode parentNode, String key, String keyValue, String targetKey) {
        for (JsonNode entry : parentNode) {
            JsonNode keyNode = entry.get(key);
            if (keyNode != null && keyNode.asText().equals(keyValue)) {
                return entry.get(targetKey);
            }
        }
        return null;
    }
}
