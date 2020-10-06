package com.conveyal.datatools.manager.utils.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import spark.Request;

import java.io.IOException;
import java.util.List;

import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;

/**
 * JSON utilities to aid with JSON parsing.
 */
public class JsonUtil {
    public static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Safely parse the request body into a JsonNode.
     * @param req The initiating request that came into datatools-server
     */
    public static JsonNode parseJsonFromBody(Request req) {
        return parseJsonFromString(req, req.body());
    }

    public static JsonNode parseJsonFromString(Request req, String json) {
        try {
            return objectMapper.readTree(json);
        } catch (IOException e) {
            logMessageAndHalt(req, 400, "Failed to parse request body", e);
            return null;
        }
    }

    /**
     * Utility method to parse generic objects from JSON String and return as list.
     * @param req The initiating request that came into datatools-server
     * @param json The string representation of JSON to be parsed.
     * @param clazz The class used to map each JSON object extracted.
     */
    public static <T> List<T> getPOJOFromJSONAsList(Request req, String json, Class<T> clazz) {
        try {
            JavaType type = objectMapper.getTypeFactory().constructCollectionType(List.class, clazz);
            return objectMapper.readValue(json, type);
        } catch (JsonProcessingException e) {
            logMessageAndHalt(req, 400, "Failed to parse JSON String", e);
            return null;
        }
    }
}
