package com.conveyal.datatools.manager.utils.json;

import com.conveyal.datatools.manager.utils.SimpleHttpResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.type.CollectionType;
import spark.Request;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.conveyal.datatools.common.utils.SparkUtils.getObjectNode;
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

    public static <T> T getPOJOFromResponse(SimpleHttpResponse response, Class<T> clazz) throws IOException {
        return objectMapper.readValue(response.body, clazz);
    }

    public static JsonNode getJsonNodeFromResponse(SimpleHttpResponse response) throws IOException {
        return objectMapper.readTree(response.body);
    }

    /**
     * Utility method to parse generic objects from {@link JsonNode} and return as list
     * (or at least an empty list).
     */
    public static <T> List<T> getPOJOFromJSONAsList(JsonNode json, Class<T> clazz) {
        if (json != null) {
            CollectionType type = objectMapper.getTypeFactory().constructCollectionType(List.class, clazz);
            ObjectReader reader = objectMapper.readerFor(type);
            try {
                return reader.readValue(json);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return new ArrayList<>();
    }

    /**
     * Serialize an object into a JSON string representation.
     */
    public static String toJson(Object object) {

        String json;
        try {
            json = objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            json = formatJSON(String.format("Unable to serialize object: %s.", object.toString()), 500, e);
        }

        return json;
    }

    /**
     * Constructs a JSON string with a result (i.e., OK or ERR), message, code, and if the exception argument is
     * supplied details about the exception encountered.
     */
    public static String formatJSON(String message, int code, Exception e) {
        return getObjectNode(message, code, e).toString();
    }

}
