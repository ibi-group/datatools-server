package com.conveyal.datatools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class TestUtils {
    public static boolean getBooleanEnvVar (String var) {
        String CI = System.getenv(var);
        return CI != null && CI.equals("true");
    }

    public static boolean isCi () {
        return getBooleanEnvVar("CI");
    }

    /**
     * Parse a json string into an unmapped JsonNode object
     */
    public static JsonNode parseJson(String jsonString) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(jsonString);
    }
}
