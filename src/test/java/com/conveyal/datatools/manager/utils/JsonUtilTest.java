package com.conveyal.datatools.manager.utils;

import com.conveyal.datatools.manager.models.transform.Substitution;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class JsonUtilTest {
    final static String JSON_LIST = "[{ \"pattern\": \"abc\" }, {\"pattern\": \"def\"}]";

    /**
     * Ensure {@link JsonUtil#getPOJOFromJSONAsList} returns at least an empty list
     * for the given jsonNode input.
    */
    @ParameterizedTest
    @MethodSource("createPOJOFromJSONCases")
    public void testGetPOJOFromJSONAsListIsNotEmpty(JsonNode jsonNode) {
        assertNotNull(JsonUtil.getPOJOFromJSONAsList(
            jsonNode,
            Substitution.class
        ));
    }

    private static Stream<Arguments> createPOJOFromJSONCases() throws JsonProcessingException {
        return Stream.of(
            // Handle cases where JSON config data is missing.
            Arguments.of((Object) null),
            // Normal cases where JSON config data is populated.
            Arguments.of(new ObjectMapper().readTree(JSON_LIST))
        );
    }
}
