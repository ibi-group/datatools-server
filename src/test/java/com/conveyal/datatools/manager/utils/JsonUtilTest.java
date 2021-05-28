package com.conveyal.datatools.manager.utils;

import com.conveyal.datatools.manager.models.transform.Substitution;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test class for JsonUtil.
 * We use, as an illustration, {@link Substitution} as the target class for JSON parsing.
 */
public class JsonUtilTest {
    final static String JSON_LIST = "[{ \"pattern\": \"abc\" }, {\"pattern\": \"def\"}]";

    /**
     * Ensure {@link JsonUtil#getPOJOFromJSONAsList} returns an empty list
     * if the given jsonNode input is null.
    */
    @Test
    public void testGetPOJOFromNullJSONIsNotNull() {

        List<Substitution> list = JsonUtil.getPOJOFromJSONAsList(
            null,
            Substitution.class
        );
        assertNotNull(list);
        assertEquals(0, list.size());
    }

    /**
     * Ensure {@link JsonUtil#getPOJOFromJSONAsList} populates a list from JSON.
     */
    @Test
    public void testGetPOJOFromJSONAsListIsPopulated() throws JsonProcessingException {
        List<Substitution> list = JsonUtil.getPOJOFromJSONAsList(
            new ObjectMapper().readTree(JSON_LIST),
            Substitution.class
        );
        assertNotNull(list);
        assertEquals(2, list.size());
        assertEquals("abc", list.get(0).pattern);
        assertEquals("def", list.get(1).pattern);
    }
}
