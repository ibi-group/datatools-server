package com.conveyal.datatools.manager.models.transform;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NormalizeFieldTransformationTest {
    @Test
    // FIXME: Convert to parametrized test
    public void testConvertToTitleCase() {
        String[][] cases = new String[][] {
            new String[] {null, ""},
            new String[] {"LATHROP/MANTECA STATION", "Lathrop/Manteca Station"},
            new String[] {"12TH@WEST", "12th@West"},
            new String[] {"12TH+WEST", "12th+West"},
            new String[] {"12TH&WEST", "12th&West"},
            new String[] {"904 OCEANA BLVD-GOOD SHEPHERD SCHOOL", "904 Oceana Blvd-Good Shepherd School"}
        };

        for (String[] c : cases) {
            String expected = c[1];
            String input = c[0];
            assertEquals(expected, NormalizeFieldTransformation.convertToTitleCase(input));
        }
    }
}
