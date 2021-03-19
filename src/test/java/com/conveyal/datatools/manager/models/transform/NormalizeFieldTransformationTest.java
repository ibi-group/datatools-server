package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NormalizeFieldTransformationTest extends UnitTest {
    @BeforeAll
    public static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();
    }

    @Test
    // FIXME[JUnit5]: Convert to parametrized test
    public void testConvertToTitleCase() {
        String[][] cases = new String[][] {
            new String[] {null, ""},
            new String[] {"LATHROP/MANTECA STATION", "Lathrop/Manteca Station"},
            new String[] {"12TH@WEST", "12th@West"},
            new String[] {"12TH+WEST", "12th+West"},
            new String[] {"12TH&WEST", "12th&West"},
            new String[] {"904 OCEANA BLVD-GOOD SHEPHERD SCHOOL", "904 Oceana Blvd-Good Shepherd School"},
            // Capitalization exceptions (found on MTC Livermore Route 1)
            new String[] {"EAST DUBLIN BART STATION", "East Dublin BART Station"},
            new String[] {"DUBLIN & ARNOLD EB", "Dublin & Arnold EB"}
        };

        for (String[] c : cases) {
            String expected = c[1];
            String input = c[0];
            assertEquals(expected, NormalizeFieldTransformation.convertToTitleCase(input));
        }
    }

    @Test
    // FIXME[JUnit5]: Convert to parametrized test
    public void testPerformReplacements() {
        String[][] cases = new String[][] {
            // Replace "@"
            new String[] {"12TH@WEST", "12TH at WEST"},
            new String[] {"12TH  @   WEST", "12TH at WEST"},
            // Replace "+"
            new String[] {"12TH+WEST", "12TH and WEST"},
            new String[] {"12TH  +   WEST", "12TH and WEST"},
            // Replace "&"
            new String[] {"12TH&WEST", "12TH and WEST"},
            new String[] {"12TH  &   WEST", "12TH and WEST"},
            // Replace contents in parentheses and surrounding whitespace.
            new String[] {"14th St & Broadway (12th St BART) ", "14th St and Broadway"}
        };

        for (String[] c : cases) {
            String expected = c[1];
            String input = c[0];
            assertEquals(expected, NormalizeFieldTransformation.performReplacements(input));
        }
    }
}
