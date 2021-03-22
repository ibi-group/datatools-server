package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NormalizeFieldTransformationTest extends UnitTest {
    @BeforeAll
    public static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();
    }

    @ParameterizedTest
    @MethodSource("createCapitalizationCases")
    public void testConvertToTitleCase(String input, String expected) {
        NormalizeFieldTransformation transform = NormalizeFieldTransformation.create(
            "table", "field", null);
        assertEquals(expected, transform.convertToTitleCase(input));
    }

    private static Stream<Arguments> createCapitalizationCases() {
        return Stream.of(
            Arguments.of(null, ""),
            Arguments.of("LATHROP/MANTECA STATION", "Lathrop/Manteca Station"),
            Arguments.of("12TH@WEST", "12th@West"),
            Arguments.of("12TH+WEST", "12th+West"),
            Arguments.of("12TH&WEST", "12th&West"),
            Arguments.of("904 OCEANA BLVD-GOOD SHEPHERD SCHOOL", "904 Oceana Blvd-Good Shepherd School"),
            // Capitalization exceptions from config (found on MTC Livermore Route 1)
            Arguments.of("EAST DUBLIN BART STATION", "East Dublin BART Station"),
            Arguments.of("DUBLIN & ARNOLD EB", "Dublin & Arnold EB")
        );
    }

    @ParameterizedTest
    @MethodSource("createCapitalizationCasesWithOwnExceptions")
    public void testConvertToTitleCaseWithOwnExceptions(String input, String expected) {
        NormalizeFieldTransformation transform = NormalizeFieldTransformation.create(
            "table", "field", Lists.newArrayList("NE", "SW"));
        assertEquals(expected, transform.convertToTitleCase(input));
    }

    private static Stream<Arguments> createCapitalizationCasesWithOwnExceptions() {
        return Stream.of(
            // Capitalization exceptions from instance (quadrant street names)
            Arguments.of("10TH STREET NE", "10th Street NE"),
            Arguments.of("10TH STREET SW", "10th Street SW")
        );
    }

    @ParameterizedTest
    @MethodSource("createSubstitutionCases")
    public void testPerformSubstitutions(String input, String expected) {
        assertEquals(expected, NormalizeFieldTransformation.performSubstitutions(input));
    }

    private static Stream<Arguments> createSubstitutionCases() {
        return Stream.of(
            // Replace "@"
            Arguments.of("12TH@WEST", "12TH at WEST"),
            Arguments.of("12TH  @   WEST", "12TH at WEST"),
            // Replace "+"
            Arguments.of("12TH+WEST", "12TH and WEST"),
            Arguments.of("12TH  +   WEST", "12TH and WEST"),
            // Replace "&"
            Arguments.of("12TH&WEST", "12TH and WEST"),
            Arguments.of("12TH  &   WEST", "12TH and WEST"),
            // Replace contents in parentheses and surrounding whitespace.
            Arguments.of("14th St & Broadway (12th St BART) ", "14th St and Broadway")
        );
    }
}
