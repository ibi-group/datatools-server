package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.List;
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
        NormalizeFieldTransformation transform = createTransformation();
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
    @MethodSource("createCapitalizationCasesWithCustomExceptions")
    public void testConvertToTitleCaseWithCustomExceptions(String input, String expected) {
        NormalizeFieldTransformation transform = createTransformation(
            "table", "field",  Lists.newArrayList("NE", "SW", "de"), null);
        assertEquals(expected, transform.convertToTitleCase(input));
    }

    private static Stream<Arguments> createCapitalizationCasesWithCustomExceptions() {
        return Stream.of(
            // Capitalization exceptions from instance (quadrant street names)
            Arguments.of("10TH STREET NE", "10th Street NE"),
            Arguments.of("10TH STREET SW", "10th Street SW"),
            // In the example below, particle 'de' should remain in lower case.
            Arguments.of("PONCE DE LEON", "Ponce de Leon")
        );
    }

    @ParameterizedTest
    @MethodSource("createSubstitutionCases")
    public void testPerformSubstitutions(String input, String expected) {
        NormalizeFieldTransformation transform = createTransformation();
        assertEquals(expected, transform.performSubstitutions(input));
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

    @ParameterizedTest
    @MethodSource("createSubstitutionCasesWithOwnExceptions")
    public void testPerformSubstitutionsWithOwnExceptions(String input, String expected) {
        NormalizeFieldTransformation transform = createTransformation(
            "table",
            "field",
            null,
            Lists.newArrayList(
                new Substitution("Station", "Stn"),
                new Substitution("#", "AND", true)
            )
        );
        assertEquals(expected, transform.performSubstitutions(input));
    }

    private static Stream<Arguments> createSubstitutionCasesWithOwnExceptions() {
        return Stream.of(
            // Capitalization exceptions from instance (quadrant street names)
            Arguments.of("Embarcadero Station", "Embarcadero Stn"),
            Arguments.of("10th#North", "10th AND North")
        );
    }

    /**
     * Proxy to create a transformation
     * (called by {@link com.conveyal.datatools.manager.jobs.NormalizeFieldTransformJobTest}).
     */
    public static NormalizeFieldTransformation createTransformation(
        String table, String fieldName, List<String> exceptions, List<Substitution> substitutions)
    {
        return NormalizeFieldTransformation.create(table, fieldName, exceptions, substitutions);
    }

    /**
     * Proxy to create a transformation
     * (called by {@link com.conveyal.datatools.manager.jobs.NormalizeFieldTransformJobTest}).
     */
    public static NormalizeFieldTransformation createTransformation() {
        return NormalizeFieldTransformation.create("table", "field", null, null);
    }
}
