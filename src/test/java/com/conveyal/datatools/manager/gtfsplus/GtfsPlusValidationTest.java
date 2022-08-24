package com.conveyal.datatools.manager.gtfsplus;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.jobs.MergeFeedsJobTest;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;
import java.util.stream.Stream;

import static com.conveyal.datatools.TestUtils.createFeedVersionFromGtfsZip;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/** Runs test to verify that GTFS+ validation runs as expected. */
public class GtfsPlusValidationTest extends UnitTest {
    private static final Logger LOG = LoggerFactory.getLogger(MergeFeedsJobTest.class);
    private static FeedVersion bartVersion1;
    private static FeedVersion bartVersion1WithQuotedValues;
    private static FeedVersion lavtaVersion1;
    private static Project project;
    private static JsonNode routeAttributesFieldsNode;


    /**
     * Create feed version for GTFS+ validation test.
     */
    @BeforeAll
    public static void setUp() throws IOException {
        // Start server if it isn't already running.
        DatatoolsTest.setUp();
        // Create a project, feed sources, and feed versions to merge.
        project = new Project();
        project.name = String.format("Test %s", new Date());
        Persistence.projects.create(project);
        FeedSource bart = new FeedSource("BART");
        bart.projectId = project.id;
        Persistence.feedSources.create(bart);
        bartVersion1 = createFeedVersionFromGtfsZip(bart, "bart_new.zip");
        bartVersion1WithQuotedValues = createFeedVersionFromGtfsZip(bart, "bart_new_with_quoted_values.zip");
        routeAttributesFieldsNode = Objects.requireNonNull(
                GtfsPlusValidation.findNode(DataManager.gtfsPlusConfig, "id", "route_attributes")
            ).get("fields");

        FeedSource lavta = new FeedSource("LAVTA");
        lavta.projectId = project.id;
        Persistence.feedSources.create(lavta);
        lavtaVersion1 = createFeedVersionFromGtfsZip(lavta, "lavta-cal-attributes.zip");
    }

    @AfterAll
    static void tearDown() {
        project.delete();
    }

    @Test
    void canValidateCleanGtfsPlus() throws Exception {
        LOG.info("Validation BART GTFS+");
        GtfsPlusValidation validation = GtfsPlusValidation.validate(bartVersion1.id);
        // Expect issues to be zero.
        assertThat("Issues count for clean BART feed is zero", validation.issues.size(), equalTo(0));
    }

    @Test
    void canValidateGtfsPlusWithQuotedValues() throws Exception {
        LOG.info("Validation BART GTFS+ with quoted values");
        GtfsPlusValidation validation = GtfsPlusValidation.validate(bartVersion1WithQuotedValues.id);
        // Expect issues to be zero.
        assertThat(
            "Issues count for clean BART feed (quoted values) is zero",
            validation.issues.size(), equalTo(0)
        );
    }

    @ParameterizedTest
    @MethodSource("createRouteSubcategoryTestCases")
    void canCheckRouteSubcategory(String routeCategoryId, String routeSubcategoryId, boolean result) {
        assertThat(GtfsPlusValidation.isValueValidWithParent(
            routeCategoryId,
            routeSubcategoryId,
            Objects.requireNonNull(
                GtfsPlusValidation.findNode(routeAttributesFieldsNode, "name", "subcategory")
            )
        ), equalTo(result));
    }

    private static Stream<Arguments> createRouteSubcategoryTestCases() {
        return Stream.of(
            // The values "0", "3" below are not valid subcategory options and should be rejected.
            Arguments.of("0", "0", false),
            Arguments.of("1", "3", false),
            // The values "201", "403" below are correct.
            Arguments.of("2", "201", true),
            Arguments.of("4", "403", true)
        );
    }

    @Test
    void canGetRouteCategorySpecPosition() {
        JsonNode[] fields = new JsonNode[] {
            GtfsPlusValidation.findNode(routeAttributesFieldsNode, "name", "route_id"),
            null, // extra column that is not a route_attributes column.
            GtfsPlusValidation.findNode(routeAttributesFieldsNode, "name", "category"),
            GtfsPlusValidation.findNode(routeAttributesFieldsNode, "name", "subcategory"),
            GtfsPlusValidation.findNode(routeAttributesFieldsNode, "name", "running_way")
        };
        assertThat(GtfsPlusValidation.getParentFieldPosition(fields, "category"), equalTo(2));
    }

    @Test
    void canGetOptionText() {
        assertThat(
            GtfsPlusValidation.getOptionText(
                "202",
                Objects.requireNonNull(
                    GtfsPlusValidation.findNode(routeAttributesFieldsNode, "name", "subcategory")
                )
            ),
            equalTo("Regional Peak"));
    }

    @Test
    void canBuildRouteSubcategoryToCategoryMap() {
        assertThat(
            GtfsPlusValidation.getOptionParentValue(
                "302",
                Objects.requireNonNull(
                    GtfsPlusValidation.findNode(routeAttributesFieldsNode, "name", "subcategory")
                )
            ), equalTo("3"));
    }

    @Test
    void shouldReportEmptyRows() throws Exception {
        // An empty row should be reported as such (separately from a row with incorrect number of columns).

        LOG.info("Validating GTFS+ with an empty row");
        GtfsPlusValidation validation = GtfsPlusValidation.validate(lavtaVersion1.id);
        // Expect one GTFS+ issue of type "empty row".
        assertThat(
            "Should have one GTFS+ validation issue on the calendar_attributes table",
            validation.issues.size(), equalTo(1)
        );
        assertThat(
            "Should have the validation issue on the calendar_attributes table",
            validation.issues.get(0).tableId, equalTo("calendar_attributes")
        );
        assertThat(
            "Should have the GTFS+ 'empty row' validation issue on the calendar_attributes table",
            validation.issues.get(0).description, equalTo("1 row(s) are empty. (File may need to be edited manually.)")
        );
    }
}
