package com.conveyal.datatools.manager.jobs.validation;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static com.conveyal.datatools.TestUtils.createFeedVersion;
import static com.conveyal.datatools.TestUtils.zipFolderFiles;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FaresV2ValidationTest {

    private static Project project;
    private static FeedVersion faresV2Version;

    @BeforeAll
    public static void setUp() throws IOException {
        // Start server if it isn't already running.
        DatatoolsTest.setUp();
        Auth0Connection.setAuthDisabled(true);
        // Create a project, feed sources, and feed versions to merge.
        project = new Project();
        project.name = String.format("Test %s", new Date());
        Persistence.projects.create(project);

        FeedSource faresV2FeedSource = new FeedSource("fares-v2");
        faresV2FeedSource.projectId = project.id;
        Persistence.feedSources.create(faresV2FeedSource);
        faresV2Version = createFeedVersion(
            faresV2FeedSource,
            zipFolderFiles("fake-agency-with-fares-v2")
        );
        Persistence.feedVersions.replace(faresV2Version.id, faresV2Version);
    }

    @AfterAll
    static void tearDown() {
        Auth0Connection.setAuthDisabled(Auth0Connection.getDefaultAuthDisabled());
        project.delete();
    }

    @Test
    void canValidateFareV2Files() {
        Document summary = (Document) faresV2Version.mobilityDataResult.get("summary");
        List<String> files = summary.getList("files", String.class);
        assertTrue(files.contains("areas.txt"));
        assertTrue(files.contains("fare_leg_rules.txt"));
        assertTrue(files.contains("fare_media.txt"));
        assertTrue(files.contains("fare_products.txt"));
        assertTrue(files.contains("fare_transfer_rules.txt"));
        assertTrue(files.contains("networks.txt"));
        assertTrue(files.contains("route_networks.txt"));
        assertTrue(files.contains("stop_areas.txt"));
        assertTrue(files.contains("timeframes.txt"));
    }

    /**
     * This is not an exhaustive test, more of a sanity check that MobilityData can detect errors in fares v2 data.
     * https://gtfs-validator.mobilitydata.org/rules.html
     */
    @ParameterizedTest
    @MethodSource("createValidationErrorChecks")
    void canDetectValidationErrors(Set<String> codes, String expectedCode) {
        assertTrue(codes.contains(expectedCode));
    }

    private static Stream<Arguments> createValidationErrorChecks() {
        ArrayList<Document> notices = (ArrayList<Document>) faresV2Version.mobilityDataResult.get("notices");
        Set<String> codes = new HashSet<>();
        for (Document notice : notices) {
            codes.add(notice.getString("code"));
        }
        return Stream.of(
            Arguments.of(codes, "fare_transfer_rule_duration_limit_type_without_duration_limit"),
            Arguments.of(codes, "fare_transfer_rule_duration_limit_without_type"),
            Arguments.of(codes, "fare_transfer_rule_missing_transfer_count"),
            Arguments.of(codes, "fare_transfer_rule_with_forbidden_transfer_count")
        );
    }
}