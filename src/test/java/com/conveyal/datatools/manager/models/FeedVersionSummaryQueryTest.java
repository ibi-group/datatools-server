package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.validator.ValidationResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.conveyal.datatools.TestUtils.createFeedSource;
import static com.conveyal.datatools.TestUtils.createProject;
import static com.conveyal.datatools.manager.models.FeedVersionSummary.getLatestActiveFeedVersionForFeedSources;
import static com.conveyal.datatools.manager.models.FeedVersionSummary.getLatestFeedVersionForFeedSources;
import static com.mongodb.client.model.Filters.in;
import static org.junit.jupiter.api.Assertions.assertEquals;

class FeedVersionSummaryQueryTest extends DatatoolsTest {
    private static Project project;
    private static FeedSource feedSource1;
    private static FeedSource feedSource2;
    private static FeedSource feedSource3;
    private static final LocalDate TODAY = LocalDate.now();

    /** Initialize application for tests to run. */
    @BeforeAll
    static void settingUp() throws Exception {
        // start server if it isn't already running
        DatatoolsTest.setUp();
        Auth0Connection.setAuthDisabled(true);

        project = createProject(String.format("Test project %s", new Date()));
        feedSource1 = createFeedSource("Test feed source 1", project);
        feedSource2 = createFeedSource("Test feed source 2", project);
        feedSource3 = createFeedSource("Test feed source 3", project);
    }

    @AfterAll
    static void tearDown() {
        Auth0Connection.setAuthDisabled(Auth0Connection.getDefaultAuthDisabled());
        if (project != null) {
            project.delete();
        }
    }

    @AfterEach
    void afterEach() {
        // Delete feed versions created for each source.
        Persistence.feedVersions.removeFiltered(
            in("feedSourceId", List.of(feedSource1.id, feedSource2.id, feedSource3.id))
        );
    }

    /**
     * Make sure the latest feed versions, whether expired, future, or current, are correct for each feed source.
     */
    @Test
    void canObtainLatestVersion() {
        createFeedVersions();

        Map<String, FeedVersionSummary> activeSummaries = getLatestFeedVersionForFeedSources(project.id);
        assertEquals(Set.of(feedSource1.id, feedSource2.id, feedSource3.id), activeSummaries.keySet());
        assertEquals("1-future", activeSummaries.get(feedSource1.id).id);
        assertEquals("2-future", activeSummaries.get(feedSource2.id).id);
        assertEquals("3-expired", activeSummaries.get(feedSource3.id).id);
    }

    /**
     * Make sure the latest active feed versions are correct for each feed source.
     */
    @Test
    void canObtainLatestActiveVersion() {
        createFeedVersions();

        Map<String, FeedVersionSummary> activeSummaries = getLatestActiveFeedVersionForFeedSources(project.id);

        // feedSource3 should not appear in the results because it has no active feeds.
        assertEquals(Set.of(feedSource1.id, feedSource2.id), activeSummaries.keySet());
        assertEquals("1-active-current", activeSummaries.get(feedSource1.id).id);
        assertEquals("2-active-current", activeSummaries.get(feedSource2.id).id);
    }

    private static void createFeedVersions() {
        createFeedVersion("1-active-older", 1, feedSource1, -30, 10);
        createFeedVersion("1-active-current", 2, feedSource1, -5, 0);
        createFeedVersion("1-future", 3, feedSource1, 1, 60);

        createFeedVersion("2-active-current", 1, feedSource2, 0, 20);
        createFeedVersion("2-future", 2, feedSource2, 1, 60);

        createFeedVersion("3-expired", 1, feedSource3, -30, -10);
    }

    /**
     * Helper method to create a feed version.
     * The id field serves as description for the purpose of each version.
     */
    private static void createFeedVersion(
        String id,
        int version,
        FeedSource feedSource,
        int startOffsetDaysFromToday,
        int endOffsetDaysFromToday
    ) {
        FeedVersion feedVersion = new FeedVersion(feedSource);
        feedVersion.id = id;
        feedVersion.version = version;
        ValidationResult validationResult = new ValidationResult();
        validationResult.firstCalendarDate = TODAY.plusDays(startOffsetDaysFromToday);
        validationResult.lastCalendarDate = TODAY.plusDays(endOffsetDaysFromToday);
        feedVersion.validationResult = validationResult;
        Persistence.feedVersions.create(feedVersion);
    }
}
