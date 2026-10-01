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

import static com.conveyal.datatools.manager.models.FeedVersionSummary.getLatestActiveFeedVersionForFeedSources;
import static com.mongodb.client.model.Filters.in;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
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

        // set up project and feed source
        project = new Project();
        project.name = String.format("Test project %s", new Date());
        Persistence.projects.create(project);

        feedSource1 = new FeedSource("Test feed source 1");
        feedSource1.projectId = project.id;
        Persistence.feedSources.create(feedSource1);

        feedSource2 = new FeedSource("Test feed source 2");
        feedSource2.projectId = project.id;
        Persistence.feedSources.create(feedSource2);

        feedSource3 = new FeedSource("Test feed source 3");
        feedSource3.projectId = project.id;
        Persistence.feedSources.create(feedSource3);

        // Add some feed versions
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
     * TODO: Make sure the correct version is obtained for the latest feed version.
     */
    @Test
    void canObtainLatestVersion() {
        // Create a project, feed sources, and feed versions to merge.
        // create two feedVersions immediately after each other which should end up having unique IDs
        FeedVersion feedVersion1 = new FeedVersion(feedSource1);
        FeedVersion feedVersion2 = new FeedVersion(feedSource1);
        assertThat(feedVersion1.id, not(equalTo(feedVersion2.id)));
    }

    /**
     * Make sure the latest active feed versions are correct for each feed source.
     */
    @Test
    void canObtainLatestActiveVersion() {
        createFeedVersion("1-active-older", feedSource1, -30, 10);
        createFeedVersion("1-active-current", feedSource1, -5, 0);
        createFeedVersion("1-future", feedSource1, 1, 60);

        createFeedVersion("2-active-current", feedSource2, 0, 20);
        createFeedVersion("2-future", feedSource2, 1, 60);

        createFeedVersion("3-expired", feedSource3, -30, -10);

        Map<String, FeedVersionSummary> activeSummaries = getLatestActiveFeedVersionForFeedSources(project.id);

        // feedSource3 should not appear in the results because it has no active feeds.
        assertEquals(Set.of(feedSource1.id, feedSource2.id), activeSummaries.keySet());
        assertEquals("1-active-current", activeSummaries.get(feedSource1.id).id);
        assertEquals("2-active-current", activeSummaries.get(feedSource2.id).id);
    }

    /**
     * Helper method to create a feed version.
     * id serves as description for each version.
     */
    private static void createFeedVersion(
        String id,
        FeedSource feedSource,
        int startOffsetDaysFromToday,
        int endOffsetDaysFromToday
    ) {
        FeedVersion feedVersion = new FeedVersion(feedSource);
        feedVersion.id = id;
        ValidationResult validationResult = new ValidationResult();
        validationResult.firstCalendarDate = TODAY.plusDays(startOffsetDaysFromToday);
        validationResult.lastCalendarDate = TODAY.plusDays(endOffsetDaysFromToday);
        feedVersion.validationResult = validationResult;
        Persistence.feedVersions.create(feedVersion);
    }
}
