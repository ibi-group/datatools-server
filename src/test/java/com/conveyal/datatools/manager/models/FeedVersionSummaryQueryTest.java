package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.validator.ValidationResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import static com.mongodb.client.model.Filters.eq;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

class FeedVersionSummaryQueryTest extends DatatoolsTest {
    private static Project project;
    private static FeedSource feedSource1;
    private static FeedSource feedSource2;
    private static FeedSource feedSource3;

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
     * TODO: Make sure the correct version is obtained for the latest active feed version.
     */
    @Test
    void canObtainLatestActiveVersion() {
        LocalDate nowDate = LocalDate.now();
        FeedVersion feedVersionActiveOlder1 = createFeedVersion(
            1,
            feedSource1,
            nowDate.minusDays(30),
            nowDate.plusDays(10)
        );
        FeedVersion feedVersionActive1 = createFeedVersion(
            2,
            feedSource1,
            nowDate.minusDays(5),
            nowDate.plusDays(0)
        );
        FeedVersion feedVersionFuture1 = createFeedVersion(
            3,
            feedSource1,
            nowDate.plusDays(1),
            nowDate.plusDays(60)
        );
        FeedVersion feedVersionActive2 = createFeedVersion(
            1,
            feedSource2,
            nowDate.minusDays(0),
            nowDate.plusDays(20)
        );
        FeedVersion feedVersionFuture2 = createFeedVersion(
            2,
            feedSource2,
            nowDate.plusDays(1),
            nowDate.plusDays(60)
        );

        FeedVersion feedVersionActiveOlder3 = createFeedVersion(
            1,
            feedSource3,
            nowDate.minusDays(30),
            nowDate.minusDays(10)
        );

        Map<String, FeedVersionSummary> activeSummaries = FeedVersionSummary.getLatestActiveFeedVersionForFeedSources(project.id);

        // feedSource3 should not appear in the results because it has no active feeds.
        assertEquals(Set.of(feedSource1.id, feedSource2.id), activeSummaries.keySet());
        assertEquals(feedVersionActive1.id, activeSummaries.get(feedSource1.id).id);
        assertEquals(feedVersionActive2.id, activeSummaries.get(feedSource2.id).id);
    }

    /**
     * Helper method to create a feed version.
     */
    private static FeedVersion createFeedVersion(
        int version,
        FeedSource feedSource,
        LocalDate startDate,
        LocalDate endDate
    ) {
        FeedVersion feedVersion = new FeedVersion(feedSource);
        ValidationResult validationResult = new ValidationResult();
        validationResult.firstCalendarDate = startDate;
        validationResult.lastCalendarDate = endDate;
        feedVersion.validationResult = validationResult;
        feedVersion.version = version;
        Persistence.feedVersions.create(feedVersion);
        return feedVersion;
    }
}
