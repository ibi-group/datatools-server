package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Date;
import java.util.stream.Stream;

import static com.conveyal.datatools.TestUtils.createFeedVersion;
import static com.conveyal.datatools.TestUtils.createFeedVersionFromGtfsZip;
import static com.conveyal.datatools.TestUtils.zipFolderFiles;
import static com.conveyal.datatools.manager.models.FeedRetrievalMethod.FETCHED_AUTOMATICALLY;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for the various {@link AutoPublishJob} cases.
 */
public class AutoPublishJobTest extends UnitTest {
    private static final Auth0UserProfile user = Auth0UserProfile.createTestAdminUser();
    private static Project project;
    private static FeedSource feedSource;

    /**
     * Prepare and start a testing-specific web server
     */
    @BeforeAll
    public static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();

        // Create a project, feed sources, and feed versions to merge.
        project = new Project();
        project.name = String.format("Test %s", new Date());
        Persistence.projects.create(project);

        FeedSource fakeAgency = new FeedSource("Feed source", project.id, FETCHED_AUTOMATICALLY);
        Persistence.feedSources.create(fakeAgency);
        feedSource = fakeAgency;
    }

    @AfterAll
    public static void tearDown() {
        if (project != null) {
            project.delete();
        }
    }

    /**
     * Ensures that a feed is or is not published depending on errors in the feed.
     */
    @ParameterizedTest
    @MethodSource("createPublishFeedCases")
    void shouldProcessFeed(String resourceName, boolean isError, String errorMessage) throws IOException {
        // Add the version to the feed source
        if (resourceName.endsWith(".zip")) {
            createFeedVersionFromGtfsZip(feedSource, resourceName);
        } else {
            createFeedVersion(feedSource, zipFolderFiles(resourceName));
        }

        // Create the job
        AutoPublishJob autoPublishJob = new AutoPublishJob(feedSource, user);

        // Run the job in this thread (we're not concerned about concurrency here).
        autoPublishJob.run();

        assertEquals(
            isError,
            autoPublishJob.status.error,
            "AutoPublish job error status was incorrectly determined."
        );
        if (isError) {
            assertEquals(errorMessage, autoPublishJob.status.message);
        }
    }

    private static Stream<Arguments> createPublishFeedCases() {
        return Stream.of(
            Arguments.of(
                "fake-agency-with-only-calendar-expire-in-2099-with-failed-referential-integrity",
                true,
                "Could not publish this feed version because it contains blocking errors."
            ),
            Arguments.of(
                "bart_old_lite.zip",
                true,
                "Could not publish this feed version because it contains GTFS+ blocking errors."
            ),
            Arguments.of(
                "bart_new_lite.zip",
                false,
                null
            )
        );
    }
}
