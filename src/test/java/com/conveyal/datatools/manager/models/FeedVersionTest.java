package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.validator.ValidationResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class FeedVersionTest extends UnitTest {
    private static Project project;
    private static FeedSource feedSource;

    /** Initialize application for tests to run. */
    @BeforeAll
    public static void setUp() throws Exception {
        // start server if it isn't already running
        DatatoolsTest.setUp();

        // set up project
        project = new Project();
        project.name = String.format("Test project %s", new Date());
        Persistence.projects.create(project);

        feedSource = new FeedSource("Test feed source");
        feedSource.projectId = project.id;
        Persistence.feedSources.create(feedSource);
    }

    @AfterAll
    public static void tearDown() {
        if (project != null) {
            project.delete();
        }
    }

    /**
     * Make sure FeedVersionIDs are always unique, even if created at the same second.
     * See https://github.com/ibi-group/datatools-server/issues/251
     */
    @Test
    void canCreateUniqueFeedVersionIDs() {
        // Create a project, feed sources, and feed versions to merge.
        // create two feedVersions immediately after each other which should end up having unique IDs
        FeedVersion feedVersion1 = new FeedVersion(feedSource);
        FeedVersion feedVersion2 = new FeedVersion(feedSource);
        assertThat(feedVersion1.id, not(equalTo(feedVersion2.id)));
    }

    /**
     * Detect feeds with blocking issues for publishing.
     */
    @Test
    void canDetectBlockingIssuesForPublishing() {
        FeedVersion feedVersion1 = new FeedVersion(feedSource);
        feedVersion1.validationResult = new ValidationResult();
        feedVersion1.validationResult.fatalException = "A fatal exception occurred";

        assertThat(feedVersion1.hasBlockingIssuesForPublishing(), equalTo(true));
    }
}
