package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Date;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class FeedVersionTest extends UnitTest {

    /** Initialize application for tests to run. */
    @BeforeClass
    public static void setUp() throws Exception {
        // start server if it isn't already running
        DatatoolsTest.setUp();
    }


    /**
     * Make sure FeedVersionIDs are always unique, even if created at the same second.
     * See https://github.com/ibi-group/datatools-server/issues/251
     */
    @Test
    public void canCreateUniqueFeedVersionIDs() {
        // Create a project, feed sources, and feed versions to merge.
        Project testProject = new Project();
        testProject.name = String.format("Test project %s", new Date().toString());
        Persistence.projects.create(testProject);
        FeedSource testFeedsoure = new FeedSource("Test feed source");
        testFeedsoure.projectId = testProject.id;
        Persistence.feedSources.create(testFeedsoure);

        // create two feedVersions immediately after each other which should end up having unique IDs
        FeedVersion feedVersion1 = new FeedVersion(testFeedsoure);
        FeedVersion feedVersion2 = new FeedVersion(testFeedsoure);
        assertThat(feedVersion1.id, not(equalTo(feedVersion2.id)));
    }
}
