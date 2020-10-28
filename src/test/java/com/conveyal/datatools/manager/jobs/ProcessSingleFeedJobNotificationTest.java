package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedRetrievalMethod;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.validator.ValidationResult;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Date;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

// TODO: Remove or enhance depending on testing requirements.
public class ProcessSingleFeedJobNotificationTest extends DatatoolsTest {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessSingleFeedJobNotificationTest.class);
    private static ValidationResult validationResult = new ValidationResult();
    private static Auth0UserProfile user = Auth0UserProfile.createTestAdminUser();
    private static FeedSource mockFeedSource;

    @BeforeClass
    public static void setUp() throws IOException {
        DatatoolsTest.setUp();
        LOG.info("{} setup", ProcessSingleFeedJobNotificationTest.class.getSimpleName());

        // Create a project, feed sources, and feed versions to merge.
        Project project = new Project();
        project.name = String.format("Test %s", new Date().toString());
        Persistence.projects.create(project);

        // Bart
        mockFeedSource = new FeedSource("Mock Feed Source", project.id, FeedRetrievalMethod.MANUALLY_UPLOADED);
        Persistence.feedSources.create(mockFeedSource);

        validationResult.firstCalendarDate = LocalDate.now();
        validationResult.lastCalendarDate = LocalDate.now();
    }

    @Test
    public void canNotifySubscribersOfErrors() {
        FeedVersion f = createMockFeedVersion();
        validationResult.errorCount = 100;
        f.validationResult = validationResult;
        ProcessSingleFeedJob processSingleFeedJob = new ProcessSingleFeedJob(f, user, true);
        assertThat(
            processSingleFeedJob.getNotificationMessage(),
            equalTo("New feed version created for Mock Feed Source between 2020-10-28 - 2020-10-28. During validation, we found 100 issue(s)")
        );
    }

    @Test
    public void canNotifySubscribersOfSuccess() {
        FeedVersion f = createMockFeedVersion();
        validationResult.errorCount = 0;
        f.validationResult = validationResult;
        ProcessSingleFeedJob processSingleFeedJob = new ProcessSingleFeedJob(f, user, true);
        assertThat(
            processSingleFeedJob.getNotificationMessage(),
            equalTo("New feed version created for Mock Feed Source between 2020-10-28 - 2020-10-28. The validation check found no issues with this new dataset!")
        );
    }

    @Test
    public void canNotifySubscribersOfFailure() {
        FeedVersion f = createMockFeedVersion();
        ProcessSingleFeedJob processSingleFeedJob = new ProcessSingleFeedJob(f, user, true);
        processSingleFeedJob.status.error = true;
        assertThat(
            processSingleFeedJob.getNotificationMessage(),
            equalTo("While attempting to process a new feed version for Mock Feed Source, Data Tools encountered an unrecoverable error. More details: unknown error")
        );
    }

    private FeedVersion createMockFeedVersion() {
        FeedVersion f = new FeedVersion();
        f.feedSourceId = mockFeedSource.id;
        f.retrievalMethod = FeedRetrievalMethod.FETCHED_AUTOMATICALLY;
        return f;
    }
}
