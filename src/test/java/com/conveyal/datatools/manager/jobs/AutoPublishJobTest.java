package com.conveyal.datatools.manager.jobs;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.conveyal.datatools.TestUtils.createFeedVersion;
import static com.conveyal.datatools.TestUtils.createFeedVersionFromGtfsZip;
import static com.conveyal.datatools.TestUtils.zipFolderFiles;
import static com.conveyal.datatools.manager.extensions.mtc.MtcFeedResource.TEST_AGENCY;
import static com.conveyal.datatools.manager.models.FeedRetrievalMethod.FETCHED_AUTOMATICALLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the various {@link AutoPublishJob} cases.
 */
public class AutoPublishJobTest extends UnitTest {
    private static final String TEST_COMPLETED_FOLDER = "test-completed";
    private static final Auth0UserProfile user = Auth0UserProfile.createTestAdminUser();
    private static Project project;
    private static FeedSource feedSource;
    private static ExternalFeedSourceProperty agencyIdProp;

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

        // Add an AgencyId entry to ExternalFeedSourceProperty
        // (one-time, it will be reused for this feed source)
        // but set the value to TEST_AGENCY to prevent actual S3 upload.
        agencyIdProp = new ExternalFeedSourceProperty(
            feedSource,
            "MTC",
            "AgencyId",
            TEST_AGENCY
        );
        Persistence.externalFeedSourceProperties.create(agencyIdProp);
    }

    @AfterAll
    public static void tearDown() {
        if (project != null) {
            project.delete();
        }
        Persistence.externalFeedSourceProperties.removeById(agencyIdProp.id);
    }

    /**
     * Ensures that a feed is or is not published depending on errors in the feed.
     */
    @ParameterizedTest
    @MethodSource("createPublishFeedCases")
    void shouldProcessFeed(String resourceName, boolean isError, String errorMessage) throws IOException {
        // Add the version to the feed source
        FeedVersion originalFeedVersion;
        if (resourceName.endsWith(".zip")) {
            originalFeedVersion = createFeedVersionFromGtfsZip(feedSource, resourceName);
        } else {
            originalFeedVersion = createFeedVersion(feedSource, zipFolderFiles(resourceName));
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

            // In case of error, the sentToExternalPublisher flag should not be set.
            FeedVersion updatedFeedVersion = Persistence.feedVersions.getById(originalFeedVersion.id);
            assertNull(updatedFeedVersion.sentToExternalPublisher);
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

    @ParameterizedTest
    @MethodSource("createUpdateFeedInfoCases")
    void shouldUpdateFeedInfoAfterPublishComplete(String agencyId, boolean isUnknownFeedId) {
        // Add the version to the feed source
        FeedVersion createdVersion = createFeedVersionFromGtfsZip(feedSource, "bart_new_lite.zip");

        // Create the job
        AutoPublishJob autoPublishJob = new AutoPublishJob(feedSource, user);

        // Run the job in this thread (we're not concerned about concurrency here).
        autoPublishJob.run();

        assertFalse(autoPublishJob.status.error);

        // Make sure that the publish-pending attribute has been set for the feed version in Mongo.
        FeedVersion updatedFeedVersion = Persistence.feedVersions.getById(createdVersion.id);
        assertNotNull(updatedFeedVersion.sentToExternalPublisher);

        // Create a test FeedUpdater instance, and simulate running the task.
        TestCompletedFeedRetriever completedFeedRetriever = new TestCompletedFeedRetriever(agencyId);
        FeedUpdater feedUpdater = FeedUpdater.createForTest(completedFeedRetriever);

        // The list of feeds processed externally (completed) should be empty at this point.
        Map<String, String> etags = feedUpdater.checkForUpdatedFeeds();
        assertTrue(etags.isEmpty());

        // Simulate completion of feed publishing.
        completedFeedRetriever.makePublished();

        // The etags should contain the id of the agency.
        // If a feed has been republished since last check, it will have a new etag/file hash,
        // and the scenario below should apply.
        Map<String, String> etagsAfter = feedUpdater.checkForUpdatedFeeds();

        FeedVersion updatedFeedVersionAfter = Persistence.feedVersions.getById(createdVersion.id);
        Date updatedDate = updatedFeedVersionAfter.processedByExternalPublisher;
        String namespace = updatedFeedVersionAfter.namespace;

        if (!isUnknownFeedId) {
            // Regular scenario: updating a known/existing feed.
            assertEquals(1, etagsAfter.size());
            assertTrue(etagsAfter.containsValue("test-etag"));

            // Make sure that the publish-complete attribute has been set for the feed version in Mongo.
            assertNotNull(updatedDate);

            // At the next check for updates, the metadata for the feeds completed above
            // should not be updated again.
            feedUpdater.checkForUpdatedFeeds();
            FeedVersion updatedFeedVersionAfter2 = Persistence.feedVersions.getById(createdVersion.id);
            assertEquals(updatedDate, updatedFeedVersionAfter2.processedByExternalPublisher);
            assertEquals(namespace, updatedFeedVersionAfter2.namespace);
        } else {
            // Edge case: an unknown feed id was provided,
            // so no update of the feed should be happening (and there should not be an exception).
            assertEquals(0, etagsAfter.size());
            assertNull(updatedDate);
        }
    }

    private static Stream<Arguments> createUpdateFeedInfoCases() {
        return Stream.of(
            Arguments.of(
                TEST_AGENCY,
                false
            ),
            Arguments.of(
                "12345",
                true
            )
        );
    }

    /**
     * This test ensures that, upon server startup,
     * feeds that meet all these criteria should not be updated/marked as published:
     * - the feed has been sent to publisher (RTD),
     * - the publisher has not published the feed,
     * - a previous version of the feed was already published.
     */
    @Test
    void shouldNotUpdateFromAPreviouslyPublishedVersionOnStartup() {
        final int TWO_DAYS_MILLIS = 48 * 3600000;

        // Set up a test FeedUpdater instance that fakes an external published date in the past.
        TestCompletedFeedRetriever completedFeedRetriever = new TestCompletedFeedRetriever(TEST_AGENCY);
        FeedUpdater feedUpdater = FeedUpdater.createForTest(completedFeedRetriever);
        completedFeedRetriever.makePublished(new Date(System.currentTimeMillis() - TWO_DAYS_MILLIS));

        // Add the version to the feed source, with
        // sentToExternalPublisher set to a date after a previous publish date.
        FeedVersion createdVersion = createFeedVersionFromGtfsZip(feedSource, "bart_new_lite.zip");
        createdVersion.sentToExternalPublisher = new Date();
        Persistence.feedVersions.replace(createdVersion.id, createdVersion);

        // The list of feeds processed externally (completed) should contain an entry for the agency we want.
        Map<String, String> etags = feedUpdater.checkForUpdatedFeeds();
        assertNotNull(etags.get(TEST_AGENCY));

        // Make sure that the feed remains unpublished.
        FeedVersion updatedFeedVersion = Persistence.feedVersions.getById(createdVersion.id);
        assertNull(updatedFeedVersion.processedByExternalPublisher);

        // Now perform publishing.
        AutoPublishJob autoPublishJob = new AutoPublishJob(feedSource, user);
        autoPublishJob.run();
        assertFalse(autoPublishJob.status.error);

        // Simulate another publishing process
        completedFeedRetriever.makePublished(new Date());

        // The list of feeds processed externally (completed) should contain an entry for the agency we want.
        Map<String, String> etagsAfter = feedUpdater.checkForUpdatedFeeds();
        assertNotNull(etagsAfter.get(TEST_AGENCY));

        // The feed should be published.
        FeedVersion publishedFeedVersion = Persistence.feedVersions.getById(createdVersion.id);
        assertNotNull(publishedFeedVersion.processedByExternalPublisher);

    }

    /**
     * Mocks the results of an {@link S3ObjectSummary} retrieval before/after the
     * external MTC publishing process is complete.
     */
    private static class TestCompletedFeedRetriever implements FeedUpdater.CompletedFeedRetriever {
        private final String agencyId;
        private boolean isPublishingComplete;
        private Date publishDate;

        public TestCompletedFeedRetriever(String agencyId) {
            this.agencyId = agencyId;
        }

        @Override
        public List<S3ObjectSummary> retrieveCompletedFeeds() {
            if (!isPublishingComplete) {
                return new ArrayList<>();
            } else {
                S3ObjectSummary objSummary = new S3ObjectSummary();
                objSummary.setETag("test-etag");
                objSummary.setKey(String.format("%s/%s", TEST_COMPLETED_FOLDER, agencyId));
                objSummary.setLastModified(publishDate);
                return Lists.newArrayList(objSummary);
            }
        }

        public void makePublished() {
            makePublished(new Date());
        }

        public void makePublished(Date publishDate) {
            isPublishingComplete = true;
            this.publishDate = publishDate;
        }
    }
}
