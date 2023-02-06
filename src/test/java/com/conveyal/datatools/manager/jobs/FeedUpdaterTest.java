package com.conveyal.datatools.manager.jobs;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.conveyal.datatools.manager.extensions.mtc.MtcFeedResource.AGENCY_ID_FIELDNAME;
import static com.mongodb.client.model.Filters.in;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FeedUpdaterTest {
    private static final String FEED_SOURCE_1_ID = "feed-source1-id";
    private static final String FEED_SOURCE_2_ID = "feed-source2-id";
    private static final String FIRST_FEED = "firstFeed";
    private static final String OTHER_FEED = "otherFeed";
    private static Project project;
    private static FeedSource source1;
    private static FeedSource source2;
    private static final String PROJECT_NAME = String.format("Test %s", new Date());

    @BeforeAll
    public static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();

        // Create a project.
        project = new Project();
        project.id = PROJECT_NAME;
        project.name = PROJECT_NAME;
        Persistence.projects.create(project);

        // Create two feed sources.
        source1 = persistFeedSource(FEED_SOURCE_1_ID);
        source2 = persistFeedSource(FEED_SOURCE_2_ID);
    }

    @AfterAll
    static void afterAll() {
        if (project != null) {
            project.delete();
        }
    }

    @AfterEach
    void afterEach() {
        // Delete feed versions created in each test.
        Persistence.feedVersions.removeFiltered(in("feedSourceId", FEED_SOURCE_1_ID, FEED_SOURCE_2_ID));
    }

    @Test
    void shouldGetFeedIdsFromS3ObjectSummaries() {
        List<S3ObjectSummary> s3objects = Lists.newArrayList(
                "s3bucket/firstFeed.zip",
                "s3bucket/otherFeed.zip",
                "s3bucket/"
            )
            .stream()
            .map(k -> {
                S3ObjectSummary s3Obj = new S3ObjectSummary();
                s3Obj.setKey(k);
                return s3Obj;
            })
            .collect(Collectors.toList());
        List<String> expectedIds = Lists.newArrayList(
            FIRST_FEED,
            OTHER_FEED
        );
        assertEquals(expectedIds, FeedUpdater.getFeedIds(s3objects));
    }

    @Test
    void shouldMapFeedIdsToFeedSourceIds() {
        List<ExternalFeedSourceProperty> properties = Lists.newArrayList(
            makeFeedSourceProperty(FIRST_FEED, FEED_SOURCE_1_ID),
            makeFeedSourceProperty(OTHER_FEED, FEED_SOURCE_2_ID),
            makeFeedSourceProperty(OTHER_FEED, "other-feed-source-id")
        );
        Map<String, String> feedIdsToFeedSourceIds = FeedUpdater.mapFeedIdsToFeedSourceIds(properties);

        assertEquals(FEED_SOURCE_1_ID, feedIdsToFeedSourceIds.get(FIRST_FEED));
        assertEquals("other-feed-source-id", feedIdsToFeedSourceIds.get(OTHER_FEED));
    }

    @Test
    void shouldMapFeedIdsToFeedSources() {
        List<FeedSource> feedSources = Lists.newArrayList(source1, source2);
        List<ExternalFeedSourceProperty> properties = Lists.newArrayList(
            makeFeedSourceProperty(FIRST_FEED, FEED_SOURCE_1_ID),
            makeFeedSourceProperty(OTHER_FEED, FEED_SOURCE_2_ID)
        );

        Map<String, String> feedIdsToFeedSourceIds = FeedUpdater.mapFeedIdsToFeedSourceIds(properties);

        Map<String, FeedSource> feedIdToFeedSource = FeedUpdater.mapFeedIdsToFeedSources(feedIdsToFeedSourceIds, feedSources);
        assertEquals(source1, feedIdToFeedSource.get(FIRST_FEED));
        assertEquals(source2, feedIdToFeedSource.get(OTHER_FEED));
    }

    @Test
    void shouldGetLatestVersionsSentToPublisher() {
        // In each of the two pre-created feed sources, create and two feed versions
        // that have different sentToExternalPublisher values.
        Instant now = Instant.now();
        Date source1LatestSentDate = Date.from(now.minusSeconds(30));
        Date source1OtherDate = Date.from(now.minusSeconds(50));
        Date source2LatestSentDate = Date.from(now.minusSeconds(20));
        Date source2OtherDate = Date.from(now.minusSeconds(70));

        persistFeedVersion("feed-version11", FEED_SOURCE_1_ID, source1LatestSentDate, null);
        persistFeedVersion("feed-version12", FEED_SOURCE_1_ID, source1OtherDate, null);
        persistFeedVersion("feed-version21", FEED_SOURCE_2_ID, source2OtherDate, null);
        persistFeedVersion("feed-version22", FEED_SOURCE_2_ID, source2LatestSentDate, null);

        Map<String, FeedVersion> latestVersions = FeedUpdater.getLatestVersionsSentForPublishing(
            Lists.newArrayList(source1, source2)
        );

        checkFeedVersionDates("feed-version11", latestVersions.get(FEED_SOURCE_1_ID), source1LatestSentDate);
        checkFeedVersionDates("feed-version22", latestVersions.get(FEED_SOURCE_2_ID), source2LatestSentDate);
    }

    private static void checkFeedVersionDates(String versionId, FeedVersion version, Date sentDate) {
        assertEquals(versionId, version.id);
        assertEquals(sentDate, version.sentToExternalPublisher);
    }

    @Test
    void shouldGetFeedVersionsToMarkAsProcessed() {
        Instant now = Instant.now();
        Date nowDate = Date.from(now);
        Date previousDate = Date.from(now.minusSeconds(60));

        persistFeedVersion("version-not-sent", FEED_SOURCE_1_ID, null, null);
        persistFeedVersion("version-not-processed", FEED_SOURCE_1_ID, nowDate, null);
        persistFeedVersion("version-newly-processed", FEED_SOURCE_1_ID, previousDate, nowDate);
        persistFeedVersion("version-newly-sent", FEED_SOURCE_1_ID, nowDate, previousDate);

        List<String> versions = FeedUpdater.getFeedVersionsToMarkAsProcessed();
        assertEquals(2, versions.size());
        assertTrue(versions.contains("version-not-processed"));
        assertTrue(versions.contains("version-newly-sent"));
    }

    private static ExternalFeedSourceProperty makeFeedSourceProperty(String feedId, String feedSourceId) {
        return new ExternalFeedSourceProperty(feedSourceId, "MTC", AGENCY_ID_FIELDNAME, feedId);
    }

    private static void persistFeedVersion(String id, String feedSourceId, Date sentDate, Date processedDate) {
        FeedVersion feedVersion = new FeedVersion();
        feedVersion.id = id;
        feedVersion.feedSourceId = feedSourceId;
        feedVersion.sentToExternalPublisher = sentDate;
        feedVersion.processedByExternalPublisher = processedDate;
        Persistence.feedVersions.create(feedVersion);
    }

    private static FeedSource persistFeedSource(String feedSourceId) {
        FeedSource source1 = new FeedSource(feedSourceId);
        source1.id = feedSourceId;
        source1.projectId = PROJECT_NAME;
        Persistence.feedSources.create(source1);
        return source1;
    }
}
