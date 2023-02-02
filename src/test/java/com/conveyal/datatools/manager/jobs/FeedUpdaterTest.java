package com.conveyal.datatools.manager.jobs;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.conveyal.datatools.manager.extensions.mtc.MtcFeedResource.AGENCY_ID_FIELDNAME;
import static com.mongodb.client.model.Filters.in;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FeedUpdaterTest {
    public static final String FEED_SOURCE_1_ID = "feed-source1-id";
    public static final String FEED_SOURCE_2_ID = "feed-source2-id";
    public static final String FIRST_FEED = "firstFeed";
    public static final String OTHER_FEED = "otherFeed";
    private static Project project;
    private static final String PROJECT_NAME = String.format("Test %s", new Date());

    @BeforeAll
    public static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();
    }

    @BeforeEach
    void beforeEach() {
        // Create a project.
        project = new Project();
        project.id = PROJECT_NAME;
        project.name = PROJECT_NAME;
        Persistence.projects.create(project);
    }

    @AfterEach
    void afterEach() {
        // Delete feed versions, then the project (that will delete feed sources too).
        Persistence.feedVersions.removeFiltered(in("feedSourceId", FEED_SOURCE_1_ID, FEED_SOURCE_2_ID));
        if (project != null) {
            project.delete();
        }
    }

    @Test
    void shouldGetFeedIdsFromS3ObjectSummaries() {
        S3ObjectSummary s3Obj1 = new S3ObjectSummary();
        s3Obj1.setKey("s3bucket/firstFeed.zip");
        S3ObjectSummary s3Obj2 = new S3ObjectSummary();
        s3Obj2.setKey("s3bucket/otherFeed.zip");
        S3ObjectSummary s3Obj3 = new S3ObjectSummary();
        s3Obj3.setKey("s3bucket/"); // format for S3 bucket top-level folders.
        List<S3ObjectSummary> s3objects = Lists.newArrayList(
            s3Obj1,
            s3Obj2,
            s3Obj3
        );
        List<String> expectedIds = Lists.newArrayList(
            FIRST_FEED,
            OTHER_FEED
        );
        assertEquals(expectedIds, FeedUpdater.getFeedIds(s3objects));
    }

    @Test
    void shouldMapFeedIdsToFeedSourceIds() {
        ExternalFeedSourceProperty prop1 = new ExternalFeedSourceProperty();
        prop1.name = AGENCY_ID_FIELDNAME;
        prop1.value = FIRST_FEED;
        prop1.feedSourceId = FEED_SOURCE_1_ID;

        ExternalFeedSourceProperty prop2 = new ExternalFeedSourceProperty();
        prop2.name = AGENCY_ID_FIELDNAME;
        prop2.value = OTHER_FEED;
        prop2.feedSourceId = FEED_SOURCE_2_ID;

        ExternalFeedSourceProperty prop3 = new ExternalFeedSourceProperty();
        prop3.name = AGENCY_ID_FIELDNAME;
        prop3.value = OTHER_FEED;
        prop3.feedSourceId = "other-feed-source-id";

        List<ExternalFeedSourceProperty> properties = Lists.newArrayList(
            prop1,
            prop2,
            prop3
        );

        Map<String, String> feedIdsToFeedSourceIds = FeedUpdater.mapFeedIdsToFeedSourceIds(properties);

        assertEquals(FEED_SOURCE_1_ID, feedIdsToFeedSourceIds.get(FIRST_FEED));
        assertEquals("other-feed-source-id", feedIdsToFeedSourceIds.get(OTHER_FEED));
        assertNull(feedIdsToFeedSourceIds.get("unknownFeed"));
    }

    @Test
    void shouldMapFeedIdsToFeedSources() {
        FeedSource fs1 = new FeedSource();
        fs1.id = FEED_SOURCE_1_ID;
        FeedSource fs2 = new FeedSource();
        fs2.id = FEED_SOURCE_2_ID;
        List<FeedSource> feedSources = Lists.newArrayList(fs1, fs2);

        ExternalFeedSourceProperty prop1 = new ExternalFeedSourceProperty();
        prop1.name = AGENCY_ID_FIELDNAME;
        prop1.value = FIRST_FEED;
        prop1.feedSourceId = FEED_SOURCE_1_ID;

        ExternalFeedSourceProperty prop2 = new ExternalFeedSourceProperty();
        prop2.name = AGENCY_ID_FIELDNAME;
        prop2.value = OTHER_FEED;
        prop2.feedSourceId = FEED_SOURCE_2_ID;

        List<ExternalFeedSourceProperty> properties = Lists.newArrayList(prop1, prop2);

        Map<String, String> feedIdsToFeedSourceIds = FeedUpdater.mapFeedIdsToFeedSourceIds(properties);

        Map<String, FeedSource> feedIdToFeedSource = FeedUpdater.mapFeedIdsToFeedSources(feedIdsToFeedSourceIds, feedSources);
        assertEquals(fs1, feedIdToFeedSource.get(FIRST_FEED));
        assertEquals(fs2, feedIdToFeedSource.get(OTHER_FEED));
    }

    @Test
    void shouldGetLatestVersionsSentToPublisher() {
        // Create two feed sources, and two feed versions in each that have different sentToExternalPublisher values.
        FeedSource source1 = new FeedSource(FEED_SOURCE_1_ID);
        source1.id = FEED_SOURCE_1_ID;
        source1.projectId = PROJECT_NAME;
        Persistence.feedSources.create(source1);

        FeedSource source2 = new FeedSource(FEED_SOURCE_2_ID);
        source2.id = FEED_SOURCE_2_ID;
        source2.projectId = PROJECT_NAME;
        Persistence.feedSources.create(source2);

        Instant now = Instant.now();
        Date source1LatestSentDate = Date.from(now.minusSeconds(30));
        Date source1OtherDate = Date.from(now.minusSeconds(50));
        Date source2LatestSentDate = Date.from(now.minusSeconds(20));
        Date source2OtherDate = Date.from(now.minusSeconds(70));

        FeedVersion version11 = new FeedVersion();
        version11.id = "feed-version11";
        version11.feedSourceId = FEED_SOURCE_1_ID;
        version11.sentToExternalPublisher = source1LatestSentDate;
        Persistence.feedVersions.create(version11);

        FeedVersion version12 = new FeedVersion(source1);
        version12.id = "feed-version12";
        version12.feedSourceId = FEED_SOURCE_1_ID;
        version12.sentToExternalPublisher = source1OtherDate;
        Persistence.feedVersions.create(version12);

        FeedVersion version21 = new FeedVersion(source2);
        version21.id = "feed-version21";
        version21.feedSourceId = FEED_SOURCE_2_ID;
        version21.sentToExternalPublisher = source2OtherDate;
        Persistence.feedVersions.create(version21);

        FeedVersion version22 = new FeedVersion(source2);
        version22.id = "feed-version22";
        version22.feedSourceId = FEED_SOURCE_2_ID;
        version22.sentToExternalPublisher = source2LatestSentDate;
        Persistence.feedVersions.create(version22);

        Map<String, FeedVersion> latestVersions = FeedUpdater.getLatestVersionsSentForPublishing(
            Lists.newArrayList(source1, source2)
        );

        FeedVersion source1LatestVersion = latestVersions.get(FEED_SOURCE_1_ID);
        FeedVersion source2LatestVersion = latestVersions.get(FEED_SOURCE_2_ID);
        assertEquals("feed-version11", source1LatestVersion.id);
        assertEquals(source1LatestSentDate, source1LatestVersion.sentToExternalPublisher);
        assertEquals("feed-version22", source2LatestVersion.id);
        assertEquals(source2LatestSentDate, source2LatestVersion.sentToExternalPublisher);
    }

    @Test
    void shouldGetFeedVersionsToMarkAsProcessed() {
        FeedSource source1 = new FeedSource(FEED_SOURCE_1_ID);
        source1.id = FEED_SOURCE_1_ID;
        source1.projectId = PROJECT_NAME;
        Persistence.feedSources.create(source1);

        Instant now = Instant.now();
        Date nowDate = Date.from(now);
        Date previousDate = Date.from(now.minusSeconds(60));

        FeedVersion versionNotSent = new FeedVersion();
        versionNotSent.id = "version-not-sent";
        versionNotSent.feedSourceId = FEED_SOURCE_1_ID;
        versionNotSent.sentToExternalPublisher = null;
        Persistence.feedVersions.create(versionNotSent);

        FeedVersion versionNotProcessed = new FeedVersion();
        versionNotProcessed.id = "version-not-processed";
        versionNotProcessed.feedSourceId = FEED_SOURCE_1_ID;
        versionNotProcessed.sentToExternalPublisher = nowDate;
        versionNotProcessed.processedByExternalPublisher = null;
        Persistence.feedVersions.create(versionNotProcessed);

        FeedVersion versionNewlyProcessed = new FeedVersion();
        versionNewlyProcessed.id = "version-newly-processed";
        versionNewlyProcessed.feedSourceId = FEED_SOURCE_1_ID;
        versionNewlyProcessed.sentToExternalPublisher = previousDate;
        versionNewlyProcessed.processedByExternalPublisher = nowDate;
        Persistence.feedVersions.create(versionNewlyProcessed);

        FeedVersion versionNewlySent = new FeedVersion();
        versionNewlySent.id = "version-newly-sent";
        versionNewlySent.feedSourceId = FEED_SOURCE_1_ID;
        versionNewlySent.sentToExternalPublisher = nowDate;
        versionNewlySent.processedByExternalPublisher = previousDate;
        Persistence.feedVersions.create(versionNewlySent);

        List<String> versions = FeedUpdater.getFeedVersionsToMarkAsProcessed();
        assertEquals(2, versions.size());
        assertTrue(versions.contains("version-not-processed"));
        assertTrue(versions.contains("version-newly-sent"));
    }
}
