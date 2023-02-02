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
import org.junit.jupiter.api.BeforeAll;
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

class FeedUpdaterTest {
    public static final String FEED_SOURCE_1_ID = "feed-source1-id";
    public static final String FEED_SOURCE_2_ID = "feed-source2-id";
    private static Project project;

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
    }

    @AfterAll
    public static void tearDown() {
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
            "firstFeed",
            "otherFeed"
        );
        assertEquals(expectedIds, FeedUpdater.getFeedIds(s3objects));
    }

    @Test
    void shouldMapFeedIdsToFeedSourceIds() {
        ExternalFeedSourceProperty prop1 = new ExternalFeedSourceProperty();
        prop1.name = AGENCY_ID_FIELDNAME;
        prop1.value = "firstFeed";
        prop1.feedSourceId = "id-for-first-feed";

        ExternalFeedSourceProperty prop2 = new ExternalFeedSourceProperty();
        prop2.name = AGENCY_ID_FIELDNAME;
        prop2.value = "otherFeed";
        prop2.feedSourceId = "id1-for-other-feed";

        ExternalFeedSourceProperty prop3 = new ExternalFeedSourceProperty();
        prop3.name = AGENCY_ID_FIELDNAME;
        prop3.value = "otherFeed";
        prop3.feedSourceId = "id2-for-other-feed";

        List<ExternalFeedSourceProperty> properties = Lists.newArrayList(
            prop1,
            prop2,
            prop3
        );

        Map<String, String> feedIdsToFeedSourceIds = FeedUpdater.mapFeedIdsToFeedSourceIds(properties);

        assertEquals("id-for-first-feed", feedIdsToFeedSourceIds.get("firstFeed"));
        assertEquals("id2-for-other-feed", feedIdsToFeedSourceIds.get("otherFeed"));
        assertNull(feedIdsToFeedSourceIds.get("unknownFeed"));
    }

    @Test
    void shouldMapFeedIdsToFeedSources() {
        FeedSource fs1 = new FeedSource();
        fs1.id = "id-for-first-feed";
        FeedSource fs2 = new FeedSource();
        fs2.id = "id-for-other-feed";

        List<FeedSource> feedSources = Lists.newArrayList(fs1, fs2);

        ExternalFeedSourceProperty prop1 = new ExternalFeedSourceProperty();
        prop1.name = AGENCY_ID_FIELDNAME;
        prop1.value = "firstFeed";
        prop1.feedSourceId = "id-for-first-feed";

        ExternalFeedSourceProperty prop2 = new ExternalFeedSourceProperty();
        prop2.name = AGENCY_ID_FIELDNAME;
        prop2.value = "otherFeed";
        prop2.feedSourceId = "id-for-other-feed";

        List<ExternalFeedSourceProperty> properties = Lists.newArrayList(prop1, prop2);

        Map<String, String> feedIdsToFeedSourceIds = FeedUpdater.mapFeedIdsToFeedSourceIds(properties);

        Map<String, FeedSource> feedIdToFeedSource = FeedUpdater.mapFeedIdsToFeedSources(feedIdsToFeedSourceIds, feedSources);
        assertEquals(fs1, feedIdToFeedSource.get("firstFeed"));
        assertEquals(fs2, feedIdToFeedSource.get("otherFeed"));
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
        Date latestSentDate11 = Date.from(now.minusSeconds(30));
        Date latestSentDate12 = Date.from(now.minusSeconds(50));
        Date latestSentDate21 = Date.from(now.minusSeconds(70));
        Date latestSentDate22 = Date.from(now.minusSeconds(20));

        FeedVersion version11 = new FeedVersion();
        version11.id = "feed-version11";
        version11.feedSourceId = FEED_SOURCE_1_ID;
        version11.sentToExternalPublisher = latestSentDate11;
        Persistence.feedVersions.create(version11);

        FeedVersion version12 = new FeedVersion(source1);
        version12.id = "feed-version12";
        version12.feedSourceId = FEED_SOURCE_1_ID;
        version12.sentToExternalPublisher = latestSentDate12;
        Persistence.feedVersions.create(version12);

        FeedVersion version21 = new FeedVersion(source2);
        version21.id = "feed-version21";
        version21.feedSourceId = FEED_SOURCE_2_ID;
        version21.sentToExternalPublisher = latestSentDate21;
        Persistence.feedVersions.create(version21);

        FeedVersion version22 = new FeedVersion(source2);
        version22.id = "feed-version22";
        version22.feedSourceId = FEED_SOURCE_2_ID;
        version22.sentToExternalPublisher = latestSentDate22;
        Persistence.feedVersions.create(version22);

        Map<String, FeedVersion> latestVersions = FeedUpdater.getLatestVersionsSentForPublishing(
            Lists.newArrayList(source1, source2)
        );

        assertEquals(latestSentDate11, latestVersions.get(FEED_SOURCE_1_ID).sentToExternalPublisher);
        assertEquals(latestSentDate22, latestVersions.get(FEED_SOURCE_2_ID).sentToExternalPublisher);
    }
}
