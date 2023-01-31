package com.conveyal.datatools.manager.jobs;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.conveyal.datatools.manager.extensions.mtc.MtcFeedResource.AGENCY_ID_FIELDNAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

class FeedUpdaterTest {
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
    void shouldGetFeedSourceIdForFeedId() {
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
        assertEquals(null, feedIdsToFeedSourceIds.get("unknownFeed"));
    }

    @Test
    void shouldBuildFeedIDToFeedSourceMap() {
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
}
