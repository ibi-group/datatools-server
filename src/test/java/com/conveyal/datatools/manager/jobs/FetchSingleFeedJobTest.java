package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

import static com.conveyal.datatools.TestUtils.assertThatSqlCountQueryYieldsExpectedCount;
import static com.conveyal.datatools.TestUtils.createFeedVersionFromGtfsZip;
import static org.junit.Assert.assertEquals;

public class FetchSingleFeedJobTest extends DatatoolsTest {
    private static final Logger LOG = LoggerFactory.getLogger(GisExportJobTest.class);
    private static FeedSource caltrain;
    private static FeedSource fakeAgency;
    private static FeedSource fakeGTFS;

    // Constant feed source ids are used in order to match the subscriptions target within a subscribers app meta data.
    private static final String CALTRAIN_FEED_SOURCE_ID = "f35d38b5-47a4-4cdd-8494-b846bf9d1c3a";
    private static final String FAKEGTFS_FEED_SOURCE_ID = "3a1b2c80-253d-4b4b-bb19-97f73954d552";
    private static final String FAKEAGENCY_FEED_SOURCE_ID = "705d5369-3712-48bf-a373-7353d17adeab";

    @BeforeClass
    public static void setUp() throws IOException {
        DatatoolsTest.setUp();
        LOG.info("{} setup", GisExportJobTest.class.getSimpleName());
        caltrain = new FeedSource("Caltrain");
        fakeGTFS = new FeedSource("FakeGTFS");
        fakeAgency = new FeedSource("FakeAgency");
        caltrain.id = CALTRAIN_FEED_SOURCE_ID;
        fakeGTFS.id = FAKEGTFS_FEED_SOURCE_ID;
        fakeAgency.id = FAKEAGENCY_FEED_SOURCE_ID;
        Persistence.feedSources.create(caltrain);
        Persistence.feedSources.create(fakeGTFS);
        Persistence.feedSources.create(fakeAgency);
    }

    @AfterClass
    public static void tearDown() {
        if (caltrain != null) Persistence.feedSources.removeById(caltrain.id);
        if (fakeGTFS != null) Persistence.feedSources.removeById(fakeGTFS.id);
        if (fakeAgency != null) Persistence.feedSources.removeById(fakeAgency.id);
    }

    @Test
    public void canNotifySubscribersOfErrors() throws SQLException {
        FeedVersion caltrainFeedVersion = createFeedVersionFromGtfsZip(caltrain, "caltrain_gtfs.zip");
        Persistence.feedVersions.removeById(caltrainFeedVersion.id);
        evaluateFeedVersionErrors(caltrainFeedVersion.namespace, 253);

    }

    @Test
    public void canNotifySubscribersOfFailure() {
        FeedVersion fakeGTFSFeedVersion = createFeedVersionFromGtfsZip(fakeGTFS, "fake-gtfs-txt-file.zip");
        Persistence.feedVersions.removeById(fakeGTFSFeedVersion.id);
        assertEquals(fakeGTFSFeedVersion.validationResult.fatalException, "failure!");
    }

    @Test
    public void canNotifySubscribersOfSuccess() throws SQLException {
        FeedVersion fakeAgencyFeedVersion = createFeedVersionFromGtfsZip(fakeAgency, "fake-agency-no-errors.zip");
        Persistence.feedVersions.removeById(fakeAgencyFeedVersion.id);
        evaluateFeedVersionErrors(fakeAgencyFeedVersion.namespace, 0);
    }

    private void evaluateFeedVersionErrors(String namespace, int expectedCount) throws SQLException {
        assertThatSqlCountQueryYieldsExpectedCount(String.format("select count(*) from %s.errors", namespace), expectedCount);
    }
}
