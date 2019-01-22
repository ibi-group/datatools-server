package com.conveyal.datatools;

import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by landon on 2/24/17.
 */
public abstract class LoadFeedTest {
    private static final Logger LOG = LoggerFactory.getLogger(LoadFeedTest.class);
    public static FeedSource source;
    public static FeedVersion version;

    @BeforeAll
    public void setUp() throws Exception {
        DatatoolsTest.setUp();
        LOG.info("ProcessGtfsSnapshotMergeTest setup");

//        File caltrainGTFS = new File(LoadFeedTest.class.getResource("caltrain_gtfs.zip").getFile());
//        source = new FeedSource("test");
//        source.save();
//        version = new FeedVersion(source);
//        InputStream is = null;
//        try {
//            is = new FileInputStream(caltrainGTFS);
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//        version.newGtfsFile(is);
//        version.save();
//        setUpIsDone = true;
    }
}
