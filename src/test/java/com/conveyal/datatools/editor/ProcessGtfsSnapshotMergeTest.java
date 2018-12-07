package com.conveyal.datatools.editor;

import com.conveyal.datatools.editor.jobs.ProcessGtfsSnapshotMerge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by landon on 2/24/17.
 */
public class ProcessGtfsSnapshotMergeTest {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessGtfsSnapshotMergeTest.class);
    static ProcessGtfsSnapshotMerge snapshotMerge;
    private static boolean setUpIsDone = false;

    // TODO: add back in test once editor load is working
//    @Before
//    public void setUp() {
//        if (setUpIsDone) {
//            return;
//        }
//        super.setUp();
//        LOG.info("ProcessGtfsSnapshotMergeTest setup");
//
//        snapshotMerge = new ProcessGtfsSnapshotMerge(super.version, "test@conveyal.com");
//        snapshotMerge.run();
//        setUpIsDone = true;
//    }
//
//    @Test
//    public void countRoutes() {
//        FeedTx feedTx = VersionedDataStore.getFeedTx(source.id);
//        assertEquals(feedTx.routes.size(), 3);
//    }
//
//    @Test
//    public void countStops() {
//        FeedTx feedTx = VersionedDataStore.getFeedTx(source.id);
//        assertEquals(feedTx.stops.size(), 31);
//    }
//
//    @Test
//    public void countTrips() {
//        FeedTx feedTx = VersionedDataStore.getFeedTx(source.id);
//        assertEquals(feedTx.trips.size(), 252);
//    }
//
//    @Test
//    public void countFares() {
//        FeedTx feedTx = VersionedDataStore.getFeedTx(source.id);
//        assertEquals(feedTx.fares.size(), 6);
//    }

//    @Test
//    public void duplicateStops() {
//        ValidationResult result = new ValidationResult();
//
//        result = gtfsValidation1.duplicateStops();
//        Assert.assertEquals(result.invalidValues.size(), 0);
//
//
//        // try duplicate stop test to confirm that stops within the buffer limit are found
//        result = gtfsValidation1.duplicateStops(25.0);
//        Assert.assertEquals(result.invalidValues.size(), 1);
//
//        // try same test to confirm that buffers below the limit don't detect duplicates
//        result = gtfsValidation1.duplicateStops(5.0);
//        Assert.assertEquals(result.invalidValues.size(), 0);
//    }
//
//    @Test
//    public void reversedTripShapes() {
//
//        ValidationResult result = gtfsValidation1.listReversedTripShapes();
//
//        Assert.assertEquals(result.invalidValues.size(), 1);
//
//        // try again with an unusually high distanceMultiplier value
//        result = gtfsValidation1.listReversedTripShapes(50000.0);
//
//        Assert.assertEquals(result.invalidValues.size(), 0);
//
//    }
//
//
//    @Test
//    public void validateTrips() {
//        ValidationResult result = gtfsValidation2.validateTrips();
//
//        Assert.assertEquals(result.invalidValues.size(), 9);
//
//    }
//
//    @Test
//    public void completeBadGtfsTest() {
//
//        GtfsDaoImpl gtfsStore = new GtfsDaoImpl();
//
//        GtfsReader gtfsReader = new GtfsReader();
//
//        File gtfsFile = new File("src/test/resources/st_gtfs_bad.zip");
//
//        try {
//
//            gtfsReader.setInputLocation(gtfsFile);
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        gtfsReader.setEntityStore(gtfsStore);
//
//
//        try {
//            gtfsReader.run();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        try {
//            GtfsValidationService gtfsValidation = new GtfsValidationService(gtfsStore);
//
//            ValidationResult results = gtfsValidation.validateRoutes();
//            results.add(gtfsValidation.validateTrips());
//
//            Assert.assertEquals(results.invalidValues.size(), 5);
//
//            System.out.println(results.invalidValues.size());
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }
//
//    @Test
//    public void completeGoodGtfsTest() {
//
//        GtfsDaoImpl gtfsStore = new GtfsDaoImpl();
//
//        GtfsReader gtfsReader = new GtfsReader();
//
//        File gtfsFile = new File("src/test/resources/st_gtfs_good.zip");
//
//        try {
//
//            gtfsReader.setInputLocation(gtfsFile);
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        gtfsReader.setEntityStore(gtfsStore);
//
//
//        try {
//            gtfsReader.run();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        try {
//            GtfsValidationService gtfsValidation = new GtfsValidationService(gtfsStore);
//
//            ValidationResult results = gtfsValidation.validateRoutes();
//            results.add(gtfsValidation.validateTrips());
//
//            Assert.assertEquals(results.invalidValues.size(), 0);
//
//            System.out.println(results.invalidValues.size());
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }
}
