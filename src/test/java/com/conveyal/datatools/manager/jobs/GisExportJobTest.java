package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedRetrievalMethod;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.SqlAssert;
import com.google.common.base.Strings;
import com.google.common.io.Files;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.Point;
import org.apache.commons.io.FileUtils;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.referencing.CRS;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opengis.feature.Feature;
import org.opengis.feature.Property;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.conveyal.datatools.TestUtils.createFeedVersionFromGtfsZip;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;

public class GisExportJobTest extends UnitTest {
    private static final Logger LOG = LoggerFactory.getLogger(GisExportJobTest.class);
    public static Project project;
    public static FeedVersion calTrainVersion;
    public static FeedVersion hawaiiVersion;

    // Bounding box for Caltrain and Hawaii is approximately:
    public static final double CALTRAIN_WEST = -122.5918;
    public static final double CALTRAIN_EAST = -121.5523;
    public static final double CALTRAIN_NORTH = 37.8499;
    public static final double CALTRAIN_SOUTH = 37.002;

    // This was added for use by DeploymentGisExportJobTest
    private static final double HAWAII_WEST = -155.0924;
    private static final double HAWAII_EAST = -155.063;
    private static final double HAWAII_NORTH = 19.719;
    private static final double HAWAII_SOUTH = 19.693;

    private static void assertCoordinateWithinBounds (String agencyName, Coordinate coordinate) {
        if (agencyName.equals("Caltrain")) {
            assertThat(coordinate.x, greaterThan(CALTRAIN_WEST));
            assertThat(coordinate.x, lessThan(CALTRAIN_EAST));
            assertThat(coordinate.y, greaterThan(CALTRAIN_SOUTH));
            assertThat(coordinate.y, lessThan(CALTRAIN_NORTH));
        } else if (agencyName.equals("Hawaii")) {
            assertThat(coordinate.x, greaterThan(HAWAII_WEST));
            assertThat(coordinate.x, lessThan(HAWAII_EAST));
            assertThat(coordinate.y, greaterThan(HAWAII_SOUTH));
            assertThat(coordinate.y, lessThan(HAWAII_NORTH));
        }
    }

    public void validateShapefiles(File[] files, String agencyName, GisExportJob.ExportType exportType) throws IOException, SQLException {
        FeatureCollection collection = getFeatureCollectionFromZippedShapefile(files);
        assertCrsIsNotNull(files);
        // Iterate over features.
        int featureCount = 0;
        try (FeatureIterator iterator = collection.features()) {
            while (iterator.hasNext()) {
                featureCount++;
                Feature feature = iterator.next();
                // GeometryAttribute sourceGeometry = feature.getDefaultGeometryProperty();
                Collection<Property> properties = feature.getProperties();
                // Iterate over feature properties and verify everything looks OK.
                for (Property property : properties) {
                    String name = property.getName().toString();
                    Object value = property.getValue();
                    LOG.info("{}: {}", name, value);
                    if ("the_geom".equals(name)) {
                        if (exportType.equals(GisExportJob.ExportType.STOPS)) {
                            Point point = (Point) value;
                            Coordinate coordinate = point.getCoordinate();
                            // Check that the geometry was exported properly.
                            assertThat(point, notNullValue());
                            // Check that coordinates are in the right spot.
                            assertCoordinateWithinBounds(agencyName, coordinate);
                        } else if (exportType.equals(GisExportJob.ExportType.ROUTES)) {
                            MultiLineString shape = (MultiLineString) value;
                            // don't log entire linestring value as it severly clutters up the logs
                            LOG.info("{}: ({} points)", name, shape.getNumPoints());
                            // Check that the geometry was exported properly.
                            assertThat(shape, notNullValue());
                            Coordinate[] coordinates = shape.getCoordinates();
                            // Check that shape has coordinates and the values are (generally) in the
                            // right place.
                            assertThat(coordinates.length, greaterThan(0));
                            for (Coordinate coordinate : coordinates) {
                                assertCoordinateWithinBounds(agencyName, coordinate);
                            }
                        } else {
                            LOG.info("{}: {}", name, value);
                        }
                    }
                }
            }
        }
        // Ensure that all stops from feed version are present in shapefile.
        // Check that feature count = pattern count from SQL query.
        SqlAssert sqlAssert = agencyName.equals("Caltrain") ? new SqlAssert(calTrainVersion) : new SqlAssert(hawaiiVersion);
        if (agencyName.equals("Caltrain")) {
            if (exportType.equals(GisExportJob.ExportType.ROUTES)){
                sqlAssert.patterns.assertCount(featureCount);
            } else if (exportType.equals(GisExportJob.ExportType.STOPS)) {
                assertThat(featureCount, equalTo(calTrainVersion.feedLoadResult.stops.rowCount));
            }
        } else if (agencyName.equals("Hawaii")) {
            if (exportType.equals(GisExportJob.ExportType.ROUTES)){
                sqlAssert.patterns.assertCount(featureCount);
            } else if (exportType.equals(GisExportJob.ExportType.STOPS)) {
                assertThat(featureCount, equalTo(hawaiiVersion.feedLoadResult.stops.rowCount));
            }
        }
    }


    @BeforeAll
    public static void setUp() throws IOException {
        DatatoolsTest.setUp();
        LOG.info("{} setup", GisExportJobTest.class.getSimpleName());

        // Create a project, feed sources, and feed versions to merge.
        project = new Project();
        project.name = String.format("Test %s", new Date().toString());
        Persistence.projects.create(project);
        FeedSource caltrain = new FeedSource("Caltrain", project.id, FeedRetrievalMethod.MANUALLY_UPLOADED);
        caltrain.deployable = true; //Set feedsources to be deployable for DeploymentGisExportJobTest
        Persistence.feedSources.create(caltrain);
        calTrainVersion = createFeedVersionFromGtfsZip(caltrain, "caltrain_gtfs.zip");
        FeedSource hawaii = new FeedSource("Hawaii", project.id, FeedRetrievalMethod.MANUALLY_UPLOADED);
        hawaii.deployable = true; //Set feedsources to be deployable for DeploymentGisExportJobTest
        Persistence.feedSources.create(hawaii);
        hawaiiVersion = createFeedVersionFromGtfsZip(hawaii, "hawaii_fake_no_shapes.zip");
    }

    /**
     * Ensures that a shapefile containing stop features for a feed version can be exported and
     * contains geometry for each stop.
     */
    @Test
    public void canExportStops () throws IOException, SQLException {
        // Run the GIS export job for stops.
        File zipFile = File.createTempFile("stops", ".zip");
        Set<String> ids = new HashSet<>();
        ids.add(calTrainVersion.id);
        Auth0UserProfile user = Auth0UserProfile.createTestAdminUser();
        GisExportJob gisExportJob = new GisExportJob(GisExportJob.ExportType.STOPS, zipFile, ids, user);
        gisExportJob.run();
        assertThat(gisExportJob.status.error, equalTo(false));
        File[] files = getFilesFromZippedShapefile(zipFile);

        // Shapefile assertions are shared with DeploymentGisExportJobTest and so are extracted.
        validateShapefiles(files, "Caltrain", GisExportJob.ExportType.STOPS);
    }

    /** Get CRS from unzipped shapefile set of files and ensure it's not null. */
    public void assertCrsIsNotNull(File[] files) throws IOException {
        CoordinateReferenceSystem crs = getCRSFromShapefiles(files);
        assertThat("Coordinate reference system is not null.", crs, notNullValue());
    }

    /**
     * Ensures that a shapefile containing route (pattern) features for a feed version can be
     * exported and contains geometry for each pattern.
     */
    @Test
    public void canExportRoutes () throws IOException, SQLException {
        // Run the GIS export job for routes.
        File zipFile = File.createTempFile("routes", ".zip");
        Set<String> ids = new HashSet<>();
        ids.add(calTrainVersion.id);
        Auth0UserProfile user = Auth0UserProfile.createTestAdminUser();
        GisExportJob gisExportJob = new GisExportJob(GisExportJob.ExportType.ROUTES, zipFile, ids, user);
        gisExportJob.run();
        assertThat(gisExportJob.status.error, equalTo(false));
        File[] files = getFilesFromZippedShapefile(zipFile);

        validateShapefiles(files, "Caltrain", GisExportJob.ExportType.ROUTES);
    }

    /**
     * Verifies that a route shapefile can be generated from its constituent pattern stops.
     */
    @Test
    public void canExportRoutesFromPatternStops() throws IOException, SQLException {
        // Run the GIS export job for stops.
        File zipFile = File.createTempFile("routes", ".zip");
        Set<String> ids = new HashSet<>();
        ids.add(hawaiiVersion.id);
        Auth0UserProfile user = Auth0UserProfile.createTestAdminUser();
        GisExportJob gisExportJob = new GisExportJob(GisExportJob.ExportType.ROUTES, zipFile, ids, user);
        gisExportJob.run();
        assertThat(gisExportJob.status.error, equalTo(false));
        File[] files = getFilesFromZippedShapefile(zipFile);
        FeatureCollection collection = getFeatureCollectionFromZippedShapefile(files);
        assertCrsIsNotNull(files);
        // Iterate over features.
        int featureCount = 0;
        try (FeatureIterator iterator = collection.features()) {
            while (iterator.hasNext()) {
                featureCount++;
                Feature feature = iterator.next();
                Collection<Property> properties = feature.getProperties();
                // Iterate over feature properties and verify everything looks OK.
                for (Property property : properties) {
                    String name = property.getName().toString();
                    Object value = property.getValue();
                    LOG.info("{}: {}", name, value);
                    if ("the_geom".equals(name)) {
                        MultiLineString shape = (MultiLineString) value;
                        // Check that the geometry was exported properly.
                        assertThat(shape, notNullValue());
                        // Fake Hawaii feed has only 5 stops and each is used in the single pattern
                        // shape, so we expect the coordinates length to be equal to stops row count.
                        assertThat(
                            shape.getCoordinates().length,
                            equalTo(hawaiiVersion.feedLoadResult.stops.rowCount)
                        );
                    }
                }
            }
        }
        // Check that feature count = pattern count from SQL query.
        SqlAssert sqlAssert = new SqlAssert(hawaiiVersion);
        sqlAssert.patterns.assertCount(featureCount);
    }

    /** Unzip the shapefile into a temp directory and return a list of its files. */
    public File[] getFilesFromZippedShapefile(File zipFile) throws IOException {
        File destDir = Files.createTempDir();
        byte[] buffer = new byte[1024];
        ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile));
        ZipEntry zipEntry = zis.getNextEntry();
        while (zipEntry != null) {
            File newFile = new File(destDir, zipEntry.getName());
            if (!newFile.toPath().normalize().startsWith(destDir.toPath().normalize())) {
                throw new RuntimeException("Bad zip entry");
            }
            FileOutputStream fos = new FileOutputStream(newFile);
            int len;
            while ((len = zis.read(buffer)) > 0) {
                fos.write(buffer, 0, len);
            }
            fos.close();
            zipEntry = zis.getNextEntry();
        }
        zis.closeEntry();
        zis.close();
        return destDir.listFiles();
    }

    /**
     * Get the coordinate reference system (contained in the .prj file) from a set of files representing an unzipped
     * shapefile.
     */
    private CoordinateReferenceSystem getCRSFromShapefiles(File[] files) throws IOException {
        for (File file : files) {
            if (file.getName().endsWith(".prj")) {
                LOG.info("Found projection entry: {}", file.getAbsolutePath());
                String wkt = FileUtils.readFileToString(file, "UTF-8");
                try {
                    CoordinateReferenceSystem crs = CRS.parseWKT(wkt);
                    LOG.info("CRS is {}", crs.getCoordinateSystem().toString());
                    return crs;
                } catch (FactoryException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    /**
     * Utility method to extract a {@link FeatureCollection} from a zipped shapefile during tests. This also asserts
     * that the projection file included in the shapefile (.prj) contains a valid/parseable coordinate reference system.
     */
    public FeatureCollection getFeatureCollectionFromZippedShapefile(File[] files) throws IOException {
        for (File file : files) {
            // Find the shapefile and return its features
            if (file.getName().endsWith(".shp")) {
                LOG.info("Found shapefile entry: {}", file.getAbsolutePath());
                try {
                    Map<String, String> connect = new HashMap<>();
                    connect.put("url", file.toURI().toString());
                    DataStore dataStore = DataStoreFinder.getDataStore(connect);
                    String[] typeNames = dataStore.getTypeNames();
                    String typeName = typeNames[0];
                    LOG.info("Reading content " + typeName);
                    // Create feature collection from data.
                    FeatureSource featureSource = dataStore.getFeatureSource(typeName);
                    return featureSource.getFeatures();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }
}
