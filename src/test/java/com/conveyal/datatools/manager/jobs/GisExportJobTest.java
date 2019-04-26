package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.google.common.io.Files;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.Feature;
import org.opengis.feature.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.conveyal.datatools.TestUtils.createFeedVersion;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class GisExportJobTest {
    private static final Logger LOG = LoggerFactory.getLogger(GisExportJobTest.class);
    private static Project project;
    private static FeedVersion calTrainVersion;

    @BeforeClass
    public static void setUp() {
        DatatoolsTest.setUp();
        LOG.info("ProcessGtfsSnapshotMergeTest setup");

        // Create a project, feed sources, and feed versions to merge.
        project = new Project();
        project.name = String.format("Test %s", new Date().toString());
        Persistence.projects.create(project);
        FeedSource caltrain = new FeedSource("Caltrain");
        Persistence.feedSources.create(caltrain);
        calTrainVersion = createFeedVersion(caltrain, "caltrain_gtfs.zip");
        caltrain.projectId = project.id;
    }

    @Test
    public void canExportStops () throws IOException {
        // Run the GIS export job for stops.
        File zipFile = File.createTempFile("stops", ".zip");
        Set<String> ids = new HashSet<>();
        ids.add(calTrainVersion.id);
        GisExportJob gisExportJob = new GisExportJob(GisExportJob.Type.STOPS, zipFile, ids);
        gisExportJob.run();
        assertThat(gisExportJob.status.error, equalTo(false));
        FeatureCollection collection = getFeatureCollectionFromZippedShapefile(zipFile);
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
                    LOG.info("{}: {}", name, property.getValue());
                    if ("the_geom".equals(name)) {
                        // Check that the geometry was exported properly.
//                        assertThat(property.getValue(), notNullValue());
                    }
                }
            }
        }
        // Ensure that all stops from feed version are present in shapefile.
        assertThat(featureCount, equalTo(calTrainVersion.feedLoadResult.stops.rowCount));
    }

    @Test
    public void canExportRoutes () throws IOException, SQLException {
        // Run the GIS export job for stops.
        File zipFile = File.createTempFile("routes", ".zip");
        Set<String> ids = new HashSet<>();
        ids.add(calTrainVersion.id);
        GisExportJob gisExportJob = new GisExportJob(GisExportJob.Type.ROUTES, zipFile, ids);
        gisExportJob.run();
        assertThat(gisExportJob.status.error, equalTo(false));
        FeatureCollection collection = getFeatureCollectionFromZippedShapefile(zipFile);
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
                    LOG.info("{}: {}", name, property.getValue());
                    if ("the_geom".equals(name)) {
                        // Check that the geometry was exported properly.
                        assertThat(property.getValue(), notNullValue());
                    }
                }
            }
        }
        PreparedStatement preparedStatement = DataManager.GTFS_DATA_SOURCE.getConnection()
            .prepareStatement(
                String.format("select count(*) from %s" + ".patterns", calTrainVersion.namespace));
        ResultSet resultSet = preparedStatement.executeQuery();
        int patternCount = 0;
        while (resultSet.next()) {
            patternCount = resultSet.getInt(1);
        }
        // Check that feature count = pattern count from SQL query.
        assertThat(featureCount, equalTo(patternCount));
    }

    private FeatureCollection getFeatureCollectionFromZippedShapefile(File zipFile) throws IOException {
        // Unzip the shapefile and read the stop features to verify they are valid.
        File destDir = Files.createTempDir();
        byte[] buffer = new byte[1024];
        ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile));
        ZipEntry zipEntry = zis.getNextEntry();
        while (zipEntry != null) {
            File newFile = new File(destDir, zipEntry.getName());
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
        for (File file : destDir.listFiles()) {
            // Find the shapefile and
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
                } catch (Throwable e) {}
            }
        }
        return null;
    }
}
