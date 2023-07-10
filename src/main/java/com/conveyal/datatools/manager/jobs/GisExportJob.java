package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.editor.utils.DirectoryZip;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.loader.Requirement;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.Agency;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.io.FileUtils;
import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Export routes or stops for a GTFS feed version as a shapefile. */
public class GisExportJob extends MonitorableJob {
    public static final Logger LOG = LoggerFactory.getLogger(GisExportJob.class);
    public ExportType exportType;
    public Collection<String> feedIds;

    public GisExportJob(ExportType exportType, File file, Collection<String> feedIds, Auth0UserProfile owner) {
        super(
            owner,
            String.format("Export %s GIS for feed", exportType.toString().toLowerCase()),
            JobType.EXPORT_GIS
        );
        this.exportType = exportType;
        this.file = file;
        this.feedIds = feedIds;
        status.update("Beginning export", 5);
    }

    // GIS export behaviour extracted to be shared with DeploymentGISExportJob
    public void packageShapefiles(File outDir, String outShpName, int percentComplete) {
        File outShp = new File(outDir, outShpName);
        Connection connection = null;
        try {
            GeometryFactory geometryFactory = new GeometryFactory();
            ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();

            Map<String, Serializable> params = new HashMap<>();
            params.put("url", outShp.toURI().toURL());

            ShapefileDataStore datastore = (ShapefileDataStore) dataStoreFactory.createNewDataStore(params);
            datastore.forceSchemaCRS(DefaultGeographicCRS.WGS84);

            final SimpleFeatureType STOP_TYPE = DataUtilities.createType(
                    "Stop",
                    String.join(",",
                        // Geometry must be the first attribute for a shapefile (and must be named
                        // "the_geom"). We must include SRID, otherwise the projection will be undefined.
                        "the_geom:Point:srid=4326",
                        "name:String",
                        "code:String",
                        "desc:String",
                        "id:String",
                        "agency:String"
                    )
            );

            final SimpleFeatureType ROUTE_TYPE = DataUtilities.createType(
                "Route", // <- the name for our feature type
                String.join(",",
                    // Geometry must be the first attribute for a shapefile (and must be named
                    // "the_geom"). We must include SRID, otherwise the projection will be undefined.
                    "the_geom:LineString:srid=4326",
                    "pattName:String",
                    "shortName:String",
                    "longName:String",
                    "desc:String",
                    "type:String",
                    "url:String",
                    "routeColor:String",
                    "textColor:String",
                    "shapeId:String",
                    "agency:String"
                )
            );
            SimpleFeatureBuilder featureBuilder;
            DefaultFeatureCollection features = new DefaultFeatureCollection();
            // Get connection for use in fetching patterns. This is outside of for loop so we're
            // not connecting multiple times.
            connection = DataManager.GTFS_DATA_SOURCE.getConnection();
            for (String feedId : feedIds) {
                // Get feed version and connection to RDBMS feed.
                FeedVersion version = Persistence.feedVersions.getById(feedId);
                if (version == null) {
                    throw new IllegalStateException(String.format("Could not find version %s", feedId));
                }
                Feed feed = new Feed(DataManager.GTFS_DATA_SOURCE, version.namespace);
                Agency agency = feed.agencies.iterator().next();
                String agencyName = agency != null
                    ? agency.agency_name
                    : version.parentFeedSource().name;
                status.update(
                    String.format(
                        "Exporting %s for %s",
                        exportType.toString().toLowerCase(),
                        agencyName),
                    percentComplete
                );
                if (exportType.equals(ExportType.STOPS)) {
                    datastore.createSchema(STOP_TYPE);
                    featureBuilder = new SimpleFeatureBuilder(STOP_TYPE);
                    for (Stop stop : feed.stops) {
                        Point point = geometryFactory.createPoint(
                            new Coordinate(stop.stop_lon, stop.stop_lat)
                        );
                        LOG.info(point.toString());
                        featureBuilder.add(point);
                        featureBuilder.add(stop.stop_name);
                        featureBuilder.add(stop.stop_code);
                        featureBuilder.add(stop.stop_desc);
                        featureBuilder.add(stop.stop_id);
                        featureBuilder.add(agencyName);
                        // Build feature (null id arg will generate default ID).
                        SimpleFeature feature = featureBuilder.buildFeature(null);
                        features.add(feature);
                    }
                } else if (exportType.equals(ExportType.ROUTES)) {
                    datastore.createSchema(ROUTE_TYPE);
                    featureBuilder = new SimpleFeatureBuilder(ROUTE_TYPE);
                    // There is not a clean way to fetch patterns out of the RDBMS and it may not
                    // be worth building a structured way with JDBCTableReader simply for
                    // exporting a shapefile. If there are future similar cases, we may need to
                    // refactor this into a more structured operation using Java objects or
                    // com.conveyal.gtfs.loader.Feed
                    // Note: we use generateSelectSql for PROPRIETARY because we encountered an issue with some feeds
                    // (perhaps legacy) not containing the column patterns#direction_id.
                    // See https://github.com/ibi-group/datatools-server/issues/203
                    // TODO: replace with Table#generateSelectAllSql
                    String patternsSql = Table.PATTERNS.generateSelectSql(version.namespace, Requirement.PROPRIETARY);
                    PreparedStatement statement = connection.prepareStatement(patternsSql);
                    ResultSet resultSet = statement.executeQuery();
                    // we loop over trip patterns. Note that this will yield several lines for routes that have
                    // multiple patterns. There's no real good way to reconcile the shapes of multiple patterns.
                    while (resultSet.next()) {
                        String pattern_id = resultSet.getString("pattern_id");
                        String route_id = resultSet.getString("route_id");
                        String name = resultSet.getString("name");
                        String shape_id = resultSet.getString("shape_id");
                        LineString shape;
                        if (shape_id != null) {
                            // Select shape points for pattern shape and build line string.
                            PreparedStatement shapeStatement = connection.prepareStatement(
                                String.format(
                                    "select shape_pt_lon, shape_pt_lat, shape_pt_sequence, "
                                            + "shape_id from %s.shapes where shape_id = ? "
                                            + "order by shape_pt_sequence",
                                    version.namespace
                                ));
                            shapeStatement.setString(1, shape_id);
                            ResultSet shapePointsResultSet = shapeStatement.executeQuery();
                            // Construct line string from shape points.
                            List<Coordinate> coordinates = new ArrayList<>();
                            while (shapePointsResultSet.next()) {
                                double lon = shapePointsResultSet.getDouble(1);
                                double lat = shapePointsResultSet.getDouble(2);
                                coordinates.add(new Coordinate(lon, lat));
                            }
                            Coordinate[] coords = new Coordinate[coordinates.size()];
                            coords = coordinates.toArray(coords);
                            shape = geometryFactory.createLineString(coords);
                        } else {
                            LOG.info("Building pattern {} from stops", pattern_id);
                            // Build the shape from the pattern stops if there is no shape for
                            // pattern.
                            PreparedStatement stopsStatement = connection.prepareStatement(
                                String.format(
                                    "select stop_lon, stop_lat, stops.stop_id, stop_sequence, pattern_id"
                                        + " from %s.stops as stops, %s.pattern_stops as ps"
                                        + " where pattern_id = ? and stops.stop_id = ps.stop_id"
                                        + " order by pattern_id, stop_sequence",
                                    version.namespace, version.namespace
                                ));
                            stopsStatement.setString(1, pattern_id);
                            List<Coordinate> coordinates = new ArrayList<>();
                            ResultSet stopsResultSet = stopsStatement.executeQuery();
                            while (stopsResultSet.next()) {
                                double lon = stopsResultSet.getDouble(1);
                                double lat = stopsResultSet.getDouble(2);
                                coordinates.add(new Coordinate(lon, lat));
                            }
                            Coordinate[] coords = new Coordinate[coordinates.size()];
                            coords = coordinates.toArray(coords);
                            shape = geometryFactory.createLineString(coords);
                        }

                        Route route = feed.routes.get(route_id);
                        if (route == null) {
                            LOG.warn("Route ({}) for pattern {} does not exist. Skipping pattern"
                                , route_id, pattern_id);
                            continue;
                        }
                        featureBuilder.add(shape);
                        featureBuilder.add(name);
                        featureBuilder.add(route.route_short_name);
                        featureBuilder.add(route.route_long_name);
                        featureBuilder.add(route.route_desc);
                        featureBuilder.add(route.route_type);
                        featureBuilder.add(route.route_url);
                        featureBuilder.add(route.route_color);
                        featureBuilder.add(route.route_text_color);
                        featureBuilder.add(shape_id);
                        featureBuilder.add(agencyName);
                        SimpleFeature feature = featureBuilder.buildFeature(null);
                        features.add(feature);
                    }
                } else {
                    throw new IllegalStateException("Invalid type");
                }
            }
            if (features.size() == 0) {
                throw new IllegalStateException("Cannot write shapefile with zero features!");
            }
            // Save the file
            Transaction transaction = new DefaultTransaction("create");
            String typeName = datastore.getTypeNames()[0];
            SimpleFeatureSource featureSource = datastore.getFeatureSource(typeName);
            // Check that we have read-write access to disk:
            // http://docs.geotools.org/stable/userguide/library/data/featuresource.html
            if (featureSource instanceof SimpleFeatureStore) {
                SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;
                featureStore.setTransaction(transaction);
                try {
                    LOG.info("Adding {} features to shapefile.", features.size());
                    featureStore.addFeatures(features);
                    transaction.commit();
                } catch (Exception e)  {
                    e.printStackTrace();
                    transaction.rollback();
                    throw e;
                } finally {
                    transaction.close();
                }
            } else {
                // If this is thrown, there could be some other issue unrelated to read/write
                // access, for example, during development of this feature this error was thrown
                // when there were no features contained within the shapefile.
                throw new Exception(typeName + " does not support read/write access (or other "
                    + "unknown issue).");
            }
        }
        catch (Exception e) {
            status.fail("An exception occurred during the shapefile packaging", e);
        } finally {
            if (connection != null) DbUtils.closeQuietly(connection);
        }
    }

    // Overload packageShapefiles for default percentComplete
    public void packageShapefiles(File outDir, String outShpName) {
        packageShapefiles(outDir, outShpName, 40);
    }

    public void zipShapefiles(File outDir) throws IOException {
        LOG.info("Zipping shapefile {}", file.getAbsolutePath());
        // zip the file
        DirectoryZip.zip(outDir, file);

        // Clean up temporary files.
        for (File f : outDir.listFiles()) {
            // DeploymentGisExportJob creates subDirectories for each feed
            // These should be cleaned as well.
            if (f.isDirectory()){
                FileUtils.deleteDirectory(f);
            } else {
                f.delete();
            }
        }
        outDir.delete();
        status.completeSuccessfully("Export complete!");
    }

    public File setupGisExport() throws IOException {
        LOG.info("Storing shapefile for feeds {} at {}", feedIds, file.getAbsolutePath());
        File outDir = Files.createTempDirectory("export_" + exportType).toFile();
        LOG.info("Temp directory for shapefile: {}", outDir.getAbsolutePath());
        return outDir;
    }

    @Override public void jobLogic() {
        // Methods are added to this JobLogic so that the DeploymentGisExportJob can make use of them
        // The DeploymentGisExportJob zips the shapefiles after repeating the packageShapefiles step,
        // hence that method is separated out.
        try {
            File outDir = setupGisExport();
            packageShapefiles(outDir, file.getName().replaceAll("\\.zip", "") + ".shp");
            zipShapefiles(outDir);
        }
        catch (Exception e) {
            status.fail("An error occurred exporting the GIS shapefiles");
        }
    }

    public enum ExportType { ROUTES, STOPS }
}
