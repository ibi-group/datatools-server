package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.Region;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import org.apache.commons.io.FilenameUtils;
import org.geotools.geojson.geom.GeometryJSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static spark.Spark.*;
import static spark.Spark.get;

/**
 * Created by landon on 4/15/16.
 */
public class RegionController {
//    public static final Logger LOG = LoggerFactory.getLogger(ProjectController.class);
//
//    public static JsonManager<Region> json =
//            new JsonManager<>(Region.class, JsonViews.UserInterface.class);
//
//    public static Region getRegion(Request req, Response res) {
//        String id = req.params("id");
//        return null;
//        // FIXME add back in regions controller
////        return Persistence.regions.getById(id);
//    }
//
//    public static Collection<Region> getAllRegions(Request req, Response res) throws JsonProcessingException {
//        Collection<Region> regions = new ArrayList<>();
//
//        String projectId = req.queryParams("projectId");
//        System.out.println(req.pathInfo());
//        regions = Persistence.regions.getAll();
////        Boolean publicFilter = Boolean.valueOf(req.queryParams("public"));
////        if(projectId != null) {
////            for (Region region: Persistence.regions.getAll()) {
////                if(region.projectId.equals(projectId)) {
////                    // if requesting public regions and region is not public; skip region
////                    if (publicFilter && !region.isPublic)
////                        continue;
////                    regions.add(region);
////                }
////            }
////        }
////        else {
////            for (Region region: Persistence.regions.getAll()) {
////                // if requesting public regions and region is not public; skip region
////                if (publicFilter && !region.isPublic)
////                    continue;
////                regions.add(region);
////            }
////        }
//
//        return regions;
//    }
//
//    public static Region createRegion(Request req, Response res) throws IOException {
//        Region region;
//
//        region = new Region();
//
//        applyJsonToRegion(region, req.body());
//        region.save();
//
//        return region;
//    }
//
//    public static Region updateRegion(Request req, Response res) throws IOException {
//        String id = req.params("id");
//        Region region = Region.retrieve(id);
//
//        applyJsonToRegion(region, req.body());
//        region.save();
//
//        return region;
//    }
//
//    public static void applyJsonToRegion(Region region, String json) throws IOException {
//        ObjectMapper mapper = new ObjectMapper();
//        JsonNode node = mapper.readTree(json);
//        Iterator<Map.Entry<String, JsonNode>> fieldsIter = node.fields();
//        while (fieldsIter.hasNext()) {
//            Map.Entry<String, JsonNode> entry = fieldsIter.next();
//
//            if(entry.getKey().equals("name")) {
//                region.name = entry.getValue().asText();
//            }
//
//            if(entry.getKey().equals("order")) {
//                region.order = entry.getValue().asText();
//            }
//
//            if(entry.getKey().equals("geometry")) {
//                region.geometry = entry.getValue().asText();
//            }
//
//            if(entry.getKey().equals("isPublic")) {
//                region.isPublic = entry.getValue().asBoolean();
//            }
//
//        }
//    }
//
//    public static Collection<Region> seedRegions(Request req, Response res) throws IOException {
//        Region.deleteAll();
//        Collection<Region> regions = new ArrayList<>();
//        String regionsDir = DataManager.getConfigPropertyAsText("application.data.regions");
//        LOG.info(regionsDir);
//        GeometryJSON gjson = new GeometryJSON();
//        Files.walk(Paths.get(regionsDir)).forEach(filePath -> {
//            if (Files.isRegularFile(filePath) && FilenameUtils.getExtension(filePath.toString()).equalsIgnoreCase("geojson")) {
//                LOG.info(String.valueOf(filePath));
//                ObjectMapper mapper = new ObjectMapper();
//                JsonNode root;
//                try {
//                    root = mapper.readTree(filePath.toFile());
//                    JsonNode features = root.get("features");
//                    for (JsonNode feature : features){
//                        Region region = new Region();
//                        String name;
//                        if (feature.get("properties").has("NAME"))
//                            name = feature.get("properties").get("NAME").asText();
//                        else if (feature.get("properties").has("name"))
//                            name = feature.get("properties").get("name").asText();
//                        else
//                            continue;
//
//                        region.name = name;
////                        LOG.info(region.name);
//                        if (feature.get("properties").has("featurecla"))
//                            region.order = feature.get("properties").get("featurecla").asText();
//
////                        LOG.info("getting geometry");
//                        if (feature.has("geometry")) {
//                            region.geometry = feature.get("geometry").toString();
//                            Reader reader = new StringReader(feature.toString());
//                            MultiPolygon poly = gjson.readMultiPolygon(reader);
//                            Point center = poly.getCentroid();
//                            region.lon = center.getX();
//                            region.lat = center.getY();
//                            Envelope envelope = poly.getEnvelopeInternal();
//                            region.east = envelope.getMaxX();
//                            region.west = envelope.getMinX();
//                            region.north = envelope.getMaxY();
//                            region.south = envelope.getMinY();
//                        }
//                        else {
//                            LOG.info("no geometry for " + region.name);
//                        }
//
//                        region.isPublic = true;
//                        region.save();
//                        regions.add(region);
//                    }
//                } catch (IOException e) {
//                    e.printStackTrace();
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//            else {
//                LOG.warn(filePath.getFileName() + " is not geojson");
//            }
//        });
//        return regions;
//    }
//    public static Region deleteRegion(Request req, Response res) {
//        String id = req.params("id");
//        Region region = Region.retrieve(id);
//        region.delete();
//        return region;
//    }

    public static void register (String apiPrefix) {
//        get(apiPrefix + "secure/region/:id", RegionController::getRegion, json::write);
//        get(apiPrefix + "secure/region", RegionController::getAllRegions, json::write);
//        post(apiPrefix + "secure/region", RegionController::createRegion, json::write);
//        put(apiPrefix + "secure/region/:id", RegionController::updateRegion, json::write);
//        delete(apiPrefix + "secure/region/:id", RegionController::deleteRegion, json::write);
//
//        // Public routes
//        get(apiPrefix + "public/region/:id", RegionController::getRegion, json::write);
//        get(apiPrefix + "public/region", RegionController::getAllRegions, json::write);
//
//        get(apiPrefix + "public/seedregions", RegionController::seedRegions, json::write);
    }
}
