package com.conveyal.datatools.editor.controllers.api;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.conveyal.datatools.editor.datastore.FeedTx;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.datastore.GlobalTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.transit.Route;
import com.conveyal.datatools.editor.models.transit.Trip;
import com.conveyal.datatools.editor.models.transit.TripPattern;
import org.apache.commons.io.IOUtils;
import org.mapdb.Fun;
import org.mapdb.Fun.Tuple2;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.servlet.MultipartConfigElement;
import javax.servlet.http.Part;

import static spark.Spark.*;


public class RouteController {
    public static JsonManager<Route> json =
            new JsonManager<>(Route.class, JsonViews.UserInterface.class);
    private static Logger LOG = LoggerFactory.getLogger(Route.class);
    public static Object getRoute(Request req, Response res) {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");
        Object json = null;

        if (feedId == null)
            feedId = req.session().attribute("feedId");

        if (feedId == null) {
            halt(400);
        }

        final FeedTx tx = VersionedDataStore.getFeedTx(feedId);

        try {
            if (id != null) {
                if (!tx.routes.containsKey(id)) {
                    tx.rollback();
                    halt(400);
                }

                Route route = tx.routes.get(id);
                route.addDerivedInfo(tx);

                json = Base.toJson(route, false);

//                return route;
            }
            else {
                Route[] ret = tx.routes.values().toArray(new Route[tx.routes.size()]);

                for (Route r : ret) {
                    r.addDerivedInfo(tx);
                }

                json = Base.toJson(ret, false);
                tx.rollback();
//                return json;
            }
        } catch (Exception e) {
            tx.rollbackIfOpen();
            e.printStackTrace();
            halt(400);
        } finally {
            tx.rollbackIfOpen();
        }
        return json;
    }

    public static Object createRoute(Request req, Response res) {
        Route route;

        try {
            route = Base.mapper.readValue(req.body(), Route.class);
            
            GlobalTx gtx = VersionedDataStore.getGlobalTx();
            if (!gtx.feeds.containsKey(route.feedId)) {
                gtx.rollback();
                halt(400);
            }
            
            if (req.session().attribute("feedId") != null && !req.session().attribute("feedId").equals(route.feedId))
                halt(400);
            
            gtx.rollback();
   
            FeedTx tx = VersionedDataStore.getFeedTx(route.feedId);
            
            if (tx.routes.containsKey(route.id)) {
                tx.rollback();
                halt(400);
            }

            // check if gtfsRouteId is specified, if not create from DB id
            if(route.gtfsRouteId == null) {
                route.gtfsRouteId = "ROUTE_" + route.id;
            }
            
            tx.routes.put(route.id, route);
            tx.commit();

            return route;
        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
        }
        return null;
    }


    public static Object updateRoute(Request req, Response res) {
        Route route;
        String id = req.params("id");
        String feedId = req.queryParams("feedId");

        try {
            route = Base.mapper.readValue(req.body(), Route.class);
            if (feedId == null) {
                halt(400);
            }
            FeedTx tx = VersionedDataStore.getFeedTx(feedId);
            
            if (!tx.routes.containsKey(id)) {
                tx.rollback();
                halt(404);
            }


            // check if gtfsRouteId is specified, if not create from DB id
            if(route.gtfsRouteId == null) {
                route.gtfsRouteId = "ROUTE_" + id;
            }
            
            tx.routes.put(id, route);
            tx.commit();

            return route;
        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
        }
        return null;
    }

    public static Object uploadRouteBranding(Request req, Response res) {
        Route route;
        String id = req.params("id");
        String feedId = req.queryParams("feedId");

        try {
            String s3Bucket = DataManager.getConfigPropertyAsText("application.data.gtfs_s3_bucket");
            if (s3Bucket == null) {
                halt(400);
            }

            if (feedId == null) {
                halt(400);
            }
            FeedTx tx = VersionedDataStore.getFeedTx(feedId);

            if (!tx.routes.containsKey(id)) {
                tx.rollback();
                halt(404);
            }

            route = tx.routes.get(id);
            String url = "";
            // Get file from request
//            if (req.raw().getAttribute("org.eclipse.jetty.multipartConfig") == null) {
//                MultipartConfigElement multipartConfigElement = new MultipartConfigElement(System.getProperty("java.io.tmpdir"));
//                req.raw().setAttribute("org.eclipse.jetty.multipartConfig", multipartConfigElement);
//            }

            req.attribute("org.eclipse.jetty.multipartConfig", new MultipartConfigElement("/temp"));

            Part part = req.raw().getPart("file");
            File tempFile = File.createTempFile(route.id, "." + part.getContentType().split("/", 0)[1]);
            tempFile.deleteOnExit();
            InputStream inputStream;
            try {
                inputStream = part.getInputStream();
                FileOutputStream out = new FileOutputStream(tempFile);
                IOUtils.copy(inputStream, out);
            } catch (Exception e) {
                LOG.error("Unable to open input stream from upload");
                halt("Unable to read uploaded file");
            }

            try {
                LOG.info("Uploading route branding to S3");
                // Upload file to s3
                AWSCredentials creds;

                // default credentials providers, e.g. IAM role
                creds = new DefaultAWSCredentialsProviderChain().getCredentials();

                String keyName = "branding/" + tempFile.getName();
                url = "https://s3.amazonaws.com/" + s3Bucket + "/" + keyName;
                AmazonS3 s3client = new AmazonS3Client(creds);
                s3client.putObject(new PutObjectRequest(
                        s3Bucket, keyName, tempFile)
                        // grant public read
                        .withCannedAcl(CannedAccessControlList.PublicRead));
            }
            catch (AmazonServiceException ase) {
                LOG.error("Error uploading feed to S3");
            }

            // set routeBrandingUrl to s3 location
            route.routeBrandingUrl = url;

            tx.routes.put(id, route);
            tx.commit();

            return route;
        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
        }
        return null;
    }

    public static Object deleteRoute(Request req, Response res) {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");

        if (feedId == null)
            feedId = req.session().attribute("feedId");

        if(id == null || feedId == null)
            halt(400);

        FeedTx tx = VersionedDataStore.getFeedTx(feedId);
        

        
        try {
            if (!tx.routes.containsKey(id)) {
                tx.rollback();
                halt(404);
            }
            
            Route r = tx.routes.get(id);

            // delete affected trips
            Set<Tuple2<String, String>> affectedTrips = tx.tripsByRoute.subSet(new Tuple2(r.id, null), new Tuple2(r.id, Fun.HI));
            for (Tuple2<String, String> trip : affectedTrips) {
                tx.trips.remove(trip.b);
            }
            
            // delete affected patterns
            // note that all the trips on the patterns will have already been deleted above
            Set<Tuple2<String, String>> affectedPatts = tx.tripPatternsByRoute.subSet(new Tuple2(r.id, null), new Tuple2(r.id, Fun.HI));
            for (Tuple2<String, String> tp : affectedPatts) {
                tx.tripPatterns.remove(tp.b);
            }
            
            tx.routes.remove(id);
            tx.commit();
            return true; // ok();
        } catch (Exception e) {
            tx.rollback();
            e.printStackTrace();
            halt(404, e.getMessage());
        }
        return null;
    }
    
    /** merge route from into route into, for the given agency ID */
    public static Object mergeRoutes (Request req, Response res) {
        String from = req.queryParams("from");
        String into = req.queryParams("into");

        String feedId = req.queryParams("feedId");

        if (feedId == null)
            feedId = req.session().attribute("feedId");

        if (feedId == null || from == null || into == null)
            halt(400);

        final FeedTx tx = VersionedDataStore.getFeedTx(feedId);

        try {
            // ensure the routes exist
            if (!tx.routes.containsKey(from) || !tx.routes.containsKey(into)) {
                tx.rollback();
                halt(400);
            }

            // get all the trip patterns for route from
            // note that we clone them here so we can later modify them
            Collection<TripPattern> tps = Collections2.transform(
                    tx.tripPatternsByRoute.subSet(new Tuple2(from, null), new Tuple2(from, Fun.HI)),
                    new Function<Tuple2<String, String>, TripPattern>() {
                        @Override
                        public TripPattern apply(Tuple2<String, String> input) {
                            try {
                                return tx.tripPatterns.get(input.b).clone();
                            } catch (CloneNotSupportedException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                                throw new RuntimeException(e);
                            }
                        }
                    });

             for (TripPattern tp : tps) {
                 tp.routeId = into;
                 tx.tripPatterns.put(tp.id, tp);
             }

             // now move all the trips
             Collection<Trip> ts = Collections2.transform(
                     tx.tripsByRoute.subSet(new Tuple2(from, null), new Tuple2(from, Fun.HI)),
                     new Function<Tuple2<String, String>, Trip>() {
                         @Override
                         public Trip apply(Tuple2<String, String> input) {
                             try {
                                return tx.trips.get(input.b).clone();
                            } catch (CloneNotSupportedException e) {
                                e.printStackTrace();
                                throw new RuntimeException(e);
                            }
                         }
                     });

             for (Trip t : ts) {
                 t.routeId = into;
                 tx.trips.put(t.id, t);
             }

             tx.routes.remove(from);

             tx.commit();
             return true; // ok();
        }
        catch (Exception e) {
            e.printStackTrace();
            tx.rollback();
            throw e;
        }
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/route/:id", RouteController::getRoute, json::write);
        options(apiPrefix + "secure/route", (q, s) -> "");
        get(apiPrefix + "secure/route", RouteController::getRoute, json::write);
        post(apiPrefix + "secure/route/merge", RouteController::mergeRoutes, json::write);
        post(apiPrefix + "secure/route", RouteController::createRoute, json::write);
        put(apiPrefix + "secure/route/:id", RouteController::updateRoute, json::write);
        post(apiPrefix + "secure/route/:id/uploadbranding", RouteController::uploadRouteBranding, json::write);
        delete(apiPrefix + "secure/route/:id", RouteController::deleteRoute, json::write);
    }
}
