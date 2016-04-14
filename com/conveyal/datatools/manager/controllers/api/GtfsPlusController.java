package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.jobs.ProcessSingleFeedJob;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletException;
import javax.servlet.http.Part;
import java.io.*;

import static spark.Spark.get;
import static spark.Spark.halt;
import static spark.Spark.post;

/**
 * Created by demory on 4/13/16.
 */
public class GtfsPlusController {

    public static final Logger LOG = LoggerFactory.getLogger(GtfsPlusController.class);

    private static FeedStore gtfsPlusStore = new FeedStore("gtfsplus");


    public static Boolean uploadGtfsPlusFile (Request req, Response res) throws IOException, ServletException {

        FeedSource s = FeedSource.get(req.queryParams("feedSourceId"));

        if (req.raw().getAttribute("org.eclipse.jetty.multipartConfig") == null) {
            MultipartConfigElement multipartConfigElement = new MultipartConfigElement(System.getProperty("java.io.tmpdir"));
            req.raw().setAttribute("org.eclipse.jetty.multipartConfig", multipartConfigElement);
        }

        Part part = req.raw().getPart("file");

        LOG.info("Saving GTFS+ feed {} from upload", s);


        InputStream uploadStream;
        try {
            uploadStream = part.getInputStream();
            //v.newFeed(uploadStream);
            gtfsPlusStore.newFeed(s.id, uploadStream, s);
        } catch (Exception e) {
            LOG.error("Unable to open input stream from upload");
            halt("Unable to read uploaded feed");
        }

        return true;
    }

    private static Object downloadGtfsPlusFile(Request req, Response res) {
        LOG.info("Downloading GTFS+ file for " + req.params("id"));
        File file = gtfsPlusStore.getFeed(req.params("id"));

        res.raw().setContentType("application/octet-stream");
        res.raw().setHeader("Content-Disposition", "attachment; filename=" + file.getName() + ".zip");

        try {
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(res.raw().getOutputStream());
            BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(file));

            byte[] buffer = new byte[1024];
            int len;
            while ((len = bufferedInputStream.read(buffer)) > 0) {
                bufferedOutputStream.write(buffer, 0, len);
            }

            bufferedOutputStream.flush();
            bufferedOutputStream.close();
        } catch (Exception e) {
            halt(500, "Error serving GTFS+ file");
        }

        return res.raw();
    }

    public static void register(String apiPrefix) {
        post(apiPrefix + "secure/gtfsplus", GtfsPlusController::uploadGtfsPlusFile, JsonUtil.objectMapper::writeValueAsString);
        get(apiPrefix + "gtfsplus/:id", GtfsPlusController::downloadGtfsPlusFile);

    }
}
