package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.editor.jobs.GisExport;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static spark.Spark.get;
import static spark.Spark.halt;

/**
 * Created by landon on 5/30/17.
 */
public class GisController {
    public static final JsonManager<GisController> json =
            new JsonManager<>(GisController.class, JsonViews.UserInterface.class);
    private static final Logger LOG = LoggerFactory.getLogger(GisController.class);


    public static FileInputStream exportGis (Request req, Response res) throws IOException {
        String type = req.queryParams("type");
        List<String> feedIds = Arrays.asList(req.queryParams("feedId").split(","));
        File temp = File.createTempFile("gis_" + type, ".zip");

        GisExport gisExport = new GisExport(GisExport.Type.valueOf(type), temp, feedIds);
        gisExport.run();

        FileInputStream fis = new FileInputStream(temp);

        res.type("application/zip");
        res.header("Content-Disposition", "attachment;filename=" + temp.getName().replaceAll("[^a-zA-Z0-9]", "") + ".zip");

        // will not actually be deleted until download has completed
        // http://stackoverflow.com/questions/24372279
//        temp.delete();

        return fis;
    }

//    res.raw().setContentType("application/zip");
//    res.raw().setHeader("Content-Disposition", "attachment; filename=" + temp.getName() + ".zip");
//
//    try {
//        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(res.raw().getOutputStream());
//        BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(temp));
//
//        byte[] buffer = new byte[1024];
//        int len;
//        while ((len = bufferedInputStream.read(buffer)) > 0) {
//            bufferedOutputStream.write(buffer, 0, len);
//        }
//
//        bufferedOutputStream.flush();
//        bufferedOutputStream.close();
//    } catch (Exception e) {
//        halt(500, "Error serving GTFS+ file");
//    }
//
//    return res.raw();

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/export/gis", GisController::exportGis, json::write);
    }
}
