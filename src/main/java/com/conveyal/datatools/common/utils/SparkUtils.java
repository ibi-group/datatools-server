package com.conveyal.datatools.common.utils;

import com.google.gson.JsonObject;
import spark.HaltException;
import spark.Response;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;

import static spark.Spark.halt;

/**
 * Created by landon on 12/15/16.
 */
public class SparkUtils {

    public static Object downloadFile(File file, String filename, Response res) {
        if(file == null) haltWithError(404, "File is null");

        res.raw().setContentType("application/octet-stream");
        res.raw().setHeader("Content-Disposition", "attachment; filename=" + filename);

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
            halt(500, "Error serving GTFS file");
        }

        return res.raw();
    }

    public static String formatJSON(String message, int code, Exception e) {
        String detail = e != null ? e.getMessage() : null;
        JsonObject object = new JsonObject();
        object.addProperty("result", code >= 400 ? "ERR" : "OK");
        object.addProperty("message", message);
        object.addProperty("code", code);
        object.addProperty("detail", detail);
        return object.toString();
    }

    public static void haltWithError (int errorCode, String message) throws HaltException {
        halt(errorCode, formatJSON(message, errorCode));
    }

    public static void haltWithError (int errorCode, String message, Exception e) throws HaltException {
        halt(errorCode, formatJSON(message, errorCode, e));
    }

    public static String formatJSON(String message, int code) {
        return formatJSON(message, code, null);
    }

    public static String formatJSON(String message) {
        return formatJSON(message, 400);
    }

    public static String formatJobMessage (String jobId, String message) {
        JsonObject object = new JsonObject();
        object.addProperty("jobId", jobId);
        object.addProperty("message", message);
        return object.toString();
    }
}
