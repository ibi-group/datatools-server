package com.conveyal.datatools.common.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

    private static final ObjectMapper mapper = new ObjectMapper();

    public static Object downloadFile(File file, String filename, Response res) {
        if(file == null) haltWithMessage(404, "File is null");

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
        ObjectNode object = mapper.createObjectNode();
        object.put("result", code >= 400 ? "ERR" : "OK");
        object.put("message", message);
        object.put("code", code);
        if (detail != null) {
            object.put("detail", detail);
        }
        return object.toString();
    }

    /**
     * Wrapper around Spark halt method that formats message as JSON using {@link SparkUtils#formatJSON}.
     */
    public static void haltWithMessage(int statusCode, String message) throws HaltException {
        halt(statusCode, formatJSON(message, statusCode));
    }

    /**
     * Wrapper around Spark halt method that formats message as JSON using {@link SparkUtils#formatJSON}. Exception
     */
    public static void haltWithMessage(int statusCode, String message, Exception e) throws HaltException {
        halt(statusCode, formatJSON(message, statusCode, e));
    }

    public static String formatJSON(String message, int code) {
        return formatJSON(message, code, null);
    }

    public static String formatJSON(String message) {
        return formatJSON(message, 400);
    }

    public static String formatJobMessage (String jobId, String message) {
        ObjectNode object = mapper.createObjectNode();
        object.put("jobId", jobId);
        object.put("message", message);
        return object.toString();
    }

    public static JsonNode formatJobResponse (String jobId, String message) {
        ObjectNode jNode = mapper.createObjectNode();
        jNode.put("jobId", jobId);
        jNode.put("message", message);
        return jNode;
    }
}
