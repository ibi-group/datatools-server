package com.conveyal.datatools.common.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.HaltException;
import spark.Response;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;

import static spark.Spark.halt;

/**
 * Contains a collection of utility methods used in conjunction with the Spark HTTP requests and responses.
 */
public class SparkUtils {
    private static final Logger LOG = LoggerFactory.getLogger(SparkUtils.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Write out the supplied file to the Spark response as an octet-stream.
     */
    public static HttpServletResponse downloadFile(File file, String filename, Response res) {
        if (file == null) haltWithMessage(404, "File is null");
        HttpServletResponse raw = res.raw();
        raw.setContentType("application/octet-stream");
        raw.setHeader("Content-Disposition", "attachment; filename=" + filename);
        // Override the gzip content encoding applied to standard API responses.
        res.header("Content-Encoding", "identity");
        try (
            FileInputStream fileInputStream = new FileInputStream(file);
            ServletOutputStream outputStream = raw.getOutputStream()
        ) {
            // Write the file input stream to the response's output stream.
            ByteStreams.copy(fileInputStream, outputStream);
            // TODO: Is flushing the stream necessary?
            outputStream.flush();
        } catch (Exception e) {
            LOG.error("Could not write file to output stream", e);
            haltWithMessage(500, "Error serving GTFS file");
        }
        return raw;
    }

    /**
     * Constructs a JSON string containing the provided key/value pair.
     */
    public static String formatJSON (String key, String value) {
        return mapper.createObjectNode()
                .put(key, value)
                .toString();
    }

    /**
     * Constructs a JSON string with a result (i.e., OK or ERR), message, code, and if the exception argument is
     * supplied details about the exception encountered.
     */
    public static String formatJSON(String message, int code, Exception e) {
        String detail = e != null ? e.getMessage() : null;
        return mapper.createObjectNode()
                .put("result", code >= 400 ? "ERR" : "OK")
                .put("message", message)
                .put("code", code)
                .put("detail", detail)
                .toString();
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

    /**
     * Convenience wrapper around formatJSON that excludes the exception argument.
     */
    public static String formatJSON(String message, int code) {
        return formatJSON(message, code, null);
    }

    /**
     * Construct JSON string response that contains message and jobId fields.
     */
    public static String formatJobMessage (String jobId, String message) {
        return mapper.createObjectNode()
            .put("jobId", jobId)
            .put("message", message)
            .toString();
    }
}
