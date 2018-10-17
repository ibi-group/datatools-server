package com.conveyal.datatools.common.utils;

import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.HaltException;
import spark.Request;
import spark.Response;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

import static com.conveyal.datatools.manager.DataManager.getConfigPropertyAsText;
import static spark.Spark.halt;

/**
 * Contains a collection of utility methods used in conjunction with the Spark HTTP requests and responses.
 */
public class SparkUtils {
    private static final Logger LOG = LoggerFactory.getLogger(SparkUtils.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String BASE_URL = getConfigPropertyAsText("application.public_url");
    private static final int DEFAULT_LINES_TO_PRINT = 10;

    /**
     * Write out the supplied file to the Spark response as an octet-stream.
     */
    public static HttpServletResponse downloadFile(File file, String filename, Request req, Response res) {
        if (file == null) haltWithMessage(req, 404, "File is null");
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
            e.printStackTrace();
            haltWithMessage(req, 500, "Error serving GTFS file", e);
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
     * Constructs an object node with a result (i.e., OK or ERR), message, code, and if the exception argument is
     * supplied details about the exception encountered.
     */
    public static ObjectNode getObjectNode(String message, int code, Exception e) {
        String detail = e != null ? e.getMessage() : null;
        return mapper.createObjectNode()
            .put("result", code >= 400 ? "ERR" : "OK")
            .put("message", message)
            .put("code", code)
            .put("detail", detail);
    }

    /**
     * Constructs a JSON string with a result (i.e., OK or ERR), message, code, and if the exception argument is
     * supplied details about the exception encountered.
     */
    public static String formatJSON(String message, int code, Exception e) {
        return getObjectNode(message, code, e).toString();
    }

    /**
     * Wrapper around Spark halt method that formats message as JSON using {@link SparkUtils#formatJSON}.
     */
    public static void haltWithMessage(Request request, int statusCode, String message) throws HaltException {
        haltWithMessage(request, statusCode, message, null);
    }

    /**
     * Wrapper around Spark halt method that formats message as JSON using {@link SparkUtils#formatJSON}. Exception
     */
    public static void haltWithMessage(
        Request request,
        int statusCode,
        String message,
        Exception e
    ) throws HaltException {
        JsonNode json = getObjectNode(message, statusCode, e);
        String logString = null;
        try {
            logString = "\n" + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
        } catch (JsonProcessingException jpe) {
            logString = message;
        }
        logRequestOrResponse(false, request, logString, statusCode);
        halt(statusCode, json.toString());
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

    /**
     * Log Spark requests.
     */
    public static void logRequest(Request request, Response response) {
        logRequestOrResponse(true, request, response);
    }

    /**
     * Log Spark responses.
     */
    public static void logResponse(Request request, Response response) {
        logRequestOrResponse(false, request, response);
    }

    /**
     * Log request/response.  Pretty print JSON if the content-type is JSON.
     */
    public static void logRequestOrResponse(boolean logRequest, Request request, Response response) {
        // NOTE: Do not attempt to read the body into a string until it has been determined that the content-type is
        // JSON.
        HttpServletResponse raw = response.raw();
        String bodyString = "";
        try {
            String contentType;
            if (logRequest) {
                contentType = request.contentType();
            } else {
                contentType = raw.getHeader("content-type");
            }
            if ("application/json".equals(contentType)) {
                bodyString = logRequest ? request.body() : response.body();
                if (bodyString != null) {
                    // Pretty print JSON if ContentType is JSON and body is not empty
                    JsonNode jsonNode = mapper.readTree(bodyString);
                    // Add new line for legibility when printing
                    bodyString = "\n" + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonNode);
                } else {
                    bodyString = "{body content is null}";
                }
            } else if (contentType != null) {
                bodyString = String.format("\nnon-JSON body type: %s", contentType);
            }
        } catch (IOException e) {
            LOG.warn("Could not parse JSON", e);
            bodyString = "\nBad JSON:\n" + bodyString;
        }
        logRequestOrResponse(logRequest, request, bodyString, raw.getStatus());
    }

    public static void logRequestOrResponse(
        boolean logRequest,
        Request request,
        String bodyString,
        int statusCode
    ) {
        Auth0UserProfile userProfile = request.attribute("user");
        String userEmail = userProfile != null ? userProfile.getEmail() : "no-auth";
        String queryString = request.queryParams().size() > 0 ? "?" + request.queryString() : "";
        LOG.info(
            "{} {} {}: {}{}{}{}",
            logRequest ? "req" : String.format("res (%s)", statusCode),
            userEmail,
            request.requestMethod(),
            BASE_URL,
            request.pathInfo(),
            queryString,
            trimLines(bodyString)
        );
    }

    private static String trimLines(String str) {
        if (str == null) return "";
        String[] lines = str.split("\n");
        if (lines.length <= DEFAULT_LINES_TO_PRINT) return str;
        return String.format(
            "%s \n...and %d more lines",
            String.join("\n", Arrays.copyOfRange(lines, 0, DEFAULT_LINES_TO_PRINT - 1)),
            lines.length - DEFAULT_LINES_TO_PRINT
        );
    }
}
