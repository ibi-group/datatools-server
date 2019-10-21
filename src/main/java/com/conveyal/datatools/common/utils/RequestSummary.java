package com.conveyal.datatools.common.utils;

import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import spark.Request;

import java.io.Serializable;
import java.util.Date;
import java.util.UUID;

/**
 * Provides a simple wrapper around a Spark {@link Request} for reporting info about recent requests to the UI.
 */
public class RequestSummary implements Serializable {
    public String id = UUID.randomUUID().toString();
    public String path;
    public String method;
    public String query;
    public String user;
    public long time;

    /** Create a summary from an incoming {@link spark.Request). */
    public static RequestSummary fromRequest (Request req) {
        RequestSummary requestSummary = new RequestSummary();
        requestSummary.time = new Date().getTime();
        requestSummary.path = req.pathInfo();
        requestSummary.method = req.requestMethod();
        requestSummary.query = req.queryString();
        Auth0UserProfile user = req.attribute("user");
        requestSummary.user = user != null ? user.getEmail() : null;
        return requestSummary;
    }

    /** Getter for time (used by Comparator). */
    public long getTime() {
        return time;
    }
}
