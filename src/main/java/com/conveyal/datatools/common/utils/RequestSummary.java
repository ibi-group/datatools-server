package com.conveyal.datatools.common.utils;

import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import spark.Request;

import java.util.Date;

public class RequestSummary {
    public String path;
    public String method;
    public String query;
    public String user;
    public long time;

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
}
