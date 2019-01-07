package com.conveyal.datatools.common.utils;

/**
 * Created by demory on 9/2/16.
 */

import spark.Spark;

import java.util.HashMap;

/**
 * Really simple helper for enabling CORS in a spark application;
 */

public final class CorsFilter {

    private static final HashMap<String, String> corsHeaders = new HashMap<String, String>();

    static {
        corsHeaders.put("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS");
        corsHeaders.put("Access-Control-Allow-Origin", "*");
        corsHeaders.put("Access-Control-Allow-Headers", "Content-Type,Authorization,X-Requested-With,Content-Length,Accept,Origin,");
        corsHeaders.put("Access-Control-Allow-Credentials", "true");
    }

    public final static void apply() {
        Spark.after((request, response) -> corsHeaders.forEach((key, value) -> response.header(key, value)));
    }
}