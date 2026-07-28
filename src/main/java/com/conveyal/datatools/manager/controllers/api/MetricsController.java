package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.metrics.MetricsService;

import spark.Request;
import spark.Response;

import static spark.Spark.get;

public class MetricsController {
    /** GET /metrics — Prometheus scrape endpoint. */
    private static String getMetrics(Request req, Response res) {
        res.type("text/plain; version=0.0.4; charset=utf-8");
        return MetricsService.registry().scrape();
    }

    public static void register() {
        get("/metrics", MetricsController::getMetrics);
    }
}
