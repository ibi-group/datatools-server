package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.metrics.MetricsService;

import spark.Request;
import spark.Response;

import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;
import static com.conveyal.datatools.manager.DataManager.getConfigPropertyAsText;
import static spark.Spark.get;

public class MetricsController {
    private static final String METRICS_API_KEY_CONFIG = "modules.metrics.api_key";

    /** GET /metrics — Prometheus scrape endpoint. */
    private static String getMetrics(Request req, Response res) {
        String apiKey = getConfigPropertyAsText(METRICS_API_KEY_CONFIG);
        if (apiKey != null && !apiKey.isEmpty()) {
            String providedKey = req.headers("X-API-Key");
            if (providedKey == null) {
                providedKey = req.queryParams("api_key");
            }
            if (!apiKey.equals(providedKey)) {
                logMessageAndHalt(req, 401, "Invalid or missing API key");
            }
        }
        res.type("text/plain; version=0.0.4; charset=utf-8");
        return MetricsService.registry().scrape();
    }

    public static void register() {
        get("/metrics", MetricsController::getMetrics);
    }
}
