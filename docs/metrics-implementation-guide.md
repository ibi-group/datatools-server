# Metrics Implementation Guide

**Target:** Java 11 / Spark 2.9.4 / Micrometer (non-Spring)  
**Repository:** `datatools-server`  
**Status:** Step-by-step implementation reference.

---

## Table of Contents

1. [Settled Decisions](#1-settled-decisions)
2. [Implementation Checklist](#2-implementation-checklist)
3. [Metrics Endpoint (Prometheus Scrape)](#3-metrics-endpoint-prometheus-scrape)
4. [Health Endpoints](#4-health-endpoints)
5. [Inventory Gauges](#5-inventory-gauges)
6. [Job Hooks](#6-job-hooks)
7. [HTTP Hooks](#7-http-hooks)
8. [Build / Startup Info](#8-build--startup-info)
9. [Metric Naming Table](#9-metric-naming-table)
10. [Testing and Packaging](#10-testing-and-packaging)
11. [Recommended Implementation Order](#11-recommended-implementation-order)
12. [Out of Scope](#12-out-of-scope)
13. [Official Source Links](#13-official-source-links)

---

## 1. Settled Decisions

| Decision | Value |
|---|---|
| **Framework** | Non-Spring Micrometer |
| **BOM** | `micrometer-bom:1.17.0`, imported under Maven `dependencyManagement` |
| **Core** | `micrometer-core` (resolved transitively from the registry) |
| **Registry** | `micrometer-registry-prometheus` |
| **Import package** | `io.micrometer.prometheusmetrics` (not the legacy `io.micrometer.prometheus`) |
| **App port** | Configurable via `application.port` in `server.yml`; defaults to Spark default `4567` stored in `DataManager.PORT` |
| **Feature flag** | `application.metrics.enabled` — defaults to `false` (opt-in) |
| **Scrape endpoint** | `GET /metrics` — served over the same Spark HTTP port |
| **Restriction** | `/metrics` **must** be network-restricted externally (firewall / reverse-proxy). Do not expose to the public internet. |
| **Liveness** | `GET /health/live` |
| **Readiness** | `GET /health/ready` |
| **Scope** | Endpoints only — no Grafana, no Prometheus server deployment |

---

## 2. Implementation Checklist

Each stage ends with an observable checkpoint. Mark stages off as completed.

- [ ] **Stage 1 — Dependencies:** Declare BOM import and registry dependency; verify conflict-free `mvn dependency:tree`.  
  *Checkpoint:* `mvn compile` succeeds.
- [ ] **Stage 2 — Registry singleton:** Create `MetricsService` class holding `PrometheusMeterRegistry`.  
  *Checkpoint:* Unit test confirms `MetricsService.registry` is not null.
- [ ] **Stage 3 — JVM binders:** Attach `JvmGcMetrics`, `JvmMemoryMetrics`, `JvmThreadMetrics`, `ClassLoaderMetrics`, `ProcessorMetrics`.  
  *Checkpoint:* JVM metrics appear in registry output.
- [ ] **Stage 4 — `/metrics` route:** Register Spark `get("/metrics", ...)` with `PrometheusMeterRegistry::scrape`.  
  *Checkpoint:* `curl localhost:PORT/metrics` returns Prometheus text, content type `text/plain; version=0.0.4; charset=utf-8`.
- [ ] **Stage 5 — Feature flag guard:** Wrap route registration in `application.metrics.enabled` check.  
  *Checkpoint:* With flag `false`, `/metrics` returns 404.
- [ ] **Stage 6 — `/health/live` and `/health/ready`:** Register routes with liveness (always 200) and readiness (bounded DB checks).  
  *Checkpoint:* `curl /health/live` returns 200; `curl /health/ready` returns 200 when DBs are reachable, 503 when not.
- [ ] **Stage 7 — Inventory gauges:** `MetricsService` maintains `AtomicLong` snapshots refreshed on a daemon schedule; register `Gauge` once.  
  *Checkpoint:* Scrape output includes `datatools_feed_sources`, `datatools_feed_versions`, etc.
- [ ] **Stage 8 — Job hooks:** Instrument `MonitorableJob.run()` seams.  
  *Checkpoint:* Job counters increment on start/complete/error.
- [ ] **Stage 9 — HTTP hooks:** Instrument universal `before`/`after` filters in `DataManager.registerRoutes()`.  
  *Checkpoint:* Request duration and active count appear in metrics.
- [ ] **Stage 10 — Build/startup info:** `MetricsService` launches `MeterBinder` for version info.  
  *Checkpoint:* Scrape includes `datatools_build_info`.
- [ ] **Stage 11 — Config templates:** Add `application.metrics.enabled` to `server.yml.tmp` files.  
  *Checkpoint:* Config files parse without error.
- [ ] **Stage 12 — Tests:** Add focused unit/integration tests.  
  *Checkpoint:* `mvn test` passes.
- [ ] **Stage 13 — Shaded JAR validation:** Inspect final JAR for Micrometer service files.  
  *Checkpoint:* `jar tf target/dt-*.jar | grep micrometer` shows registry classes.

---

## 3. Metrics Endpoint (Prometheus Scrape)

### 3.1 Dependency Management

The worktree currently contains a Micrometer BOM declaration inside the regular `<dependencies>` block. Move the BOM to `<dependencyManagement>`; Maven's `import` scope only performs version management there. Keep the registry in `<dependencies>`:

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-bom</artifactId>
            <version>1.17.0</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
    <!-- existing application dependencies -->
</dependencies>
```

`micrometer-core` is resolved transitively via `micrometer-registry-prometheus`, so no explicit `<dependency>` is needed. The BOM at version `1.17.0` locks all Micrometer module versions.

**Verify with:**
```shell
mvn dependency:tree -Dincludes=io.micrometer
```

You should see `micrometer-core`, `micrometer-registry-prometheus`, and `micrometer-commons` all pinned to `1.17.0`.

### 3.2 Registry Ownership — `MetricsService`

Create a new class under `com.conveyal.datatools.manager.utils` (or `.manager.metrics`):

**`src/main/java/com/conveyal/datatools/manager/metrics/MetricsService.java`**

```java
package com.conveyal.datatools.manager.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Owns the single PrometheusMeterRegistry and all Micrometer bindings. */
public class MetricsService {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsService.class);

    private static final PrometheusMeterRegistry registry;

    static {
        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        // JVM metrics (standard Micrometer binders — no micrometer-jvm-extras needed)
        bind(new ClassLoaderMetrics());
        bind(new JvmMemoryMetrics());
        bind(new JvmGcMetrics());
        bind(new JvmThreadMetrics());
        bind(new ProcessorMetrics());
    }

    private static void bind(MeterBinder binder) {
        binder.bindTo(registry);
    }

    public static PrometheusMeterRegistry registry() {
        return registry;
    }

    /** Convenience accessor for adding custom gauges/timers. */
    public static MeterRegistry meterRegistry() {
        return registry;
    }
}
```

**Design notes:**

- The registry is a `static final` singleton. This avoids passing it through the class hierarchy. Prefer tests that construct an isolated registry or reset only test-owned custom meters; clearing the process-wide registry can remove JVM binders for later tests.
- JVM binders are activated once at class load. They register `jvm.*`, `process.*` metrics automatically.
- No XML or Spring involved. The `PrometheusConfig.DEFAULT` config uses reasonable defaults (export empty distributions, 1-step clock). No need for a custom config implementation in this app.

### 3.3 Spark Controller / Routes

Add a new controller (or inline routes in `DataManager.registerRoutes()`). The guide recommends a separate controller for clarity:

**`src/main/java/com/conveyal/datatools/manager/controllers/api/MetricsController.java`**

```java
package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.metrics.MetricsService;
import spark.Request;
import spark.Response;

import static spark.Spark.get;

public class MetricsController {

    private static final String METRICS_CONTENT_TYPE =
        "text/plain; version=0.0.4; charset=utf-8";

    /** GET /metrics — Prometheus scrape endpoint. */
    private static String getMetrics(Request req, Response res) {
        res.type(METRICS_CONTENT_TYPE);
        return MetricsService.registry().scrape();
    }

    public static void register() {
        get("/metrics", MetricsController::getMetrics);
    }
}
```

**Route registration placement in `DataManager.registerRoutes()`:**

Insert the metrics and health routes **before** the catch-all `get("/api/" + "*", ...)` and `get("/*", ...)` routes. The existing API auth and JSON/gzip filters do not match these root-level paths.

```java
// Pseudocode — place before the catch-all routes in DataManager.registerRoutes():
if (isMetricsEnabled()) {
    MetricsController.register();
}
// Health remains available independently of metrics collection.
HealthController.register();
```

**Why this placement?**

- The metrics route must precede the catch-all routes. Since `/metrics` does not match `API_PREFIX` (`/api/manager/`), it naturally escapes the API JSON/gzip filter regardless of that filter's registration position.
- The auth `before` filters at lines 272–281 only match `API_PREFIX + "secure/*"` and `EDITOR_API_PREFIX + "secure/*"`, so `/metrics` is **not** subject to authentication. This is intentional: Prometheus scrapers cannot carry Auth0 tokens. External network restriction is the enforcement mechanism.
- The `before` filter at line 328 (request logging) and `after` at line 335 (response logging) **will** apply to `/metrics` — this is acceptable.

**Tabular summary of filter applicability:**

| Filter | Applies to `/metrics`? | Notes |
|---|---|---|
| `CorsFilter.apply()` (Spark.after) | Yes | Harmless |
| `before(API_PREFIX + "secure/*", ...)` — auth | No | Path does not match |
| `before(EDITOR_API_PREFIX + "secure/*", ...)` — auth | No | Path does not match |
| `before(API_PREFIX + "*")` — JSON Content-Type / gzip | No | `/metrics` does not start with `/api/manager/` |
| `before(EDITOR_API_PREFIX + "*")` — JSON Content-Type / gzip | No | Path does not match |
| `before(...)` — request logging | Yes | Acceptable |
| `after(...)` — response logging | Yes | Acceptable |
| `exception(...)` — global error handler | Yes | Acceptable |

### 3.4 Feature Flag — Config Template

**`configurations/default/server.yml.tmp`** and **`configurations/test/server.yml.tmp`** — add under the `application:` block:

```yaml
application:
  # ... existing fields ...
  metrics:
    enabled: false      # opt-in; switch to true to expose /metrics
```

Use the settled dedicated property under `application:` rather than treating metrics as an optional application module:

```java
// Pseudocode for a new guard in DataManager or MetricsService.
// Check hasConfigProperty first because the default is disabled.
public static boolean isMetricsEnabled() {
    return hasConfigProperty("application.metrics.enabled")
        && "true".equals(getConfigPropertyAsText("application.metrics.enabled"));
}
```

### 3.5 Correct Prometheus Content Type

The response content type must be exactly:

```
text/plain; version=0.0.4; charset=utf-8
```

This is what Prometheus expects. The `PrometheusMeterRegistry.scrape()` output is already in the correct text format. Do not use `application/json`.

### 3.6 Minimal Scrape Smoke Test

After the endpoint is registered and `application.metrics.enabled` is `true`:

```shell
# Assuming default port 4567, or the configured application.port value:
curl -v http://localhost:4567/metrics
```

Expected output:
- HTTP `200 OK`
- `Content-Type: text/plain; version=0.0.4; charset=utf-8`
- Body contains lines like:

```
# HELP jvm_memory_used_bytes ...
# TYPE jvm_memory_used_bytes gauge
jvm_memory_used_bytes{area="heap",id="..."} 2.12345678E7
```

---

## 4. Health Endpoints

### 4.1 Liveness — `/health/live`

Always returns 200 if the server is alive. No external dependency check. This is the simplest health indicator.

```java
// Pseudocode for HealthController
private static String live(Request req, Response res) {
    res.type("application/json");
    return "{\"status\":\"UP\"}";
}
```

### 4.2 Readiness — `/health/ready`

Returns 200 when all critical downstream dependencies are reachable, 503 otherwise. **Never leak secrets, connection strings, or exception stack traces in the response body.**

Checks to perform (with **bounded timeouts**):

1. **MongoDB** — `Persistence.getMongoDatabase().runCommand(new Document("ping", 1))`. The 4.0.6 driver uses client-level server-selection and socket timeouts; `runCommand` does not accept a simple per-call two-second timeout. Verify that the configured Mongo connection applies suitably bounded `serverSelectionTimeoutMS` and socket/connect timeouts before relying on this check. Wrap the call in `try/catch`.

2. **PostgreSQL (GTFS data source)** — `GTFS_DATA_SOURCE.getConnection().isValid(2)`. Return the connection to the pool after the check (use try-with-resources).

**Response format (consistent with Kubernetes conventions):**

```json
// HTTP 200
{"status":"UP"}

// HTTP 503 — one or more dependencies down
{"status":"DOWN","checks":{"mongodb":{"status":"DOWN"},"postgresql":{"status":"UP"}}}
```

**Important:**

- Bound every health check. `Connection.isValid(2)` provides a JDBC timeout; MongoDB timeout behavior must be bounded in the shared client's settings. A stuck check will block readiness and delay pod rotation.
- Close the JDBC connection with try-with-resources so it returns to the pool. Do **not** close `Persistence`'s shared Mongo client or database from a request handler.
- Do not expose exception messages, stack traces, or database names in the response.
- **Do not conflate health with metrics.** The health endpoint checks liveness and readiness. It is not a Prometheus scrape endpoint, and it should not emit metrics.
- The health endpoint should be network-restricted similarly to `/metrics`. If that is not possible, at minimum it must not expose sensitive information.

### 4.3 Route Registration

```java
get("/health/live", HealthController::live);
get("/health/ready", HealthController::ready);
```

These routes, like `/metrics`, should be registered in `registerRoutes()` after the CORS filter and before the catch-all routes.

---

## 5. Inventory Gauges

### 5.1 Canonical Terms

| Correct term | Incorrect / ambiguous |
|---|---|
| Feed Source | Ambiguous “feed” or “feed count” |
| Feed Version | Ambiguous “feed” or “feed count” |
| Project | — |
| Organization | — |

### 5.2 Aggregates

Counts needed:

- `datatools_feed_sources` — current number of `FeedSource` documents in MongoDB
- `datatools_feed_versions` — current number of `FeedVersion` documents
- `datatools_projects` — current number of `Project` documents
- `datatools_organizations` — current number of `Organization` documents

### 5.3 Refresh Strategy — Cached AtomicLong

**Do not query MongoDB on every scrape.** The Prometheus scrape interval is typically 15–30 seconds; running `countDocuments` on every scrape is wasteful.

**Recommended approach:**

```java
public class MetricsService {
    private static final AtomicLong feedSourceCount = new AtomicLong(0);
    private static final AtomicLong feedVersionCount = new AtomicLong(0);
    private static final AtomicLong projectCount = new AtomicLong(0);
    private static final AtomicLong organizationCount = new AtomicLong(0);
    private static final AtomicLong lastRefreshEpoch = new AtomicLong(0);
    private static final AtomicLong refreshFailures = new AtomicLong(0);

    static {
        // ... JVM binders ...

        // Register gauges ONCE with references to the AtomicLong objects.
        // The Gauges will call .get() on every scrape.
        Gauge.builder("datatools.feed.sources", feedSourceCount, AtomicLong::get)
            .description("Total number of Feed Sources")
            .strongReference(true)
            .register(registry);
        Gauge.builder("datatools.feed.versions", feedVersionCount, AtomicLong::get)
            .description("Total number of Feed Versions")
            .strongReference(true)
            .register(registry);
        Gauge.builder("datatools.projects", projectCount, AtomicLong::get)
            .description("Total number of Projects")
            .strongReference(true)
            .register(registry);
        Gauge.builder("datatools.organizations", organizationCount, AtomicLong::get)
            .description("Total number of Organizations")
            .strongReference(true)
            .register(registry);

        // Also expose refresh age and failure count
        Gauge.builder("datatools.inventory.refresh.age.seconds",
                lastRefreshEpoch, e -> e.get() == 0
                    ? Double.NaN
                    : (System.currentTimeMillis() - e.get()) / 1000.0)
            .description("Seconds since last inventory refresh")
            .strongReference(true)
            .register(registry);
    }
}
```

**Why `Gauge.builder(name, obj, function)` and not a boxed number:**
The three-argument form reads a mutable state object on every scrape. Micrometer gauges normally hold only a weak reference to that object, so retain the `AtomicLong` in a long-lived field and use `.strongReference(true)` explicitly. Do **not** register `feedSourceCount.get()`; that boxed value cannot reflect later updates.

### 5.4 Refresh Schedule

Create a dedicated **daemon** single-threaded scheduler for inventory refresh. Do not reuse `Scheduler.schedulerService` (which is used for feed-fetch scheduling and uses a single thread that must not be blocked).

**Option A — Dedicated ScheduledExecutorService (recommended):**

```java
private static final ScheduledExecutorService inventoryScheduler =
    Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "metrics-inventory-refresh");
        t.setDaemon(true);
        return t;
    });

public static void startInventoryRefresh(long period, TimeUnit unit) {
    inventoryScheduler.scheduleAtFixedRate(MetricsService::refreshInventory,
        0, period, unit);
}
```

**Option B — Tradeoff with existing Scheduler:** The existing `Scheduler.schedulerService` is a single-thread pool that handles feed-fetch scheduling. Adding inventory refresh to it is not recommended because:

- A slow DB query during refresh could delay scheduled feed fetches.
- The Scheduler already has complex cancellation logic tied to `FeedSource` IDs.
- Separation of concerns: metrics infrastructure should not depend on feed-management scheduling.

**Recommendation:** Use an `Executors.newSingleThreadScheduledExecutor(r -> {...})` with daemon threads. It is a few lines of focused code and does not risk interfering with feed operations.

### 5.5 Refresh Implementation

```java
private static void refreshInventory() {
    try {
        // TypedPersistence.count(Bson) delegates to MongoCollection.countDocuments.
        Document allDocuments = new Document();
        feedSourceCount.set(Persistence.feedSources.count(allDocuments));
        feedVersionCount.set(Persistence.feedVersions.count(allDocuments));
        projectCount.set(Persistence.projects.count(allDocuments));
        organizationCount.set(Persistence.organizations.count(allDocuments));
        lastRefreshEpoch.set(System.currentTimeMillis());
    } catch (Exception e) {
        inventoryRefreshFailures.increment();
        LOG.warn("Inventory refresh failed", e);
    }
}
```

`TypedPersistence.count(Bson)` is the repository's existing abstraction (`TypedPersistence.java:88`); prefer it over reaching into `getMongoCollection()`.

### 5.6 Timer / Counter Adjuncts

Attach a Timer and a counter for each refresh:

```java
private static final Timer inventoryRefreshTimer =
    Timer.builder("datatools.inventory.refresh.duration")
        .description("Duration of inventory refresh")
        .register(registry);
private static final Counter inventoryRefreshCounter =
    Counter.builder("datatools.inventory.refresh.attempts")
        .description("Number of inventory refresh attempts")
        .register(registry);
private static final Counter inventoryRefreshFailures =
    Counter.builder("datatools.inventory.refresh.failures")
        .description("Number of failed inventory refresh attempts")
        .register(registry);
```

Wrap the refresh body in `inventoryRefreshTimer.record(() -> { ... })` and increment the counter at start.

---

## 6. Job Hooks

### 6.1 `MonitorableJob.run()` Seams

The `run()` method at line 158 of `MonitorableJob.java` is the single entry point for all monitorable jobs. Instrumentation should be added **inside** `run()` because:

- The method is `final` (cannot be overridden).
- It already has a try/catch/finally structure.
- All job subtypes (`FeedSourceJob`, `FeedVersionJob`, standalone jobs) converge here.

The instrumentation should wrap the existing try block and the finally block. Because this method is already the canonical sequence, **do not modify `run()` itself in this guide**. Instead, the guide describes the hooks to add. Actual implementation should add these calls at the indicated seams.

**Pseudocode for the modified `run()` (seams marked with `// >>>`):**

```java
public void run() {
    active = true;
    boolean parentJobErrored = false;
    boolean subTaskErrored = false;
    String cancelMessage = "";

    // >>> COUNT JOB START
    MetricsService.jobStarted(type);

    try {
        // First execute the core logic...
        jobLogic();
        // ... [existing code unchanged] ...

        // >>> On success (after sub-jobs complete and before jobFinished)
        // This is in the finally block pathway — see below.

    } catch (Exception e) {
        status.fail("Job failed due to unhandled exception!", e);
        // >>> COUNT JOB ERROR
        MetricsService.jobErrored(type);
    } finally {
        LOG.info("...");
        active = false;
        // >>> COUNT JOB COMPLETION + RECORD DURATION
        MetricsService.jobCompleted(type, status.error, status.duration);
    }
}
```

**Better approach — wrap the outcome using existing `status.error` and `status.duration` in `finally`:**

```java
} finally {
    LOG.info("{} (jobId={}) {} in {} ms", type, jobId,
        status.error ? "errored" : "completed", status.duration);
    active = false;
    // Single hook point:
    MetricsService.recordJobOutcome(type, status.error, status.duration);
}
```

### 6.2 Counters and Timer

```java
// In MetricsService:

// job_type label must be bounded — use MonitorableJob.JobType enum values.
private static final Counter jobStartedCounter =
    Counter.builder("datatools.jobs.started")
        .description("Jobs started")
        .tag("job_type", "unknown")
        .register(registry);

private static final Counter jobCompletedCounter =
    Counter.builder("datatools.jobs.completed")
        .description("Jobs completed")
        .tag("job_type", "unknown")
        .tag("status", "success")
        .register(registry);

private static final Timer jobDurationTimer =
    Timer.builder("datatools.jobs.duration")
        .description("Job execution duration")
        .tag("job_type", "unknown")
        .tag("status", "success")
        .publishPercentiles(0.5, 0.95, 0.99)
        .register(registry);

// Active jobs gauge — uses a tracked Set
private static final ConcurrentMap<String, AtomicInteger> activeJobsByType = new ConcurrentHashMap<>();

// Staged jobs gauge — uses a tracked Set
// A staged job is one registered but not yet active.
// Since MonitorableJob sets active=false and only sets it true in run(),
// the window between construction and run() is "staged".
// For simplicity, this guide stages a single gauge: "staged" = registered,
// active is tracked below; gauge is derived: staged = registered - active.

// Register once in static initializer:
Gauge.builder("datatools.jobs.active", activeJobsByType, map -> {
        // Sum all values
        return map.values().stream().mapToInt(AtomicInteger::get).sum();
    })
    .description("Currently active jobs")
    .register(registry);

// Static helpers:
public static void jobStarted(MonitorableJob.JobType type) {
    String label = type.name().toLowerCase();
    jobStartedCounter.increment();
    // Increment active for this type
    activeJobsByType
        .computeIfAbsent(label, k -> new AtomicInteger(0))
        .incrementAndGet();
}

public static void recordJobOutcome(MonitorableJob.JobType type, boolean errored, long durationMillis) {
    String label = type.name().toLowerCase();
    String status = errored ? "error" : "success";
    jobCompletedCounter.increment();
    jobDurationTimer.record(durationMillis, TimeUnit.MILLISECONDS);
    // Decrement active for this type
    AtomicInteger counter = activeJobsByType.get(label);
    if (counter != null) counter.decrementAndGet();
}
```

### 6.3 Label Cardinality Warning

The `job_type` label is derived from the `MonitorableJob.JobType` enum. This enum currently has ~25 values, which is acceptable cardinality. **Do not add** labels for:

- User / owner
- Feed Source ID
- Job ID (UUID — infinite cardinality)
- Class name (unbounded if subclasses proliferate)

If a job type is not represented in the enum, it defaults to `UNKNOWN_TYPE`. Map the label to `.name().toLowerCase()`.

### 6.4 Executor Queue / Active Metrics and `ThreadPoolExecutor` Refactor

`JobUtils.heavyExecutor` and `JobUtils.lightExecutor` are declared as `Executor`, not `ThreadPoolExecutor`:

```java
public static Executor heavyExecutor = Executors.newFixedThreadPool(4);
public static Executor lightExecutor = Executors.newSingleThreadExecutor();
```

To expose queue depth, pool size, and active thread count, these must be `ThreadPoolExecutor` instances.

**Recommended refactor (type change only):**

```java
public static ThreadPoolExecutor heavyExecutor =
    (ThreadPoolExecutor) Executors.newFixedThreadPool(4);
public static ThreadPoolExecutor lightExecutor =
    (ThreadPoolExecutor) Executors.newSingleThreadExecutor();
```

**Risk:** Tests that mock or substitute `heavyExecutor`/`lightExecutor` (e.g., with `Executors.newSingleThreadExecutor()`) may need updating if they cast to `Executor` only. A grep for `JobUtils.heavyExecutor` and `JobUtils.lightExecutor` across `src/test/` will reveal any assumptions. The fields are declared `public static`, so any test using a custom executor will need to reassign with a `ThreadPoolExecutor`.

**Alternative (lower risk):** Keep the `Executor`-typed fields and add a separate getter for the `ThreadPoolExecutor`:

```java
public static ThreadPoolExecutor getHeavyExecutor() {
    return (ThreadPoolExecutor) heavyExecutor;
}
```

This lets existing callers continue using `heavyExecutor.execute(...)` while metrics code can obtain the pool details.

**Once the executor is available as `ThreadPoolExecutor`, register Micrometer `ExecutorServiceMetrics`:**

```java
ExecutorServiceMetrics.monitor(
    registry, heavyExecutor, "datatools.executor.heavy",
    new String[0]);
ExecutorServiceMetrics.monitor(
    registry, lightExecutor, "datatools.executor.light",
    new String[0]);
```

This adds gauges for `queue_depth`, `active`, `pool_size`, `completed` with the given name prefix.

---

## 7. HTTP Hooks

### 7.1 Existing Filters in `DataManager`

Two universal filters are registered at the end of `registerRoutes()`:

```java
before((request, response) -> {
    RequestSummary summary = RequestSummary.fromRequest(request);
    lastRequestForUser.put(summary.user, summary);
    logRequest(request, response);
});

after((request, response) -> {
    logResponse(request, response);
});
```

These apply to **all** routes (`/*`). They are the natural place to add HTTP metrics instrumentation.

### 7.2 Timer.Sample via Request Attribute

**Do not use static ThreadLocal.** Spark request attributes are the correct mechanism:

```java
// Before filter:
before((request, response) -> {
    // ... existing logging ...
    Timer.Sample sample = Timer.start(MetricsService.meterRegistry());
    request.attribute("http-timer-sample", sample);
});

// After filter:
after((request, response) -> {
    Timer.Sample sample = request.attribute("http-timer-sample");
    if (sample != null) {
        int statusClass = response.status() / 100;   // e.g., 2, 4, 5
        String method = request.requestMethod();      // GET, POST, etc.
        // Do NOT use request.pathInfo() as a label — see §7.4
        sample.stop(Timer.builder("datatools.http.requests")
            .tag("status_class", String.valueOf(statusClass))
            .tag("method", method)
            .register(MetricsService.meterRegistry()));
    }
    // ... existing logging ...
});
```

### 7.3 Active Request Gauge

```java
private static final AtomicInteger activeRequests = new AtomicInteger(0);

// In static initializer:
Gauge.builder("datatools.http.active", activeRequests, AtomicInteger::get)
    .description("Currently in-flight HTTP requests")
    .register(registry);

// In before filter:
activeRequests.incrementAndGet();

// In after filter:
activeRequests.decrementAndGet();
```

### 7.4 Path Label — Spark 2.9.4 Limitation

Spark 2.9.4 does not expose the route template (e.g., `/api/manager/secure/feedsources/:id`) in `Request`. Available methods are `pathInfo()` (the actual path, e.g., `/api/manager/secure/feedsources/abc123`), `uri()`, and `url()`.

**Do not use `pathInfo()` as a label.** Raw paths produce unbounded cardinality (every `:id` value creates a new time series).

**Three options (choose one):**

| Option | Cardinality | Effort | Recommendation |
|---|---|---|---|
| **A. No path label** (safe default) | None | Minimal | Recommended for initial implementation |
| **B. Normalized route group** | ~5–10 groups | Low | Parse `pathInfo()` into groups like `feedsources`, `feedversions`, `projects`, `deployments`, `status` |
| **C. Per-controller wrappers** | ~20–30 | Medium | Each controller registers its own `before`/`after` with a hardcoded route name |

**Option A is the recommended starting point** — omit the path label entirely and track by status class and method only. This is safe, simple, and avoids any cardinality risk.

**Option B detail:** Extract the first two path segments after `/api/manager/`:

```java
String path = request.pathInfo();  // e.g., "/api/manager/secure/feedsources/abc123"
String[] segments = path.split("/");
String group = (segments.length >= 4) ? segments[3] : "other";
// "secure" is at [2], entity name at [3]
```

Map `segments[3]` to a finite set (the entity names). Reject anything outside the known set → label value `other`. This keeps cardinality bounded.

**Option C detail:** Each `*Controller.register()` method wraps its route handling with its own `before`/`after` using Spark's path-specific filters. This requires more code but gives precise route labels and is consistent with the existing pattern in `StatusController`, `FeedSourceController`, etc.

**Mark for local verification:** Test that `request.pathInfo()` returns the expected raw path (not the template) in Spark 2.9.4. Confirm the behavior with a quick integration test.

---

## 8. Build / Startup Info

### 8.1 Build Information from `DataManager`

`DataManager` holds:

- `repoUrl` — loaded from `.properties` (line 167)
- `commit` — loaded from `git.properties` (line 179), produced by `git-commit-id-plugin`
- `PORT` — configured port (line 95)
- `serverStartTime` — local variable in `main()` (line 105) — not stored as a static field

### 8.2 Info Meter

Use Micrometer's `MeterRegistry.config().commonTags()` or a `Gauge` of constant value 1 that carries version info as tags. The latter is the Prometheus-friendly approach:

```java
// In MetricsService (added to static block):
Gauge.builder("datatools.build.info", () -> 1)
    .description("Build information (constant 1)")
    .tag("repo_url", DataManager.repoUrl != null ? DataManager.repoUrl : "unknown")
    .tag("commit", DataManager.commit != null ? DataManager.commit : "unknown")
    .tag("port", String.valueOf(DataManager.PORT))
    .register(registry);
```

**Promoting `serverStartTime` to a static field:**

In `DataManager.java`, change the local variable to a public static field:

```java
// Add near other static fields:
public static long serverStartTimeMillis;

// In main() before initializeApplication:
serverStartTimeMillis = System.currentTimeMillis();
```

Then add a gauge for uptime:

```java
Gauge.builder("datatools.uptime.seconds",
        DataManager.serverStartTimeMillis,
        start -> (System.currentTimeMillis() - start) / 1000.0)
    .description("Server uptime in seconds")
    .register(registry);
```

### 8.3 Cardinality Warning

The build info gauge carries `repo_url` and `commit` as tags. This is acceptable cardinality (one time series per deployment). Do not add build number, CI run ID, or hostname as tags.

---

## 9. Metric Naming Table

All custom metrics use the `datatools_` prefix in their Prometheus-normalized name. Standard Micrometer JVM/process metrics keep their standard names (`jvm_*`, `process_*`).

| Prometheus Name | Micrometer Name | Type | Labels | Source |
|---|---|---|---|---|
| `jvm_memory_used_bytes` | `jvm.memory.used` | Gauge | `{area, id}` | `JvmHeapMemoryMetrics` |
| `jvm_memory_max_bytes` | `jvm.memory.max` | Gauge | `{area, id}` | `JvmHeapMemoryMetrics` |
| `jvm_gc_pause_seconds` | `jvm.gc.pause` | Timer (base unit seconds) | `{action, cause}` | `JvmGcMetrics` |
| `jvm_threads_live_threads` | `jvm.threads.live` | Gauge | — | `JvmThreadMetrics` |
| `jvm_classes_loaded_classes` | `jvm.classes.loaded` | Gauge | — | `ClassLoaderMetrics` |
| `process_cpu_usage` | `process.cpu.usage` | Gauge | — | `ProcessorMetrics` |
| `system_cpu_count` | `system.cpu.count` | Gauge | — | `ProcessorMetrics` |
| `datatools_feed_sources_total` | `datatools.feed.sources` | Gauge | — | Inventory refresh (`Persistence`) |
| `datatools_feed_versions_total` | `datatools.feed.versions` | Gauge | — | Inventory refresh |
| `datatools_projects_total` | `datatools.projects` | Gauge | — | Inventory refresh |
| `datatools_organizations_total` | `datatools.organizations` | Gauge | — | Inventory refresh |
| `datatools_inventory_refresh_age_seconds` | `datatools.inventory.refresh.age.seconds` | Gauge | — | Inventory refresh |
| `datatools_inventory_refresh_failures_total` | `datatools.inventory.refresh.failures` | Gauge | — | Inventory refresh |
| `datatools_inventory_refresh_duration_seconds` | `datatools.inventory.refresh.duration` | Timer | — | Inventory refresh |
| `datatools_inventory_refresh_attempts_total` | `datatools.inventory.refresh.attempts` | Counter | — | Inventory refresh |
| `datatools_jobs_started_total` | `datatools.jobs.started` | Counter | `{job_type}` | `MonitorableJob.run()` |
| `datatools_jobs_completed_total` | `datatools.jobs.completed` | Counter | `{job_type, status}` | `MonitorableJob.run()` |
| `datatools_jobs_duration_seconds` | `datatools.jobs.duration` | Timer | `{job_type, status}` | `MonitorableJob.run()` |
| `datatools_jobs_active` | `datatools.jobs.active` | Gauge | `{job_type}` | `MonitorableJob.active` |
| `datatools_http_requests_duration_seconds` | `datatools.http.requests` | Timer | `{status_class, method}` | `before`/`after` filters |
| `datatools_http_active` | `datatools.http.active` | Gauge | — | `before`/`after` filters |
| `datatools_executor_heavy_*` | `datatools.executor.heavy.*` | Various | — | `ExecutorServiceMetrics` |
| `datatools_executor_light_*` | `datatools.executor.light.*` | Various | — | `ExecutorServiceMetrics` |
| `datatools_build_info` | `datatools.build.info` | Gauge (constant 1) | `{repo_url, commit, port}` | `DataManager` static fields |
| `datatools_uptime_seconds` | `datatools.uptime.seconds` | Gauge | — | `serverStartTime` |

**Naming rules applied:**

- Prometheus convention: lowercase, underscore-separated, `_total` suffix for counters only, `_seconds` suffix for base-unit-seconds.
- Micrometer names use dot notation; Prometheus normalizer converts to underscore.
- The `_total` suffix is added automatically by the Prometheus naming convention for Counter type. For consistency, inventory gauges are not named `_total` (they are not counters — they can go up and down). `_total` is used for job started/completed counters and refresh attempts counter.
- Do not add suffix `_total` to a Gauge type. Micrometer's Prometheus naming convention only appends `_total` to `Counter`-type meters.

---

## 10. Testing and Packaging

### 10.1 Test File Templates to Copy

Copy the pattern from existing tests:

| Existing pattern | New file |
|---|---|
| `AppInfoControllerTest.java` | `MetricsControllerTest.java` |
| `PersistenceTest.java` | `InventoryGaugeTest.java` |
| — | `MetricsServiceTest.java` |

All tests should extend `UnitTest` (which skips under E2E) and call `DatatoolsTest.setUp()`.

### 10.2 Focused Unit Tests

**`MetricsServiceTest.java`:**

```java
class MetricsServiceTest extends UnitTest {
    @BeforeAll
    static void setUp() throws Exception {
        DatatoolsTest.setUp();
    }

    @Test
    void registryIsPresent() {
        assertNotNull(MetricsService.registry());
    }

    @Test
    void jvmMetricsPresent() {
        String scrape = MetricsService.registry().scrape();
        assertTrue(scrape.contains("jvm_memory_used_bytes"));
        assertTrue(scrape.contains("jvm_gc_pause_seconds"));
        assertTrue(scrape.contains("jvm_threads_live_threads"));
    }

    @Test
    void scrapeOutputIsValidPrometheusFormat() {
        String scrape = MetricsService.registry().scrape();
        assertTrue(scrape.startsWith("# HELP"));
    }
}
```

**`MetricsControllerTest.java`** (if using an integration test that starts Spark; otherwise this is a manual smoke test):

This test requires Spark to be running. Since `DatatoolsTest.setUp()` already calls `DataManager.main(args)`, a full integration test can use `RestAssured`:

```java
class MetricsControllerTest extends UnitTest {
    @BeforeAll
    static void setUp() throws Exception {
        DatatoolsTest.setUp();
    }

    @Test
    void metricsEndpoint() {
        given()
            .port(DataManager.PORT)
        .when()
            .get("/metrics")
        .then()
            .statusCode(200)
            .contentType(containsString("text/plain"));
    }
}
```

**`InventoryGaugeTest.java`:**

```java
class InventoryGaugeTest extends UnitTest {
    @BeforeAll
    static void setUp() throws Exception {
        DatatoolsTest.setUp();
    }

    @Test
    void inventoryGaugesPresent() {
        String scrape = MetricsService.registry().scrape();
        assertTrue(scrape.contains("datatools_feed_sources_total"));
        assertTrue(scrape.contains("datatools_feed_versions_total"));
    }
}
```

### 10.3 Maven Commands

```shell
# Verify dependency tree
mvn dependency:tree -Dincludes=io.micrometer

# Compile only
mvn compile

# Run all tests (unit tests skip if RUN_E2E=true)
mvn test

# Package the shaded JAR
mvn package -DskipTests
```

### 10.4 Shaded JAR Service Resource Inspection

The `maven-shade-plugin` 2.2 is configured with `ServicesResourceTransformer` (line 76 of `pom.xml`). This merges `META-INF/services` files from all JARs, which includes Micrometer's `MeterBinder` service files. **No changes to the shade plugin configuration are required** unless the merge fails.

**Inspect the final JAR:**

```shell
# List the actual shaded JAR
ls -la target/dt-*.jar

# Inspect for Micrometer service files
jar tf target/dt-*.jar | grep -i micrometer

# Check META-INF/services for MeterBinder
jar tf target/dt-*.jar | grep "META-INF/services"
```

Expected output includes:
```
io/micrometer/
io/micrometer/prometheusmetrics/
META-INF/services/io.micrometer.core.instrument.binder.MeterBinder
```

If `MeterBinder` services are missing, the `ServicesResourceTransformer` may not be picking them up. In that case, verify the transformer is the first and only transformer in the `<transformers>` block. The current config has a single transformer (line 76), which is correct.

**Do not upgrade `maven-shade-plugin` from 2.2 unless validation fails.** If you encounter a `SecurityException` related to signed JARs, the existing `<filters>` that exclude `META-INF/*.SF`, `*.DSA`, `*.RSA` (lines 83–88) already handle this.

### 10.5 Launch and Curl Smoke Test

```shell
# Start the server
java -jar target/dt-*.jar configurations/default/env.yml configurations/default/server.yml

# In another terminal:
# Metrics endpoint
curl -v http://localhost:4567/metrics

# Liveness
curl -v http://localhost:4567/health/live

# Readiness
curl -v http://localhost:4567/health/ready
```

---

## 11. Recommended Implementation Order

1. **Create `MetricsService.java`** — registry, JVM binders, `scrape()` accessor.
2. **Add metrics feature flag** to config templates — `application.metrics.enabled: false`.
3. **Create `MetricsController.java`** — `/metrics` route with guard.
4. **Register routes** in `DataManager.registerRoutes()`.
5. **Scrape smoke test** — verify `curl localhost:PORT/metrics` returns Prometheus text.
6. **Create `HealthController.java`** — `/health/live` and `/health/ready` with bounded MongoDB and PostgreSQL checks.
7. **Inventory refresh** — AtomicLong gauges, dedicated daemon scheduler, refresh logic.
8. **Job hooks** — add `MetricsService` calls to `MonitorableJob.run()`.
9. **Executor metrics** — refactor `JobUtils` executors to `ThreadPoolExecutor`, add `ExecutorServiceMetrics`.
10. **HTTP hooks** — add `Timer.Sample` and active request gauge to `before`/`after` filters.
11. **Build / startup info** — promote `serverStartTime` to static field, add info and uptime gauges.
12. **Config templates** — update both `configurations/default/server.yml.tmp` and `configurations/test/server.yml.tmp`.
13. **Tests** — add `MetricsServiceTest`, `MetricsControllerTest`, `InventoryGaugeTest`.
14. **Final validation** — `mvn clean package`, inspect shaded JAR, launch, and curl.

---

## 12. Out of Scope

The following are explicitly **not** part of this implementation:

- **Grafana or any dashboard** — no dashboard JSON, no provisioning config.
- **Prometheus server deployment** — no `docker-compose` or `prometheus.yml` shipping.
- **Per-tenant / per-organization labels** — multi-tenant label breakdown is a future concern.
- **Raw path labels on HTTP metrics** — see §7.4; never use `request.pathInfo()` directly as a label.
- **OpenTelemetry migration** — this is a Micrometer + Prometheus native implementation. No OTel SDK, no OTel exporter.
- **Modifying Maven Shade plugin version or configuration** — the existing `maven-shade-plugin:2.2` with `ServicesResourceTransformer` works. Only upgrade if validation fails.
- **`micrometer-jvm-extras`** — not required. The standard `micrometer-core` binders cover the standard JVM metrics. The `micrometer-jvm-extras` library provides non-standard metrics like memory pools, and can be evaluated separately.
- **Per-class-name labels** — do not emit the job's `getClass().getSimpleName()` as a label; use the `JobType` enum.
- **`gauge(name, primitive)`** — never register a gauge by passing a boxed primitive. Always use the three-argument form with a state object.

---

## 13. Official Source Links

| Resource | URL |
|---|---|
| Micrometer install / BOM | https://micrometer.io/docs/ |
| Prometheus registry (1.17.x) | https://github.com/micrometer-metrics/micrometer/tree/v1.17.0/implementations/micrometer-registry-prometheus |
| JVM metrics (MeterBinder list) | https://github.com/micrometer-metrics/micrometer/tree/v1.17.0/micrometer-core/src/main/java/io/micrometer/core/instrument/binder/jvm |
| Micrometer support policy | https://micrometer.io/docs/support |
| Prometheus naming best practices | https://prometheus.io/docs/practices/naming/ |
| Prometheus metric and label naming | https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels |
| Prometheus cardinality guidance | https://prometheus.io/docs/practices/instrumentation/#cardinality |
| Spark 2.9.4 Request API | https://javadoc.io/doc/com.sparkjava/spark-core/2.9.4/spark/Request.html |
| MongoDB 4.0.6 driver `countDocuments` | https://mongodb.github.io/mongo-java-driver/4.0/apidocs/mongodb-driver-sync/com/mongodb/client/MongoCollection.html#countDocuments-- |

---

*End of implementation guide.*
