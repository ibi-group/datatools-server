package com.conveyal.datatools.manager.metrics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.Document;

import com.conveyal.datatools.common.status.MonitorableJob.JobType;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadDeadlockMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

public class MetricsService {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsService.class);

    private static final PrometheusMeterRegistry registry;

    private static final AtomicLong feedSourceCount = new AtomicLong(0);
    private static final AtomicLong feedVersionCount = new AtomicLong(0);
    private static final AtomicLong projectCount = new AtomicLong(0);
    private static final AtomicLong organizationCount = new AtomicLong(0);
    private static final AtomicLong lastRefreshEpoch = new AtomicLong(0);
    private static final AtomicLong refreshFailures = new AtomicLong(0);

    private static final ConcurrentMap<JobType, Counter> failedJobsByType = new ConcurrentHashMap<>();
    private static final ConcurrentMap<JobType, Counter> completedJobsByType = new ConcurrentHashMap<>();
    private static final ConcurrentMap<JobType, Timer> jobDurationByType = new ConcurrentHashMap<>();
    private static Timer jobDuration;

    static {
        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        populateAndRegisterJobMetricsByType();

        new ClassLoaderMetrics().bindTo(registry);
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new JvmThreadDeadlockMetrics().bindTo(registry);

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
        Gauge.builder("datatools.inventory.refresh.failures", refreshFailures, AtomicLong::get)
            .description("Total number of failed Refreshes")
            .strongReference(true)
            .register(registry);
        Gauge.builder("datatools.inventory.refresh.age.seconds",
                lastRefreshEpoch, e -> e.get() == 0
                    ? Double.NaN
                    : (System.currentTimeMillis() - e.get()) / 1000.0)
            .description("Seconds since last inventory refresh")
            .strongReference(true)
            .register(registry);
    }

    private static void populateAndRegisterJobMetricsByType() {
        for(JobType type : JobType.values()) {
            failedJobsByType.put(type, Counter.builder("datatools.jobs."+type.name()+".failed")
                .description("Number of failed jobs of type "+type.name())
                .register(registry));
            completedJobsByType.put(type, Counter.builder("datatools.jobs."+type.name()+".completed")
                .description("Number of completed jobs of type "+type.name())
                .register(registry));
            jobDurationByType.put(type, Timer.builder("datatools.jobs."+type.name()+".duration")
                .description("Execution duration of jobs of type "+type.name())
                .register(registry));
        }

        Gauge.builder("datatools.jobs.failed", failedJobsByType,
                map -> map.values().stream().mapToDouble(Counter::count).sum())
            .description("Number of failed jobs")
            .register(registry);
        Gauge.builder("datatools.jobs.completed", completedJobsByType,
                map -> map.values().stream().mapToDouble(Counter::count).sum())
            .description("Number of completed jobs")
            .register(registry);
        jobDuration = Timer.builder("datatools.jobs.duration")
            .description("Job execution duration")
            .register(registry);
    }

    public static PrometheusMeterRegistry registry() {
        return registry;
    }

    public static void refreshInventory() {
        try {
            Document allDocuments = new Document();
            feedSourceCount.set(Persistence.feedSources.count(allDocuments));
            feedVersionCount.set(Persistence.feedVersions.count(allDocuments));
            projectCount.set(Persistence.projects.count(allDocuments));
            organizationCount.set(Persistence.organizations.count(allDocuments));
            lastRefreshEpoch.set(System.currentTimeMillis());
        } catch (Exception e) {
            refreshFailures.getAndIncrement();
			LOG.warn("Inventory refresh failed", e);
        }
    }

    public static void recordJobOutcome(JobType type, boolean error, long duration) {
        ConcurrentMap<JobType, Counter> outcomeCountersByType = error ? failedJobsByType : completedJobsByType;

        outcomeCountersByType.get(type).increment();
        jobDuration.record(duration, TimeUnit.MILLISECONDS);
        jobDurationByType.get(type).record(duration, TimeUnit.MILLISECONDS);
    }
}
