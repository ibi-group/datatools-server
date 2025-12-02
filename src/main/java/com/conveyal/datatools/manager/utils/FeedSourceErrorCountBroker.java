package com.conveyal.datatools.manager.utils;

import com.conveyal.datatools.manager.models.FeedSourceSummary;
import com.conveyal.gtfs.graphql.fetchers.ErrorCountFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Broker to fetch error counts for multiple feed sources in parallel.
 */
public class FeedSourceErrorCountBroker {

    private static final Logger LOG = LoggerFactory.getLogger(FeedSourceErrorCountBroker.class);
    private static final int ERROR_COUNT_REQUEST_TIMEOUT_IN_SECONDS = 60;

    /**
     * Fetch error counts for all qualifying feed sources.
     */
    public static HashMap<String, List<ErrorCountFetcher.ErrorCount>> getErrorCountsForFeedSources(
        Collection<FeedSourceSummary> feedSourceSummaries
    ) {
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        try  {
            ConcurrentMap<String, CompletableFuture<List<ErrorCountFetcher.ErrorCount>>> errorCountTasks = assignErrorCountToFeedSource(
                feedSourceSummaries,
                executor
            );

            HashMap<String, List<ErrorCountFetcher.ErrorCount>> feedSourceErrorCounts = new HashMap<>();

            CompletableFuture<Void> allDone = CompletableFuture.allOf(
                errorCountTasks.values().toArray(new CompletableFuture[0])
            );

            try {
                allDone.get(ERROR_COUNT_REQUEST_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
            } catch (Exception e) {
                LOG.warn("Timeout or interruption while waiting for error counts.", e);
            }

            errorCountTasks.forEach((feedSourceId, future) -> {
                try {
                    List<ErrorCountFetcher.ErrorCount> errorCounts = future.getNow(null);
                    if (errorCounts != null) {
                        LOG.debug("Error counts for {}: {}", feedSourceId, errorCounts);
                        feedSourceErrorCounts.put(feedSourceId, errorCounts);
                    } else {
                        LOG.error("No error counts for {}.", feedSourceId);
                    }
                } catch (CompletionException e) {
                    LOG.error("Failed to get error counts for {}.", feedSourceId, e);
                }
            });

            return feedSourceErrorCounts;
        } catch (Exception e) {
            LOG.error("Exception during error count fetching.", e);
            return new HashMap<>();
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Asynchronously fetch error counts for feed sources that have a 'latest namespace'.
     */
    private static ConcurrentMap<String, CompletableFuture<List<ErrorCountFetcher.ErrorCount>>> assignErrorCountToFeedSource(
        Collection<FeedSourceSummary> feedSourceSummaries,
        ExecutorService executor
    ) {
        ErrorCountFetcher errorCountFetcher = new ErrorCountFetcher();
        return feedSourceSummaries
            .stream()
            .filter(fs -> fs.latestNamespace != null)
            .collect(Collectors.toConcurrentMap(
                fs -> fs.id,
                fs -> CompletableFuture.supplyAsync(() -> errorCountFetcher.getErrorCounts(fs.latestNamespace), executor))
            );
    }
}
