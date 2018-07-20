package com.conveyal.datatools.manager.persistence;

import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.r5.transit.TransportNetwork;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * This cache manages r5 TransportNetworks that are associated with FeedVersions in the application. There is a
 * TransportNetworkCache in r5, but it functions in a manner specific to analysis, so we need a special class here.
 *
 * WARNING: this is not necessarily built for scalable use of isochrone generation in the application, but rather as an
 * experimental approach to quickly generate isochrones for a GTFS feed.
 */
public class TransportNetworkCache {
    private static final Logger LOG = LoggerFactory.getLogger(TransportNetworkCache.class);
    private final LoadingCache<String, TransportNetwork> transportNetworkCache;
    private final Map<String, Long> loadedTransportNetworks = new HashMap<>();
    private static final int DEFAULT_CACHE_SIZE = 3;
    private static final long DEFAULT_DURATION = 2;
    private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MINUTES;
    public final int cacheSize;
    public final long duration;
    public final TimeUnit timeUnit;

    /**
     * Listens for removal from cache (due to expiration or size restriction) and sets transportNetwork to null for GC.
     */
    private final RemovalListener<String, GTFSFeed> removalListener = removalNotification -> {
        String feedVersionId = removalNotification.getKey();
        LOG.info("Evicting transport network. Cause: {}; ID: {}", removalNotification.getCause(), feedVersionId);
        loadedTransportNetworks.remove(feedVersionId);
    };

    public TransportNetworkCache (int cacheSize, long duration, TimeUnit timeUnit) {
        this.cacheSize = cacheSize;
        this.duration = duration;
        this.timeUnit = timeUnit;
        transportNetworkCache = CacheBuilder.newBuilder()
                .expireAfterAccess(10, TimeUnit.MINUTES)
                // TODO: Use maximumWeight instead of using maximumSize for better resource consumption estimate?
                .maximumSize(cacheSize)
                .removalListener(removalListener)
                .build(new CacheLoader() {
                    @Override
                    public TransportNetwork load(Object key) throws Exception {
                        // Thanks, java, for making me use a cast here. If I put generic arguments to new CacheLoader
                        // due to type erasure it can't be sure I'm using types correctly.
                        FeedVersion version = Persistence.feedVersions.getById((String) key);
                        if (version != null) {
                            return version.readTransportNetwork();
                        } else {
                            LOG.error("Version does not exist for id {}", key);
                            // This throws a CacheLoader$InvalidCacheLoadException
                            return null;
                        }
                    }
                });
    }

    public TransportNetworkCache () {
         this(DEFAULT_CACHE_SIZE, DEFAULT_DURATION, DEFAULT_TIME_UNIT);
    }

    /**
     * Wraps get method on cache to handle any exceptions.
     */
    public TransportNetwork getTransportNetwork (String feedVersionId) throws ExecutionException {
        try {
            loadedTransportNetworks.put(feedVersionId, System.currentTimeMillis());
            TransportNetwork tn = transportNetworkCache.get(feedVersionId);
            return tn;
        } catch (Exception e) {
            LOG.error("Could not read or build transport network for {}", feedVersionId);
            e.printStackTrace();
            throw e;
        }
    }

    public boolean containsTransportNetwork (String feedVersionId) {
        return loadedTransportNetworks.containsKey(feedVersionId);
    }

    public boolean isAtCapacity () {
        return cacheSize == loadedTransportNetworks.size();
    }

    /**
     * Calculates the time since the earliest active transport network was loaded into the cache, or returns
     * Long.MAX_VALUE if the cache is empty.
     */
    public long getTimeSinceEarliestLoad() {
        return loadedTransportNetworks.size() > 0
                ? loadedTransportNetworks.values().stream().min(Long::compare).get() - System.currentTimeMillis()
                : Long.MAX_VALUE;
    }
}
