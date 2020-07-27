package com.conveyal.datatools.common.utils;

public class ExpiringAsset<T> {
    public final T asset;
    private final long expirationTimeMillis;

    public ExpiringAsset(T asset, long expirationTimeMillis) {
        this.asset = asset;
        this.expirationTimeMillis = expirationTimeMillis;
    }

    /**
     * @return true if the asset hasn't yet expired
     */
    public boolean isActive() {
        return expirationTimeMillis > System.currentTimeMillis();
    }
}
