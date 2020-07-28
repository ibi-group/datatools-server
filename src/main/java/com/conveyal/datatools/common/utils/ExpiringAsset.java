package com.conveyal.datatools.common.utils;

public class ExpiringAsset<T> {
    public final T asset;
    private final long expirationTimeMillis;

    public ExpiringAsset(T asset, long validDurationMillis) {
        this.asset = asset;
        this.expirationTimeMillis = System.currentTimeMillis() + validDurationMillis;
    }

    /**
     * @return true if the asset hasn't yet expired
     */
    public boolean isActive() {
        return expirationTimeMillis > System.currentTimeMillis();
    }
}
