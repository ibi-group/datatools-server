package com.conveyal.datatools.common.utils;

/**
 * A class that holds another variable and keeps track of whether the variable is still considered to be active (ie not
 * expired)
 */
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

    /**
     * @return the amount of time that the asset is still valid for in milliseconds.
     */
    public long timeRemainingMillis() {
        return expirationTimeMillis - System.currentTimeMillis();
    }
}
