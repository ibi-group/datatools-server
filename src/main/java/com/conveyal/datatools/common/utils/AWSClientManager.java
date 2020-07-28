package com.conveyal.datatools.common.utils;

import java.util.HashMap;

import static com.conveyal.datatools.common.utils.AWSUtils.DEFAULT_EXPIRING_AWS_ASSET_VALID_DURATION_MILLIS;

public abstract class AWSClientManager<T> {
    protected final T defaultClient;
    private HashMap<String, T> nonRoleClientsByRegion = new HashMap<>();
    private HashMap<String, ExpiringAsset<T>> clientsByRoleAndRegion = new HashMap<>();

    public AWSClientManager (T defaultClient) {
        this.defaultClient = defaultClient;
    }

    public abstract T buildDefaultClientWithRegion(String region);

    protected abstract T buildCredentialedClientForRoleAndRegion(String role, String region)
        throws NonRuntimeAWSException;

    /**
     * Obtain a potentially cached AWS client for the provided role ARN and region. If the role and region are null, the
     * default AWS client will be used. If just the role is null a cached client configured for the specified
     * region will be returned. For clients that require using a role, a client will be obtained (either via a cache or
     * by creation and then insertion into the cache) that has obtained the proper credentials.
     */
    public T getClient(String role, String region) throws NonRuntimeAWSException {
        // return default client for null region and role
        if (role == null && region == null) {
            return defaultClient;
        }

        // if the role is null, return a potentially cached EC2 client with the region configured
        T client;
        if (role == null) {
            client = nonRoleClientsByRegion.get(region);
            if (client == null) {
                client = buildDefaultClientWithRegion(region);
                nonRoleClientsByRegion.put(region, client);
            }
            return client;
        }

        // check for the availability of a EC2 client already associated with the given role and region
        String roleRegionKey = makeRoleRegionKey(role, region);
        ExpiringAsset<T> clientWithRole = clientsByRoleAndRegion.get(roleRegionKey);
        if (clientWithRole != null && clientWithRole.isActive()) return clientWithRole.asset;

        // Either a new client hasn't been created or it has expired. Create a new client and cache it.
        T credentialedClientForRoleAndRegion = buildCredentialedClientForRoleAndRegion(role, region);
        clientsByRoleAndRegion.put(
            roleRegionKey, 
            new ExpiringAsset<T>(credentialedClientForRoleAndRegion, DEFAULT_EXPIRING_AWS_ASSET_VALID_DURATION_MILLIS)
        );
        return credentialedClientForRoleAndRegion;
    }

    private static String makeRoleRegionKey(String role, String region) {
        return String.format("role=%s,region=%s", role, region);
    }
}
