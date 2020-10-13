package com.conveyal.datatools.common.utils.aws;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.conveyal.datatools.common.utils.ExpiringAsset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * This abstract class provides a framework for managing the creation of AWS Clients. Three types of clients are stored
 * in this class:
 * 1. A default client to use when not requesting a client using a specific role and/or region
 * 2. A client to use when using a specific region, but not with a role
 * 3. A client to use with a specific role and region combination (including null regions)
 *
 * The {@link AWSClientManager#getClient(String, String)} handles the creation and caching of clients based on the given
 * role and region inputs.
 */
public abstract class AWSClientManager<T> {
    private static final Logger LOG = LoggerFactory.getLogger(AWSClientManager.class);

    private static final long DEFAULT_EXPIRING_AWS_ASSET_VALID_DURATION_MILLIS = 800 * 1000;
    private static final HashMap<String, ExpiringAsset<AWSStaticCredentialsProvider>> crendentialsProvidersByRole =
        new HashMap<>();

    protected final T defaultClient;
    private final HashMap<String, T> nonRoleClientsByRegion = new HashMap<>();
    private final HashMap<String, ExpiringAsset<T>> clientsByRoleAndRegion = new HashMap<>();

    public AWSClientManager (T defaultClient) {
        this.defaultClient = defaultClient;
    }

    /**
     * Create credentials for a new session for the provided IAM role. The primary AWS account for the Data Tools
     * application must be able to assume this role (e.g., through delegating access via an account IAM role
     * https://docs.aws.amazon.com/IAM/latest/UserGuide/tutorial_cross-account-with-roles.html). The credentials can be
     * then used for creating a temporary client.
     */
    private static ExpiringAsset<AWSStaticCredentialsProvider> getCredentialsForRole(
        String role
    ) throws CheckedAWSException {
        String roleSessionName = "data-tools-session";
        // check if an active credentials provider exists for this role
        ExpiringAsset<AWSStaticCredentialsProvider> session = crendentialsProvidersByRole.get(role);
        if (session != null && session.isActive()) {
            LOG.debug("Returning active role-based session credentials");
            return session;
        }
        // either a session hasn't been created or an existing one has expired. Create a new session.
        STSAssumeRoleSessionCredentialsProvider sessionProvider = new STSAssumeRoleSessionCredentialsProvider
            .Builder(
                role,
                roleSessionName
            )
            .build();
        AWSSessionCredentials credentials;
        try {
            credentials = sessionProvider.getCredentials();
        } catch (AmazonServiceException e) {
            throw new CheckedAWSException("Failed to obtain AWS credentials");
        }
        LOG.info("Successfully created role-based session credentials");
        AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(
            new BasicSessionCredentials(
                credentials.getAWSAccessKeyId(),
                credentials.getAWSSecretKey(),
                credentials.getSessionToken()
            )
        );
        session = new ExpiringAsset<>(credentialsProvider, DEFAULT_EXPIRING_AWS_ASSET_VALID_DURATION_MILLIS);
        // store the credentials provider in a lookup by role for future use
        crendentialsProvidersByRole.put(role, session);
        return session;
    }

    /**
     * An abstract method where the implementation will create a client with the specified region, but not with a role.
     */
    public abstract T buildDefaultClientWithRegion(String region);

    /**
     * An abstract method where the implementation will create a client with the specified role and region.
     */
    protected abstract T buildCredentialedClientForRoleAndRegion(
        AWSCredentialsProvider credentials, String region, String role
    ) throws CheckedAWSException;

    /**
     * Obtain a potentially cached AWS client for the provided role ARN and region. If the role and region are null, the
     * default AWS client will be used. If just the role is null a cached client configured for the specified
     * region will be returned. For clients that require using a role, a client will be obtained (either via a cache or
     * by creation and then insertion into the cache) that has obtained the proper credentials.
     */
    public T getClient(String role, String region) throws CheckedAWSException {
        // return default client for null region and role
        if (role == null && region == null) {
            LOG.debug("Using default {} client", getClientClassName());
            return defaultClient;
        }

        // if the role is null, return a potentially cached EC2 client with the region configured
        T client;
        if (role == null) {
            client = nonRoleClientsByRegion.get(region);
            if (client == null) {
                client = buildDefaultClientWithRegion(region);
                LOG.info("Successfully built a {} client for region {}", getClientClassName(), region);
                nonRoleClientsByRegion.put(region, client);
            }
            LOG.debug("Using a non-role based {} client for region {}", getClientClassName(), region);
            return client;
        }

        // check for the availability of a client already associated with the given role and region
        String roleRegionKey = makeRoleRegionKey(role, region);
        ExpiringAsset<T> clientWithRole = clientsByRoleAndRegion.get(roleRegionKey);
        if (clientWithRole != null && clientWithRole.isActive()) {
            LOG.debug("Using previously created role-based {} client", getClientClassName());
            return clientWithRole.asset;
        }

        // Either a new client hasn't been created or it has expired. Create a new client and cache it.
        ExpiringAsset<AWSStaticCredentialsProvider> session = getCredentialsForRole(role);
        T credentialedClientForRoleAndRegion = buildCredentialedClientForRoleAndRegion(session.asset, region, role);
        LOG.info("Successfully created role-based {} client", getClientClassName());
        clientsByRoleAndRegion.put(
            roleRegionKey, 
            new ExpiringAsset<>(credentialedClientForRoleAndRegion, session.timeRemainingMillis())
        );
        return credentialedClientForRoleAndRegion;
    }

    private String getClientClassName() {
        return defaultClient.getClass().getSimpleName();
    }

    private static String makeRoleRegionKey(String role, String region) {
        return String.format("role=%s,region=%s", role, region);
    }
}
