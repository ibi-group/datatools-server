package com.conveyal.datatools.common.utils.aws;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.identitymanagement.model.InstanceProfile;
import com.amazonaws.services.identitymanagement.model.ListInstanceProfilesResult;

public class IAMUtils {
    private static final AmazonIdentityManagement DEFAULT_IAM_CLIENT = AmazonIdentityManagementClientBuilder
        .defaultClient();
    private static IAMClientManagerImpl IAMClientManager = new IAMClientManagerImpl(DEFAULT_IAM_CLIENT);

    /**
     * A class that manages the creation of IAM clients.
     */
    private static class IAMClientManagerImpl extends AWSClientManager<AmazonIdentityManagement> {
        public IAMClientManagerImpl(AmazonIdentityManagement defaultClient) {
            super(defaultClient);
        }

        @Override
        public AmazonIdentityManagement buildDefaultClientWithRegion(String region) {
            return defaultClient;
        }

        @Override
        public AmazonIdentityManagement buildCredentialedClientForRoleAndRegion(
            AWSCredentialsProvider credentials, String region, String role
        ) {
            return AmazonIdentityManagementClientBuilder.standard().withCredentials(credentials).build();
        }
    }

    public static AmazonIdentityManagement getIAMClient(String role, String region) throws CheckedAWSException {
        return IAMClientManager.getClient(role, region);
    }

    /** Get IAM instance profile for the provided role ARN. */
    public static InstanceProfile getIamInstanceProfile(
        AmazonIdentityManagement iamClient, String iamInstanceProfileArn
    ) {
        ListInstanceProfilesResult result = iamClient.listInstanceProfiles();
        // Iterate over instance profiles. If a matching ARN is found, silently return.
        for (InstanceProfile profile: result.getInstanceProfiles()) {
            if (profile.getArn().equals(iamInstanceProfileArn)) return profile;
        }
        return null;
    }
}
