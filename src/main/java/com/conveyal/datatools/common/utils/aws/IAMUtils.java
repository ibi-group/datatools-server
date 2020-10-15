package com.conveyal.datatools.common.utils.aws;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.identitymanagement.model.InstanceProfile;
import com.amazonaws.services.identitymanagement.model.ListInstanceProfilesResult;
import org.apache.commons.lang3.StringUtils;

/**
 * This class contains utilities related to using AWS IAM services.
 */
public class IAMUtils {
    private static final AmazonIdentityManagement DEFAULT_IAM_CLIENT = AmazonIdentityManagementClientBuilder
        .defaultClient();
    private static final IAMClientManagerImpl IAMClientManager = new IAMClientManagerImpl(DEFAULT_IAM_CLIENT);

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
            AmazonIdentityManagementClientBuilder builder = AmazonIdentityManagementClientBuilder
                .standard()
                .withCredentials(credentials);
            if (region != null) {
                builder = builder.withRegion(region);
            }
            return builder.build();
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

    /** Validate that IAM instance profile ARN exists and is not empty. */
    public static EC2ValidationResult validateIamInstanceProfileArn(
        AmazonIdentityManagement client, String iamInstanceProfileArn
    ) {
        EC2ValidationResult result = new EC2ValidationResult();
        String message = "Server must have valid IAM instance profile ARN (e.g., arn:aws:iam::123456789012:instance-profile/otp-ec2-role).";
        if (StringUtils.isEmpty(iamInstanceProfileArn)) {
            result.setInvalid(message);
            return result;
        }
        if (
            IAMUtils.getIamInstanceProfile(client, iamInstanceProfileArn) == null
        ) {
            result.setInvalid(message);
        }
        return result;
    }
}
