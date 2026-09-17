package com.conveyal.datatools.common.utils.aws;

import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.IamClientBuilder;
import software.amazon.awssdk.services.iam.model.InstanceProfile;
import software.amazon.awssdk.services.iam.model.ListInstanceProfilesResponse;

/**
 * This class contains utilities related to using AWS IAM services.
 */
public class IAMUtils {
    private static final IamClient DEFAULT_IAM_CLIENT = IamClient.builder().build();
    private static final IAMClientManagerImpl IAMClientManager = new IAMClientManagerImpl(DEFAULT_IAM_CLIENT);

    /**
     * A class that manages the creation of IAM clients.
     */
    private static class IAMClientManagerImpl extends AWSClientManager<IamClient> {
        public IAMClientManagerImpl(IamClient defaultClient) {
            super(defaultClient);
        }

        @Override
        public IamClient buildDefaultClientWithRegion(String region) {
            return defaultClient;
        }

        @Override
        public IamClient buildCredentialedClientForRoleAndRegion(
            AwsCredentialsProvider credentials, String region, String role
        ) {
            IamClientBuilder builder = IamClient.builder()
                .credentialsProvider(credentials);
            if (region != null) {
                builder = builder.region(Region.of(region));
            }
            return builder.build();
        }
    }

    public static IamClient getIAMClient(String role, String region) throws CheckedAWSException {
        return IAMClientManager.getClient(role, region);
    }

    /** Get IAM instance profile for the provided role ARN. */
    public static InstanceProfile getIamInstanceProfile(
        IamClient iamClient, String iamInstanceProfileArn
    ) {
        ListInstanceProfilesResponse result = iamClient.listInstanceProfiles();
        // Iterate over instance profiles. If a matching ARN is found, silently return.
        for (InstanceProfile profile: result.instanceProfiles()) {
            if (profile.arn().equals(iamInstanceProfileArn)) return profile;
        }
        return null;
    }

    /** Validate that IAM instance profile ARN exists and is not empty. */
    public static EC2ValidationResult validateIamInstanceProfileArn(
        IamClient client, String iamInstanceProfileArn
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
